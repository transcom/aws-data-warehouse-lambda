import multiprocessing
from multiprocessing.connection import Connection as multi_processing_connection
import boto3
import pg8000
import json
import time
import io
import db_conn
import os
from uuid import UUID
import datetime
import gc
from multiprocessing import Process, Pipe
from typing import List, Tuple

# Storing current time, we will use this to update SSM when finished so that we
# know for the next run which time to select from
current_run_time = datetime.datetime.now()

# Called to sleep the current thread and allow the workers
# more processors
def yield_for_workers():
    time.sleep(15)

# Define batch size fetching of records for json dumps
batch_size = 100000

# Lambda global connection for warm starts
# This connection is only used to grab the table names
# When multiprocessing, the worker processes each create their own
# connection
connection = None

max_processes = multiprocessing.cpu_count()  # Processor limit

# Class to format json UUID's
class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)

def parallel_worker(worker_conn, batch, column_names):
    # Handle process worker -> back to json mapping and
    # respond it back via the pipe. We'll convert a batch
    # of rows into JSON and then close the pipe with our response
    try:
        fragment = convert_batch_to_json(batch, column_names)
        worker_conn.send(("fragment", fragment))
    except Exception as e:
        worker_conn.send(("error", f"ERROR: {e}"))
    finally:
        worker_conn.close()

def convert_batch_to_json(batch, column_names):
    # Convert a batch of rows to comma delimited JSON fragments (No start/end brackets)
    records = []
    for row in batch:
        as_dict = map_row_to_columns(row, column_names)
        records.append(json.dumps(as_dict, cls=UUIDEncoder, default=str))
    return ",".join(records).encode("utf-8") # Return as bytes

#  Helper function to batch fetch data and use multipart uploading with S3
def fetch_and_upload_cursor_results(
    cursor: pg8000.Cursor, bucket_name, key_name, column_names
):
    s3_client = boto3.client("s3")
    # Initiate multipart upload
    multipart_upload = s3_client.create_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        ServerSideEncryption="AES256",
    )
    upload_id = multipart_upload["UploadId"]
    parts = []
    part_number = 1
    buffer = io.BytesIO()
    buffer_size = 0
    min_part_size = 50 * 1024 * 1024  # 50 MB
    
    batches = list(fetch_batches(cursor))
    total_batches = len(batches)
    print(f"Data Warehouse Lambda - INFO - {total_batches} batches found for {key_name}")
    
    # Handle case of no records to convert to JSON
    if total_batches == 0:
        # No records, make a new buffer and write an empty array for the dump
        try:
            buffer.write(b"[]")
            buffer.seek(0)
            response = s3_client.upload_part(
                Bucket=bucket_name,
                Key=key_name,
                PartNumber=part_number,
                UploadId=upload_id,
                Body=buffer,
            )
            parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
            s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key_name,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            print(
                f"Data Warehouse Lambda - INFO - DB Extract - Successfully wrote {bucket_name}/{key_name}"
            )
        except Exception as e:
            # Abort multipart upload in case of failure
            s3_client.abort_multipart_upload(
                Bucket=bucket_name, Key=key_name, UploadId=upload_id
            )
            print(
                f"Data Warehouse Lambda - ERROR - DB Extract - Error during multipart upload: {e}"
            )
            raise e
        # Early return
        return
    
    # Create a manager -> worker process for each batch in parallel
    # This makes a manager and worker combo for every batch that we will await later
    workers: List[Tuple[Process, multi_processing_connection, int]] = []
    for i, batch in enumerate(batches):
        manager, worker = Pipe()
        p = Process(target=parallel_worker, args=(worker, batch, column_names))
        workers.append((p, manager, i))
        p.start()
            
    #
    # Start our JSON output document
    #
    # Begin with our bracket as we are
    # going to format this manually
    buffer.write(b"[")
    buffer_size += 1

    # Concurrently wait for the workers to be complete
    while workers:
        for i in range(len(workers) - 1, -1, -1): # Backwards loop because we pop list entries
            process, manager, batch_index = workers[i]
            # Temporarily free this processor so the workers can use it
            yield_for_workers()
            # Loop over the workers infinitely until we are out of workers
            # or until we find a worker whose process has completed
            if not process.is_alive():
                print(
                f"Data Warehouse Lambda - DEBUG - DB Extract - {key_name} worker for batch index {batch_index} complete, polling it now. {len(workers)} workers remain"
                )
                is_last_worker = len(workers) == 1
                # Process completed and worker has a letter for us
                if manager.poll():
                    # Have the manager receive the worker's data
                    msg_type, data = manager.recv()
                    if msg_type == "error":
                        print(f"Data Warehouse Lambda - ERROR - Worker {batch_index} failed: {data}")
                        raise RuntimeError(f"Worker {batch_index} error: {data}")
                    print(
                    f"Data Warehouse Lambda - DEBUG - DB Extract - {key_name} worker for batch index {batch_index} polled successfully. {len(workers)} workers remain"
                    )
                    # Add data fragment to buffer
                    buffer.write(data)
                    buffer_size += len(data)
                    
                    if not is_last_worker:
                        # If it's not the last worker, append `,` to support a final
                        # [${worker_1_json}, ${worker_2_json}, ${worker_3_json}] output
                        buffer.write(b",")
                        buffer_size += 1
                        
                    # Upload part if buffer reaches the minimum part size or
                    # if this is the final worker
                    if buffer_size >= min_part_size or is_last_worker:
                        if is_last_worker:
                            # Last call, close it up!
                            buffer.write(b"]")
                            buffer_size += 1
                        buffer.seek(0)
                        response = s3_client.upload_part(
                            Bucket=bucket_name,
                            Key=key_name,
                            PartNumber=part_number,
                            UploadId=upload_id,
                            Body=buffer,
                        )
                        parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
                        part_number += 1
                        buffer.close()
                        buffer = io.BytesIO()
                        buffer_size = 0

                        gc.collect()
                manager.close() # Free thread
                process.join() # Make sure worker is done
                workers.pop(i) # Clear the worker
                

    print(
    f"Data Warehouse Lambda - DEBUG - DB Extract - {key_name} workers complete, announcing multipart completion"
    )

    # Workers have completed and there is nothing left to upload to the multi-part
    buffer.close()
    del buffer
    # Announce multipart upload completion
    s3_client.complete_multipart_upload(
        Bucket=bucket_name,
        Key=key_name,
        UploadId=upload_id,
        MultipartUpload={"Parts": parts},
    )
    print(
        f"Data Warehouse Lambda - INFO - DB Extract - Successfully wrote {bucket_name}/{key_name}"
    )

# Helper func to yield results rather than return
# to boost processing efficiency
def fetch_batches(cursor: pg8000.Cursor):
    while True:
        batch = cursor.fetchmany(batch_size)
        if not batch:
            # No more batch results
            break
        yield batch


def map_row_to_columns(row, column_names):
    return {column_names[i]: row[i] for i in range(len(column_names))}


def table_extractor(
    table_name,
    json_parameter_value,
    bucket_name,
    worker: multi_processing_connection,
):
    try:
        connection = db_conn.get_connection()
        if connection is None:
            error_msg = (
                f"Failed to connect to database during worker process for {table_name}"
            )
            print(f"Data Warehouse Lambda - ERROR - DB Extract - {error_msg}")
            worker.send(error_msg)
            return
        print(
            f"Data Warehouse Lambda - INFO - Worker extraction process created for table {table_name}"
        )
        s3_key = f"db_data/{str(json_parameter_value['data']['serialNumber'] + 1).zfill(6)}/{table_name}.json"
        cursor = connection.cursor()  # type: ignore
        cursor.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name=%s",
            (table_name,),
        )
        column_names = [column[0] for column in cursor.fetchall()]

        # Determine whether we have the timestamp fields created_at and updated_at
        found_created_at = False
        found_updated_at = False
        for column in column_names:
            if "updated_at" in column:
                found_updated_at = True
            if "created_at" in column:
                found_created_at = True

        # Set the statement timeout to 600 seconds for this session
        cursor.execute("SET statement_timeout = '600s'")

        # Handle if the table being iterated on does not have updated_at or created_at
        # Since we do not have timestamps to compare to, we must full dump the table without updated or created at
        if found_updated_at == False and found_created_at == False:
            print(
                "Data Warehouse Lambda - INFO - DB Extract - Performing full dump on "
                + str(table_name)
            )
            # Tell the database to execute this query, we will ingest it in chunks
            cursor.execute("SELECT * FROM " + str(table_name))
            # Fetch cursor results and upload to S3
            fetch_and_upload_cursor_results(cursor, bucket_name, s3_key, column_names)
        # If we have created_at but no updated_at, we dump based only on created_at
        elif found_updated_at == False and found_created_at == True:
            last_run_time = json_parameter_value["data"]["lastRunTime"]
            cursor.execute(
                "SELECT * FROM "
                + str(table_name)
                + " where (created_at > '"
                + str(last_run_time)
                + "') order by created_at;"
            )
            fetch_and_upload_cursor_results(cursor, bucket_name, s3_key, column_names)
        # If we have created_at and updated_at, we dump based on both
        elif found_updated_at == True and found_created_at == True:
            last_run_time = json_parameter_value["data"]["lastRunTime"]
            cursor.execute(
                "SELECT * FROM "
                + str(table_name)
                + " where ((created_at > %s) OR (updated_at > %s)) order by created_at;",
                (last_run_time, last_run_time),
            )
            fetch_and_upload_cursor_results(cursor, bucket_name, s3_key, column_names)
        else:
            print(
                "Data Warehouse Lambda - ERROR - DB Extract - "
                + str(table_name)
                + " does not match any criteria for data warehousing"
            )

        # Worker process complete
        worker.send(f"Successfully processed {table_name}")
    except Exception as e:
        error_msg = f"Error processing {table_name}: {e}"
        print(f"Data Warehouse Lambda - ERROR - DB Extract - {error_msg}")
        worker.send(error_msg)
    finally:
        worker.close()


def db_extractor():
    # Use the get_parameter method of the SSM client to retrieve the parameter
    try:
        # Create an SSM client
        ssm_client = boto3.client("ssm")
        response = ssm_client.get_parameter(
            Name=os.environ["parameter_name"], WithDecryption=True
        )
        # The value of the parameter is stored in the 'Value' field of the response
        parameter_value = response["Parameter"]["Value"]
        json_parameter_value = json.loads(parameter_value)
    except Exception as e:
        print(
            "Data Warehouse Lambda - ERROR - DB Extract - Failed to retrieve values from SSM"
            + str(e)
        )

    global connection
    try:
        if connection is None:
            connection = db_conn.get_connection()
        if connection is None:
            print(
                "Data Warehouse Lambda - ERROR - DB Extract - Failed to connect to database"
            )
            return

        # Instantiate the cursor object
        cursor = connection.cursor()  # type: ignore (Type none is handled)

        # Get a list of all tables in the database
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        )
        tables_tmp = cursor.fetchall()

        # Sanitize the table name's
        tables_list = []
        for tmp_table_name in tables_tmp:
            tmp_table_name = tmp_table_name[0]
            # remove any special characters or whitespaces
            tmp_table_name = "".join(
                e for e in tmp_table_name if e.isalnum() or e == "_"
            )
            # make sure the table name is lowercase
            tmp_table_name = tmp_table_name.lower()
            # add the sanitized table name to the list
            tables_list.append(tmp_table_name)
        tables = tuple(tables_list)

        # List tables that should be excluded from the dump
        table_dump_ignore = [
            "zip3_distances",
            "transportation_service_provider_performances",
            "move",
            "move_to_gbloc",
            "archived_access_codes",
            "schema_migration",
            "audit_history_tableslist",
            "audit_history",
            "v_locations",
        ]

        # S3 bucket upload location
        bucket_name = os.environ["bucket_name"]

        # Filter out ignored tables
        tables = [table for table in tables_list if table not in table_dump_ignore]

        # Create multi processes for the number of tables we have
        # for the number of processors we have
        cursor.close()  # Close the current cursor, we are done with it. Worker processes make new ones

        # Process each table in a separate Process with Pipe  
        table_index = iter(tables)
        processes: List[Tuple[Process, multi_processing_connection]] = (
            []
        )  # Holds the individual processes in tuples with the parent/manager pipes
        
        # TODO: Logic to handle only 1-2 processors
        # or fail if there are 2 processors or less
        print(
            f"Data Warehouse Lambda - INFO - {max_processes} processors available for use"
        )
        
        while True:
            while len(processes) < max_processes:
                try:
                    table_name = next(table_index)
                except StopIteration:
                    # This exception is thrown when the next function can't find anything
                    break
                manager, worker = Pipe()
                process = Process(
                    target=table_extractor,
                    args=(table_name, json_parameter_value, bucket_name, worker),
                )
                processes.append((process, manager))
                process.start()
            if not processes:
                # No remaining processes are active
                break
            
            # Manage processes concurrently without sequential waiting
            for i in range(len(processes) - 1, -1, -1):
                # Temporarily free this processor so the workers can use it
                yield_for_workers()
                process, pipe = processes[i]
                if not process.is_alive():
                    # Process has finished, poll it and receive its message
                    if pipe.poll():
                        message = pipe.recv()
                        print(f"Data Warehouse Lambda - INFO - {message}")
                    pipe.close()
                    process.join()
                    processes.pop(
                        i
                    )  # Remove the finished process to free up processor capacity

        # Create an SSM client
        try:
            ssm_client = boto3.client("ssm")
            json_parameter_value["data"]["serialNumber"] += 1
            json_parameter_value["data"]["lastRunTime"] = str(current_run_time)
            ssm_client.put_parameter(
                Name=os.environ["parameter_name"],
                Value=json.dumps(json_parameter_value),
                Type="SecureString",
                Overwrite=True,
            )

            print("Data Warehouse Lambda - INFO - DB Extract - Updated tracking in SSM")
        except Exception as e:
            print(
                "Data Warehouse Lambda - ERROR - DB Extract - Error writing to SSM"
                + str(e)
            )

    except Exception as e:
        try:
            connection.close()  # type: ignore (Type none is handled)
        except Exception as e:
            connection = None
        print("Data Warehouse Lambda - ERROR - DB Extract - Failed due to :" + str(e))
