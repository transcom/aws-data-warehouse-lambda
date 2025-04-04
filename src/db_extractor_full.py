import multiprocessing
from multiprocessing.connection import Connection as multi_processing_connection
from multiprocessing.connection import wait
from multiprocessing import Process, Pipe
from threading import Semaphore
import resource
from socket import socket
import boto3
import pg8000
import json
import io
import db_conn
import os
from uuid import UUID
import datetime
import gc
import traceback
from typing import List, Tuple

# Storing current time, we will use this to update SSM when finished so that we
# know for the next run which time to select from
current_run_time = datetime.datetime.now()

# How many SQL records a worker will fetch at a time
batch_size = 25000

# Max amount of concurrent batch workers.
# Not workers in general, just the amount allowed to work the batch processing
# at the same time for a given table.
# So X amount for table Y and X amount for table Z
MAX_CONCURRENT_BATCH_WORKERS = 5

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

def parallel_worker(worker_conn, batch, column_names, key_name, sema):
    # Handle process worker -> back to json mapping and
    # respond it back via the pipe. We'll convert a batch
    # of rows into JSON and then close the pipe with our response
    try:
        fragment = convert_batch_to_json(batch, column_names)
        worker_conn.send(("fragment", fragment))
    except Exception as e:
        tb = traceback.format_exc()
        worker_conn.send(("error", f"ERROR: {e}\nKEY: {key_name}\nTrace: {tb}"))
    finally:
        worker_conn.close()
        sema.release()

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
    sema = Semaphore(MAX_CONCURRENT_BATCH_WORKERS)
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
    
    usage = resource.getrusage(resource.RUSAGE_SELF)
    mem_mb = usage.ru_maxrss / 1024  # KB to MB
    print(
        f"Data Warehouse Lambda - DEBUG - DB Extract - Creating batch generator for {bucket_name}/{key_name}\n"
        f"Data Warehouse Lambda - DEBUG - Memory usage: {mem_mb:.2f} MB"
    )
    batch_generator = fetch_batches(cursor)
    
    # Since a generator doesn't return a list, check the first case of if it is empty
    try:
        first_batch = next(batch_generator)
    except StopIteration:
        # This exception WILL be thrown if no SQL records return
        first_batch = None

    if first_batch is None:
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
        # Early return, mark process to no longer be alive
        return
    
    # Get memory usage
    usage = resource.getrusage(resource.RUSAGE_SELF)
    mem_mb = usage.ru_maxrss / 1024  # KB to MB
    print(
        f"Data Warehouse Lambda - DEBUG - DB Extract - Beginning worker generation for {bucket_name}/{key_name}\n"
        f"Data Warehouse Lambda - DEBUG - Memory usage: {mem_mb:.2f} MB"
    )
    # Create a manager -> worker process for each batch in parallel
    # This makes a manager and worker combo for every batch that we will await later
    workers: List[Tuple[Process, multi_processing_connection, int]] = []
    # Make sure we still process the first batch
    # fb = first batch
    fbManager, fbWorker = Pipe()
    fbp = Process(target=parallel_worker, args=(fbWorker, first_batch, column_names, key_name, sema))
    workers.append((fbp, fbManager, 1))
    usage = resource.getrusage(resource.RUSAGE_SELF)
    mem_mb = usage.ru_maxrss / 1024  # KB to MB
    print(
        f"Data Warehouse Lambda - DEBUG - DB Extract - Beginning first batch for {bucket_name}/{key_name}\n"
        f"Data Warehouse Lambda - DEBUG - Memory usage: {mem_mb:.2f} MB"
    )
    fbp.start()
    # Process remaining batches from the generator
    for i, batch in enumerate(batch_generator):
        usage = resource.getrusage(resource.RUSAGE_SELF)
        mem_mb = usage.ru_maxrss / 1024  # KB to MB
        print(
            f"Data Warehouse Lambda - DEBUG - DB Extract - Beginning batch {i} for {bucket_name}/{key_name}\n"
            f"Data Warehouse Lambda - DEBUG - Memory usage: {mem_mb:.2f} MB"
        )
        
        sema.acquire()
        manager, worker = Pipe()
        p = Process(target=parallel_worker, args=(worker, batch, column_names, key_name, sema))
        workers.append((p, manager, i+1))  # +1 because of first batch preceding this
        p.start()

    #
    # Start our JSON output document
    #
    # Begin with our bracket as we are
    # going to format this manually
    buffer.write(b"[")
    buffer_size += 1
    
    print(f'Data Warehouse Lambda - INFO - {key_name} has {len(workers)} workers')

    # Concurrently wait for the workers to be complete
    while workers:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        mem_mb = usage.ru_maxrss / 1024  # KB to MB
        print(
            f"Data Warehouse Lambda - DEBUG - DB Extract - Waiting for workers for {bucket_name}/{key_name}\n"
            f"Data Warehouse Lambda - DEBUG - Memory usage: {mem_mb:.2f} MB"
        )
        finished_workers: List[multi_processing_connection | socket | int] = wait([mgr for (proc, mgr, bIndex) in workers], timeout=None)
        for finished_worker in finished_workers:
            is_last_worker = len(workers) == 1
            for i, (proc, mgr, batch_index) in enumerate(workers):
                if mgr is finished_worker:
                    if mgr.poll():
                        msg_type, data = mgr.recv()
                        mgr.close() # Free thread
                        proc.join() # Make sure worker is done
                        workers.pop(i) # Clear the worker

                        if msg_type == "error":
                            print(f"Data Warehouse Lambda - ERROR - Worker {batch_index} failed: {data}")
                            raise RuntimeError(f"Worker {batch_index} error: {data}")
                        print(
                        f"Data Warehouse Lambda - INFO - DB Extract - {key_name} worker for batch index {batch_index} polled successfully. {len(workers)} workers remain"
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



    print(
    f"Data Warehouse Lambda - INFO - DB Extract - {key_name} workers complete, announcing multipart completion"
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
        cursor.execute(f"FETCH FORWARD {batch_size} FROM data_cursor")
        # Fetch data from client-side cursor that was provided by the server-side cursor
        batch = cursor.fetchall()
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
        cursor.execute("BEGIN READ ONLY;")

        # Handle if the table being iterated on does not have updated_at or created_at
        # Since we do not have timestamps to compare to, we must full dump the table without updated or created at
        if found_updated_at == False and found_created_at == False:
            print(
                "Data Warehouse Lambda - INFO - DB Extract - Performing full dump on "
                + str(table_name)
            )
            # Tell the database to execute this query, we will ingest it in chunks
            # Create a server-side cursor
            cursor.execute(f"DECLARE data_cursor CURSOR FOR SELECT * FROM {table_name}")
            # Fetch cursor results and upload to S3
            fetch_and_upload_cursor_results(cursor, bucket_name, s3_key, column_names)
            cursor.execute("CLOSE data_cursor")
            cursor.execute("COMMIT;")
        # If we have created_at but no updated_at, we dump based only on created_at
        elif found_updated_at == False and found_created_at == True:
            last_run_time = json_parameter_value["data"]["lastRunTime"]
            cursor.execute(
                f"""
                DECLARE data_cursor CURSOR FOR
                SELECT * FROM {table_name}
                WHERE created_at > %s
                ORDER BY created_at
                """,
                (last_run_time,)
            )
            fetch_and_upload_cursor_results(cursor, bucket_name, s3_key, column_names)
            cursor.execute("CLOSE data_cursor")
            cursor.execute("COMMIT;")
        # If we have created_at and updated_at, we dump based on both
        elif found_updated_at == True and found_created_at == True:
            last_run_time = json_parameter_value["data"]["lastRunTime"]
            cursor.execute(f"""
                DECLARE data_cursor CURSOR FOR 
                SELECT * FROM {table_name}
                WHERE ((created_at > %s) OR (updated_at > %s))
                ORDER BY created_at
            """, (last_run_time, last_run_time,))
            fetch_and_upload_cursor_results(cursor, bucket_name, s3_key, column_names)
            cursor.execute("CLOSE data_cursor")
            cursor.execute("COMMIT;")
        else:
            print(
                "Data Warehouse Lambda - ERROR - DB Extract - "
                + str(table_name)
                + " does not match any criteria for data warehousing"
            )

        # Worker process complete
        worker.send(f"Successfully processed {table_name}")
    except Exception as e:
        error_msg = f"Error processing {table_name}: {repr(e)}"
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
        workers: List[Tuple[Process, multi_processing_connection]] = (
            []
        )  # Holds the individual processes in tuples with the parent/manager pipes
        
        print(
            f"Data Warehouse Lambda - INFO - {max_processes} processors available for use"
        )
        
        while True:
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
            workers.append((process, manager))
            process.start()
            
            if not workers:
                # No remaining workers have jobs, db export complete
                break

            for i in range(len(workers) - 1, -1, -1):
                finished_workers: List[multi_processing_connection | socket | int] = wait([mgr for (proc, mgr) in workers], timeout=None)
                for finished_worker in finished_workers:
                    # find the matching worker
                    for i, (proc, mgr) in enumerate(workers):
                        if mgr is finished_worker:
                            if mgr.poll():
                                message = mgr.recv()
                                print(f"Data Warehouse Lambda - INFO - {message}")
                                mgr.close()
                                proc.join()
                                workers.pop(i)

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
