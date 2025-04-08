import multiprocessing
from multiprocessing.connection import Connection as multi_processing_connection
from multiprocessing.connection import wait
from multiprocessing import Process, Pipe
import multiprocessing.connection
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
from typing import Any, Iterator, List, Tuple

# Storing current time, we will use this to update SSM when finished so that we
# know for the next run which time to select from
current_run_time = datetime.datetime.now()

# How many SQL records a worker will fetch at a time
batch_size = 20000

# Max amount of concurrent batch workers.
# Not workers in general, just the amount allowed to work the batch processing
# at the same time for a given table.
# So X amount for table Y and X amount for table Z
MAX_CONCURRENT_BATCH_WORKERS = 4

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
    
def print_memory():
    usage = resource.getrusage(resource.RUSAGE_SELF)
    mem_mb = usage.ru_maxrss / 1024  # KB to MB
    print(
        f"Data Warehouse Lambda - DEBUG - Memory usage: {mem_mb:.2f} MB"
    )

def parallel_worker(worker_conn, batch, column_names, key_name):
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

def convert_batch_to_json(batch, column_names):
    # Convert a batch of rows to comma delimited JSON fragments (No start/end brackets)
    records = []
    for row in batch:
        as_dict = map_row_to_columns(row, column_names)
        records.append(json.dumps(as_dict, cls=UUIDEncoder, default=str))
    return ",".join(records).encode("utf-8") # Return as bytes

def upload_empty_json(s3_client, bucket_name, key_name, part_number, upload_id, parts):
    buffer = io.BytesIO()
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
    
    print(f"Data Warehouse Lambda - DEBUG - DB Extract - Creating batch generator for {bucket_name}/{key_name}\n")
    print_memory()
    batch_generator = lookahead(fetch_batches(cursor))
    
    #
    # Start our JSON output document
    #
    # Begin with our bracket as we are
    # going to format this manually
    buffer.write(b"[")
    buffer_size += 1

    print(
        f"Data Warehouse Lambda - DEBUG - DB Extract - Beginning worker generation for {bucket_name}/{key_name}\n"
    )
    print_memory()
    # Create a manager -> worker process for each batch in parallel
    # This makes a manager and worker combo for every batch that we will await later
    workers: List[Tuple[Process, multi_processing_connection, int, bool]] = []

    # Process remaining batches from the generator
    did_batches_run = False
    all_workers_have_finished = False
    total_finished_workers: List[Tuple[Process, multi_processing_connection, int, bool]] = []
    for i, (batch, is_last_generator) in enumerate(batch_generator):
        did_batches_run = True
        print(
            f"Data Warehouse Lambda - DEBUG - DB Extract - Beginning batch {i} for {bucket_name}/{key_name}\n"
        )
        print_memory()
        
        # Queue the worker
        manager, worker = Pipe()
        p = Process(target=parallel_worker, args=(worker, batch, column_names, key_name))
        workers.append((p, manager, i, is_last_generator))
        p.start()
        
        # Prep to extract the last batch worker from the queue
        # This is done so that we can conditionally make sure
        # that we close the JSON array properly `]` in the
        # multi-part upload
        last_batch_worker = None
        
        # If we hit the max amount of workers or it is the last generator
        # then we want to infinitely loop until either a worker is freed
        # or the loop is completely exited because the last generator is present
        while len(workers) >= MAX_CONCURRENT_BATCH_WORKERS or (is_last_generator and not all_workers_have_finished):
            # Max workers reached or this is the end of the line for the generator,
            # before creating a new process we need to
            # clear out the current workers
            if len(workers) >= MAX_CONCURRENT_BATCH_WORKERS:
                print(
                f"Data Warehouse Lambda - DEBUG - DB Extract - Max concurrent batch workers reached for {bucket_name}/{key_name}\nbeginning upload\nHave all workers finished? {all_workers_have_finished}"
                )
            if is_last_generator:
                print(
                f"Data Warehouse Lambda - DEBUG - DB Extract - Last generator reached for {bucket_name}/{key_name}\nbeginning upload\nHave all workers finished? {all_workers_have_finished}"
                )
                
            # Grab any finished worker managers
            current_finished_worker_managers = wait(
                [mgr for (_, mgr, _, _) in workers], timeout=5
            )
             
            # Map the manager to the worker
            current_finished_workers: List[Tuple[Process, multi_processing_connection, int, bool]] = [
                (proc, mgr, bIndex, is_final_batch)
                for (proc, mgr, bIndex, is_final_batch) in workers
                if mgr in current_finished_worker_managers
            ]
            
            print(
                f"Data Warehouse Lambda - DEBUG - DB Extract - Found this many current finished worker managers {len(current_finished_worker_managers)} which translated to {len(current_finished_workers)} current finished workers {bucket_name}/{key_name}"
            )
            
            # Add them to the total
            for finished_worker in current_finished_workers:
                # For each finished worker, find their respective `worker` entry
                # and once found, add it to the total_finished_workers
                for worker in workers:
                    proc, mgr, batch_index, is_final_batch = worker
                    if mgr is finished_worker[1] and worker not in total_finished_workers:
                        total_finished_workers.append(worker)
                        break
                    
            print(
                f"With {len(current_finished_workers)} current finished workers, I now have {len(total_finished_workers)} total finished workers for {key_name}"
            )
            
            # Iterate over each finished worker, we will poll and upload each of them
            # making sure to only process the final batch worker last if present
            for finished_worker in total_finished_workers:
                proc, mgr, batch_index, is_final_batch = finished_worker
                if last_batch_worker is None:
                    print(
                        f"No last batch worker found for {key_name}"
                    )
                    # Last batch worker hasn't been found yet
                    # Check if this worker is the last batch worker
                    if is_final_batch:
                        print(
                            f"Found the last batch worker found for {key_name}"
                        )
                        # This is the one
                        last_batch_worker = finished_worker
                if last_batch_worker is not None:
                    print(
                        f"if last_batch_worker is not None for {key_name} with a len of {len(workers)} workers"
                    )
                    # We know that if this worker is present, all remaining workers are present
                    # We should only process the final worker if it is the last one
                    # (Yes this if condition can be joined int one, but this is more readable)
                    if len(workers) > 1:
                        # There are unpopped workers present, we should not process the
                        # final batch worker until it is the last one
                        print(
                        f"Data Warehouse Lambda - DEBUG - DB Extract - {bucket_name}/{key_name}\n attempted to poll the last worker while more remain, skipping"
                        )
                        # Instead of immediately continuing the loop, wait until the next worker is complete before continuing
                        # This is a way of preserving CPU threads from being hogged by an infinite loop
                        
                        # Only wait for a new worker if there are any pending+
                        # This is a good condition because it will only be run
                        # if the final batch worker has been found, meaning
                        # the len of total finished workers must be equal to len of workers
                        # themselves
                        if len(total_finished_workers) != len(workers):
                            # Wait for the other worker that is not the last batch worker
                            wait(
                                [tfwMgr for (_, tfwMgr, _, _) in workers if tfwMgr is not last_batch_worker[1]],
                                timeout=5
                            )
                        
                        # Check if this worker is the last batch worker while len workers > 1
                        # If len workers > 1 and this is the last batch worker, skip this worker and process the other one
                        if mgr == last_batch_worker[1]:
                            # This worker's manager is the same manager as the last batch worker, skip it
                            continue
                    
                # Poll the manager, receive their message, upload and pop
                print(
                    f"Data Warehouse Lambda - DEBUG - DB Extract - Seeing if manager has a message for {bucket_name}/{key_name}"
                )
                if mgr.poll():
                    print(
                        f"Data Warehouse Lambda - DEBUG - DB Extract - polling manager with a len of {len(total_finished_workers)} total finished worker and len {len(workers)} workers for {bucket_name}/{key_name}"
                    )
                    msg_type, data = mgr.recv()
                    mgr.close() # Free thread
                    proc.join() # Make sure worker is done
                    # Remove this worker from the workers list
                    total_finished_workers.remove(finished_worker)

                    # Find the corresponding worker and pop it
                    for j, w in enumerate(workers):
                        # If the managers are the same, pop it
                        if w[1] is mgr:
                            workers.pop(j)
                            break
                    print(
                        f"attempted to remove finished worker from total finished workers, new len of total finished workers is {len(total_finished_workers)} as well as len {len(workers)} workers for {key_name}\nIs final batch: {is_final_batch}"
                    )
                    is_final_worker = is_final_batch and len(workers) == 0 # Track if this is the last one
                    if is_final_batch and len(workers) > 1:
                        # This condition should be impossible as it would
                        # allow the processing of the final batch worker
                        # even though there are other workers to be processed first
                        raise RuntimeError(f"Worker {key_name} unexpected error: the final batch worker is attempting to be polled even though they are not the last worker")
                    if msg_type == "error":
                        print(f"Data Warehouse Lambda - ERROR - Worker {batch_index} failed: {data}")
                        raise RuntimeError(f"Worker {batch_index} error: {data}")
                    print(
                    f"Data Warehouse Lambda - INFO - DB Extract - {key_name} worker for batch index {batch_index} polled successfully. {len(workers)} workers remain"
                    )
                    # Add data fragment to buffer
                    buffer.write(data)
                    buffer_size += len(data)
                    
                    if not is_final_worker:
                        # If it's not the last worker, append `,` to support a final
                        # [${worker_1_json}, ${worker_2_json}, ${worker_3_json}] output
                        buffer.write(b",")
                        buffer_size += 1
                        
                    # Upload part if buffer reaches the minimum part size or
                    # if this is the final worker
                    if buffer_size >= min_part_size or is_final_worker:
                        print(
                            f"Triggering upload with buffer size {buffer_size} for {key_name}"
                        )
                        if is_final_worker:
                            all_workers_have_finished = True
                            print(f"Final worker for {key_name} for batch index {batch_index} reached, {len(workers)} workers remain")
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
                        buffer = io.BytesIO()
                        buffer_size = 0

                        gc.collect()


    if not did_batches_run:
        # The for loop won't execute if the cursor returns no rows
        upload_empty_json(s3_client, bucket_name, key_name, part_number, upload_id, parts)
        # Early return
        return
        
    print(
    f"Data Warehouse Lambda - INFO - DB Extract - {key_name} workers complete, announcing multipart completion"
    )

    # Workers have completed and there is nothing left to upload to the multi-part
    buffer.close()
    del buffer
    # Announce multipart upload completion
    try:
        s3_client.complete_multipart_upload(
            Bucket=bucket_name,
            Key=key_name,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
    except Exception as e:
        raise RuntimeError(f"Multipart upload completion error for {key_name}: {e}")
    print(
        f"Data Warehouse Lambda - INFO - DB Extract - Successfully wrote {bucket_name}/{key_name}"
    )

# helper when fetching batches to look ahead in the generator
# letting us know when we've reached the last entry in the
# generator. This assists in json formatting
def lookahead(gen: Iterator) -> Iterator[Tuple[Any, bool]]:
    try:
        prev = next(gen)
    except StopIteration:
        return
    for val in gen:
        yield prev, False
        prev = val
    yield prev, True # End of generator reached
    
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
