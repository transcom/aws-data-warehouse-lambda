import multiprocessing
from multiprocessing.connection import Connection as multi_processing_connection
import boto3
import pg8000
import json
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

# Define batch size fetching of records for json dumps
batch_size = 100000

# Lambda global connection for warm starts
# This connection is only used to grab the table names
# When multiprocessing, the child processes each create their own
# connection
connection = None

# Class to format json UUID's
class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)


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
    min_part_size = 5 * 1024 * 1024  # 5 MB
    first_record = True # Track the first record for JSON formatting


    try:
        # Write the opening bracket for the JSON array
        buffer.write(b'[')
        buffer_size += 1
        # Fetch the results of the cursor query
        for batch in fetch_batches(cursor):
            data_with_col_names = (
                map_row_to_columns(row, column_names) for row in batch
            )
            for record in data_with_col_names:
                if not first_record:
                    # Add comma before each record except the first
                    buffer.write(b',')
                    buffer_size += 1
                else:
                    # Set to False after processing the first record
                    first_record = False 
                
                json_line = json.dumps(record, cls=UUIDEncoder, default=str)
                json_line_bytes = json_line.encode("utf-8")
                buffer.write(json_line_bytes)
                buffer_size += len(json_line_bytes)

                # Upload part if buffer reaches the minimum part size of 5MB
                if buffer_size >= min_part_size:
                    # This part is now finished being appended too
                    # Trigger upload, reset buffer, and proceed with the next one
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

        if first_record:
            # No records were written, make a new buffer and write an empty array
            buffer = io.BytesIO()
            buffer.write(b'[]')
            buffer_size = 2
        else:
            # Write closing bracket to close the JSON array
            buffer.write(b']')
            buffer_size += 1

        # Upload the final part (Or only part if no records)
        buffer.seek(0)
        response = s3_client.upload_part(
            Bucket=bucket_name,
            Key=key_name,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=buffer,
        )
        parts.append({"PartNumber": part_number, "ETag": response["ETag"]})
        buffer.close()

        if parts:
            # Complete multipart upload
            s3_client.complete_multipart_upload(
                Bucket=bucket_name,
                Key=key_name,
                UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            print(
                f"Data Warehouse Lambda - INFO - DB Extract - Successfully wrote {bucket_name}/{key_name}"
            )
        else:
            # Abort the multipart upload if no parts were uploaded
            s3_client.abort_multipart_upload(
                Bucket=bucket_name, Key=key_name, UploadId=upload_id
            )
            print(
                f"Data Warehouse Lambda - INFO - No data to upload for table {key_name}"
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
    child_conn: multi_processing_connection,
):
    try:
        connection = db_conn.get_connection()
        if connection is None:
            error_msg = (
                f"Failed to connect to database during child process for {table_name}"
            )
            print(f"Data Warehouse Lambda - ERROR - DB Extract - {error_msg}")
            child_conn.send(error_msg)
            return
        print(
            f"Data Warehouse Lambda - INFO - Child extraction process created for table {table_name}"
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

        # Main child process complete
        child_conn.send(f"Successfully processed {table_name}")
    except Exception as e:
        error_msg = f"Error processing {table_name}: {e}"
        print(f"Data Warehouse Lambda - ERROR - DB Extract - {error_msg}")
        child_conn.send(error_msg)
    finally:
        child_conn.close()


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
        ]

        # S3 bucket upload location
        bucket_name = os.environ["bucket_name"]

        # Filter out ignored tables
        tables = [table for table in tables_list if table not in table_dump_ignore]

        # Create multi processes for the number of tables we have
        # for the number of processors we have
        cursor.close()  # Close the current cursor, we are done with it. Child processes make new ones

        # Process each table in a separate Process with Pipe
        max_processes = multiprocessing.cpu_count()  # Retrieve processor limit
        table_index = iter(tables)
        processes: List[Tuple[Process, multi_processing_connection]] = (
            []
        )  # Holds the individual processes in tuples with the parent pipes

        while True:
            while len(processes) < max_processes:
                try:
                    table_name = next(table_index)
                except StopIteration:
                    # This exception is thrown when the next function can't find anything
                    break
                parent_conn, child_conn = Pipe()
                process = Process(
                    target=table_extractor,
                    args=(table_name, json_parameter_value, bucket_name, child_conn),
                )
                processes.append((process, parent_conn))
                process.start()
            if not processes:
                # No remaining processes are active
                break

            # Manage processes concurrently without sequential waiting
            for i in range(len(processes) - 1, -1, -1):
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
