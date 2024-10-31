import boto3
import pg8000
import json
import io
import db_conn
import os
from uuid import UUID
import datetime
import gc

# Declare the global connection object to use during warm starting
# to reuse connections that were established during a previous invocation.
connection = None

# Storing current time, we will use this to update SSM when finished so that we
# know for the next run which time to select from
current_run_time = datetime.datetime.now()

# Define batch size fetching of records for json dumps
batch_size = 100000


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

    try:
        # Fetch the results of the cursor query
        for batch in fetch_batches(cursor):
            data_with_col_names = (
                map_row_to_columns(row, column_names) for row in batch
            )
            for record in data_with_col_names:
                json_line = json.dumps(record, cls=UUIDEncoder, default=str) + "\n"
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

        # Upload any remaining data in the buffer (Eg, the last batch finished with a part < 5MB)
        if buffer_size > 0:
            # Final part upload
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

        # Loop over each table and dump its data. Handling initial dump and incremental dumps on reruns
        for table in tables:
            table_name = table

            s3_key = f"db_data/{str(json_parameter_value['data']['serialNumber'] + 1).zfill(6)}/{table_name}.json"
            # get a list of the table's columns
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

            # If we ignore the table, we do nothing
            if str(table_name) in table_dump_ignore:
                print(
                    "Data Warehouse Lambda - INFO - DB Extract - Didn't extract data for table(ignore list): "
                    + str(table_name)
                )
            # Handle if the table being iterated on does not have updated_at or created_at
            # Since we do not have timestamps to compare to, we must full dump the table without updated or created at
            elif found_updated_at == False and found_created_at == False:
                print(
                    "Data Warehouse Lambda - INFO - DB Extract - Performing full dump on "
                    + str(table_name)
                )
                # Tell the database to execute this query, we will ingest it in chunks
                cursor.execute("SELECT * FROM " + str(table_name))
                # Fetch cursor results and upload to S3
                fetch_and_upload_cursor_results(
                    cursor, bucket_name, s3_key, column_names
                )
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
                fetch_and_upload_cursor_results(
                    cursor, bucket_name, s3_key, column_names
                )
            # If we have created_at and updated_at, we dump based on both
            elif found_updated_at == True and found_created_at == True:
                last_run_time = json_parameter_value["data"]["lastRunTime"]
                cursor.execute(
                    "SELECT * FROM "
                    + str(table_name)
                    + " where ((created_at > %s) OR (updated_at > %s)) order by created_at;",
                    (last_run_time, last_run_time),
                )
                fetch_and_upload_cursor_results(
                    cursor, bucket_name, s3_key, column_names
                )
            else:
                print(
                    "Data Warehouse Lambda - ERROR - DB Extract - "
                    + str(table_name)
                    + " does not match any criteria for data warehousing"
                )

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
