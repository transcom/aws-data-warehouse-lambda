import boto3
import pg8000
import json
import time
import db_conn
import hashlib
import os


# Declare the global connection object to use during warm starting
# to reuse connections that were established during a previous invocation.
connection = None


def db_schema_dump():

    # Use the get_parameter method of the SSM client to retrieve the parameter
    try:
        # Create an SSM client
        ssm_client = boto3.client('ssm')
        response = ssm_client.get_parameter(
            Name=os.environ['parameter_name'],
            WithDecryption=True
        )
        # The value of the parameter is stored in the 'Value' field of the response
        parameter_value = response['Parameter']['Value']
        json_parameter_value = json.loads(parameter_value)
#        print("Data Warehouse Lambda - DB Schema - Retrieved values from SSM")
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Schema - Failed to retrieve values from SSM" + str(e))


    global connection
    try:
        if connection is None:
            connection = db_conn.get_connection()
        if connection is None:
            print("Data Warehouse Lambda - ERROR - DB Data - Failed to connect to database")

        # Instantiate the cursor object
        cursor = connection.cursor()

        # Initialize an empty dictionary to store the data
        data = {}

        # Get a list of all tables in the database
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name NOT in ('audit_history','v_locations') ")
        tables = cursor.fetchall()

        # For each table, get a list of its columns
        for table in tables:
            table_name = table[0]
            cursor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (table_name,))
            columns = cursor.fetchall()
            data[table_name] = columns

        # Serialize the data as JSON
        json_data = json.dumps(data)


        # Check md5 hash of the DB tables to detect changes
        hash_object = hashlib.md5()
        hash_object.update(json_data.encode())
        db_schema_hash = hash_object.hexdigest()

        if json_parameter_value['schema']['lastMD5Hash'] != db_schema_hash:
            print("Data Warehouse Lambda - WARN - DB Schema - Database Schema has changed since last run")

            # Write the data to S3
            try:
                file_name = str(json_parameter_value['schema']['serialNumber'] + 1).zfill(6) + '_db_schema.json'
                s3 = boto3.client('s3')
                s3.put_object(Bucket=os.environ['bucket_name'], Key=os.environ['folder_name_schema'] + '/' + file_name, Body=json_data, ServerSideEncryption='AES256')
                print('Data Warehouse Lambda - INFO - DB Schema - Successfully wrote ' + os.environ['bucket_name'] + '/' + os.environ['folder_name_schema'] + '/' + file_name)
            except Exception as e:
                print("Data Warehouse Lambda - ERROR - DB Schema - Error writing to S3" + str(e))


            # Create an SSM client
            try:
                ssm_client = boto3.client('ssm')
                json_parameter_value['schema']['serialNumber'] += 1
                json_parameter_value['schema']['lastMD5Hash'] = db_schema_hash

                ssm_client.put_parameter(
                        Name=os.environ['parameter_name'],
                        Value=json.dumps(json_parameter_value),
                        Type='SecureString',
                        Overwrite=True
                    )
    #            print("Data Warehouse Lambda - DB Schema - Updated tracking in SSM")
            except Exception as e:
                print("Data Warehouse Lambda - ERROR - DB Schema - Error writing to SSM" + str(e))
        else:
            print("Data Warehouse Lambda - INFO - DB Schema - No changes to DB schema, not writing to S3")

    except Exception as e:
        try:
            connection.close()
        except Exception as e:
            connection = None
        print ("Data Warehouse Lambda - ERROR - DB Schema - Failed due to :" + str(e))
