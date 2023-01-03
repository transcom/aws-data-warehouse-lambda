import boto3
import pg8000
import json
import datetime
import db_conn
import os


# Declare the global connection object to use during warm starting
# to reuse connections that were established during a previous invocation.
connection = None


def db_data_dump():

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
#        print("Data Warehouse Lambda - DB Data - Retrieved values from SSM")
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Data - Failed to retrieve values from SSM" + str(e))

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
        query = 'SELECT * FROM audit_history WHERE action_tstamp_tx > \'' + str(json_parameter_value['data']['lastFileTime']) + '\' order by action_tstamp_tx'

        cursor.execute(query)
        rows = cursor.fetchall()

        payload = "{    "
        last_timestamp = ""

        count = 0

        for row in rows:
            row_data = '\
                "'+str(count)+'": \
                { \
                "audit_id": "' + str(row[0]) + '", \
                "schema_name": "' + str(row[1]) + '", \
                "table_name": "' + str(row[2]) + '", \
                "relid": "' + str(row[3]) + '", \
                "id": "' + str(row[4]) + '", \
                "session_userid": "' + str(row[5]) + '", \
                "event_name": "' + str(row[6]) + '", \
                "action_tstamp_tx": "'+str(row[7])+'", \
                "action_tstamp_stm": "'+str(row[8])+'", \
                "action_tstamp_clk": "'+str(row[9])+'", \
                "transaction_id": "' + str(row[10]) + '", \
                "action": "' + str(row[12]) + '", \
                "statement_only": "' + str(row[15]) + '", \
                "old_data": '+json.dumps(row[13])+', \
                "changed_data": '+json.dumps(row[14])+'},'

            payload += "\n" + row_data

            row_data = ""
            count += 1
            last_timestamp = str(row[7])

        payload = payload[:-1]
        payload += "\n}"

        if count > 0:
            print("Data Warehouse Lambda - INFO - DB Data - Found "+ str(count) + " audit_history records since last run")

        if last_timestamp:
            file_name = str(json_parameter_value['data']['serialNumber'] + 1).zfill(6) + '_db_data.json'

            s3 = boto3.client('s3')
            try:
                s3.put_object(Bucket=os.environ['bucket_name'], Key=os.environ['folder_name_data'] + '/' + file_name, Body=payload, ServerSideEncryption='AES256')
                print('Data Warehouse Lambda - INFO - DB Data - Successfully wrote ' + os.environ['bucket_name'] + '/' + os.environ['folder_name_data'] + '/' + file_name)
            except Exception as e:
                print("Data Warehouse Lambda - ERROR - DB Data - Error writing to S3" + str(e))


            # Create an SSM client
            try:
                ssm_client = boto3.client('ssm')
                json_parameter_value['data']['serialNumber'] += 1
                json_parameter_value['data']['lastFileTime'] = last_timestamp
                ssm_client.put_parameter(
                        Name=os.environ['parameter_name'],
                        Value=json.dumps(json_parameter_value),
                        Type='SecureString',
                        Overwrite=True
                    )
                #print("Data Warehouse Lambda - INFO - DB Data - Updated tracking in SSM")
            except Exception as e:
                print("Data Warehouse Lambda - ERROR - DB Data - Error writing to SSM" + str(e))
        else:
            print("Data Warehouse Lambda - WARN - DB Data - No new data in the audit_history table to write")



    except Exception as e:
        try:
            connection.close()
        except Exception as e:
            connection = None
        print ("Data Warehouse Lambda - ERROR - DB Data - Failed:" + str(e))
