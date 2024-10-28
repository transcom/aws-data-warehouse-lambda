import boto3
import pg8000
import json
import time
import db_conn
import os
from uuid import UUID
import datetime


# Declare the global connection object to use during warm starting
# to reuse connections that were established during a previous invocation.
connection = None

# Storing current time, we will use this to update SSM when finished so that we
# know for the next run which time to select from
current_run_time = datetime.datetime.now()

#Class to format json UUID's
class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)


def db_extractor():
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
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Extract - Failed to retrieve values from SSM" + str(e))


    global connection
    try:
        if connection is None:
            connection = db_conn.get_connection()
        if connection is None:
            print("Data Warehouse Lambda - ERROR - DB Extract - Failed to connect to database")

        # Instantiate the cursor object
        cursor = connection.cursor()

        # Initialize an empty dictionary to store the data
        data = {}

        # Get a list of all tables in the database
        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        #tmp_tables = cursor.fetchall()
        tables_tmp = cursor.fetchall()

        # Sanitize the table name's
        tables_list = []
        for tmp_table_name in tables_tmp:
            tmp_table_name = tmp_table_name[0]
            # remove any special characters or whitespaces
            tmp_table_name = ''.join(e for e in tmp_table_name if e.isalnum() or e == '_')
            # make sure the table name is lowercase
            tmp_table_name = tmp_table_name.lower()
            # add the sanitized table name to the list
            tables_list.append(tmp_table_name)
        tables = tuple(tables_list)


#        table_dump_ignore = ['django_migrations', 'audit_history', 'archived_access_codes', 'schema_migration', 'audit_history_tableslist', 'awsdms_ddl_audit']
        table_dump_ignore = ['zip3_distances', 'transportation_service_provider_performances','move' ,'move_to_gbloc' , 'archived_access_codes', 'schema_migration', 'audit_history_tableslist']

        # For each table
        for table in tables:
            table_name = table
            # get a list of the table's columns
            cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name=%s", (table_name,))
            column_names = list(cursor.fetchall())

            # Determine whether we have the timestamp fields created_at and updated_at
            found_created_at = False
            found_updated_at = False
            for column in column_names:
                if 'updated_at' in column:
                    found_updated_at = True
                if 'created_at' in column:
                    found_created_at = True

            # By default we will want to write our data to S3, on error we will skip this
            write_to_s3 = True

            # Set the statement timeout to 600 seconds for this session
            cursor.execute("SET statement_timeout = '600s'")

            # If we ignore the table, we do nothing
            if str(table_name) in table_dump_ignore:
                print("Data Warehouse Lambda - INFO - DB Extract - Didn't extract data for table(ignore list): " + str(table_name))
            # If we have neither timestamp field, we do a full dump     
            elif found_updated_at == False and found_created_at == False:
                print("Data Warehouse Lambda - INFO - DB Extract - Performing full dump on " + str(table_name))                
                cursor.execute("SELECT * FROM " + str(table_name))
                    chunk_size = 800000
                    file_increment = 1
                    while True:
                    results = cursor.fetchmany(chunk_size)
                    if not results:
                        break                                         
                    column_names = [desc[0] for desc in cursor.description]
                    data_with_col_names = [{column_names[i]: row[i] for i in range(len(column_names))} for row in results] 
                    json_data = json.dumps(data_with_col_names, cls=UUIDEncoder, default=str)                        
                    if write_to_s3 == True:
                        try:
                            s3 = boto3.client('s3')
                            if file_increment == 1:
                                s3.put_object(Bucket=os.environ['bucket_name'], Key="db_data" + "/" + str(json_parameter_value['data']['serialNumber'] + 1).zfill(6) + "/" + table_name + ".json", Body=json_data, ServerSideEncryption='AES256')
                                print('Data Warehouse Lambda - INFO - DB Extract - Successfully wrote ' + os.environ['bucket_name'] + "/" + "db_data/" + "/"+str(json_parameter_value['data']['serialNumber'] + 1).zfill(6)+"/" + table_name + ".json")
                            else:    
                                s3.put_object(Bucket=os.environ['bucket_name'], Key="db_data" + "/" + str(json_parameter_value['data']['serialNumber'] + 1).zfill(6) + "/" + table_name + "_" + str(file_increment) + ".json", Body=json_data, ServerSideEncryption='AES256')
                                print('Data Warehouse Lambda - INFO - DB Extract - Successfully wrote ' + os.environ['bucket_name'] + "/" + "db_data/" + "/"+str(json_parameter_value['data']['serialNumber'] + 1).zfill(6)+"/" + table_name + "_" + str(file_increment) + ".json")
                            file_increment += 1                            
                        except Exception as e:
                            print("Data Warehouse Lambda - ERROR - DB Extract - Error writing to S3" + str(e))   
                            write_to_s3 = False                
                            break
                write_to_s3 = False
            # If we have created_at but no updated_at, we dump based only on created_at
            elif found_updated_at == False and found_created_at == True:
                last_run_time = json_parameter_value['data']['lastRunTime']
                cursor.execute("SELECT * FROM " + str(table_name) + " where (created_at > '" + str(last_run_time) + "') order by created_at;")
                table_data = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                data_with_col_names = [{column_names[i]: row[i] for i in range(len(column_names))} for row in table_data]
                json_data = json.dumps(data_with_col_names, cls=UUIDEncoder, default=str)
            # If we have created_at and updated_at, we dump based on both
            elif found_updated_at == True and found_created_at == True:
                last_run_time = json_parameter_value['data']['lastRunTime']
                cursor.execute("SELECT * FROM " + str(table_name) + " where ((created_at > %s) OR (updated_at > %s)) order by created_at;", (last_run_time, last_run_time))
                table_data = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description]
                data_with_col_names = [{column_names[i]: row[i] for i in range(len(column_names))} for row in table_data]
                json_data = json.dumps(data_with_col_names, cls=UUIDEncoder, default=str)
            else:
                print('Data Warehouse Lambda - ERROR - DB Extract - ' + str(table_name) + ' does not match any criteria for data warehousing')
                write_to_s3 = False

            # Write the data to S3
            if write_to_s3 == True:
                try:
                    s3 = boto3.client('s3')
                    s3.put_object(Bucket=os.environ['bucket_name'], Key="db_data" + "/" + str(json_parameter_value['data']['serialNumber'] + 1).zfill(6) + "/" + table_name + ".json", Body=json_data, ServerSideEncryption='AES256')
                    print('Data Warehouse Lambda - INFO - DB Extract - Successfully wrote ' + os.environ['bucket_name'] + "/" + "db_data/" + "/"+str(json_parameter_value['data']['serialNumber'] + 1).zfill(6)+"/" + table_name + ".json")
                except Exception as e:
                    print("Data Warehouse Lambda - ERROR - DB Extract - Error writing to S3" + str(e))

       # Create an SSM client
        try:
            ssm_client = boto3.client('ssm')
            json_parameter_value['data']['serialNumber'] += 1
            json_parameter_value['data']['lastRunTime'] = str(current_run_time)
            ssm_client.put_parameter(
                    Name=os.environ['parameter_name'],
                    Value=json.dumps(json_parameter_value),
                    Type='SecureString',
                    Overwrite=True
                )

            #print("Data Warehouse Lambda - INFO - DB Extract - Updated tracking in SSM")
        except Exception as e:
            print("Data Warehouse Lambda - ERROR - DB Extract - Error writing to SSM" + str(e))

    except Exception as e:
        try:
            connection.close()
        except Exception as e:
            connection = None
        print("Data Warehouse Lambda - ERROR - DB Extract - Failed due to :" + str(e))
