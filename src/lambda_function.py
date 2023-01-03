import db_schema_dump
import db_dump

def lambda_handler(event, context):

    print("Data Warehouse Lambda - INFO - Export starting")


    # DB Schema export
    try:
        db_schema_dump.db_schema_dump()
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Schema - Failure: " + str(e))

    # DB Data export
    try:
        db_dump.db_data_dump()
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Data - Failure:" + str(e))


    print("Data Warehouse Lambda - INFO - Export finished")



