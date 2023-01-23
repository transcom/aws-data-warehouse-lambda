import db_schema_dump
import db_extractor_full
#import db_extractor_history

def lambda_handler(event, context):

    print("Data Warehouse Lambda - INFO - Export starting")

    # DB Schema Export
    try:
        db_schema_dump.db_schema_dump()
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Schema - Failure: " + str(e))

    # DB Extractor - Full
    try:
        db_extractor_full.db_extractor()
    except Exception as e:
        print("Data Warehouse Lambda - ERROR - DB Extractor - Full - Failure: " + str(e))

    print("Data Warehouse Lambda - INFO - Export finished")

    # DB Extractor - History Table
#    try:
#        db_extractor_history.db_extractor()
#    except Exception as e:
#        print("Data Warehouse Lambda - ERROR - DB Extractor - History - Failure: " + str(e))
#
#    print("Data Warehouse Lambda - INFO - Export finished")

