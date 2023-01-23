import boto3
import pg8000
import os

def get_connection():
    """
        Method to establish the connection.
    """
    try:
        #print ("Data Warehouse Lambda - Connecting to database")

        # Create a low-level client with the service name for rds
        client = boto3.client("rds")

        # Generates an auth token used to connect to a db with IAM credentials.
        password = client.generate_db_auth_token(
            DBHostname=os.environ['db_end_point'], Port=5432, DBUsername=os.environ['db_user_name'], Region=os.environ['db_region']
        )
        # Establishes the connection with the server using the token generated as password
        conn = pg8000.connect(
            host=os.environ['db_end_point'],
            user=os.environ['db_user_name'],
            database=os.environ['db_name'],
            password=password,
            ssl_context=True,
            timeout=900,
            tcp_keepalive=True,
        )
        return conn
    except Exception as e:
        print ("Data Warehouse Lambda - DB Schema - DB Connection failed: " + str(e))
        return None
