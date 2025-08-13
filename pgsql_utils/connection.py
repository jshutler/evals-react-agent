import psycopg2
import os
from psycopg2 import Error
from db_constants import SALES_DB_CONNECTION_STRING

def get_pgsql_connection(connection_string: str):
    """
    Connect to sales database using environment variables
    """
    try:
        connection = psycopg2.connect(connection_string)
        
        print("Successfully connected to sales database!")
        return connection
        
    except (Exception, Error) as error:
        print(f"Error connecting to PostgreSQL: {error}")
        return None