import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from db_constants import SALES_DB_CONNECTION_STRING, TRANSACTIONS_TABLE_NAME
# Read Excel file
df = pd.read_csv('sales_data.csv')

# Connect to PostgreSQL
engine = create_engine(SALES_DB_CONNECTION_STRING)

# Import to database
df.to_sql(TRANSACTIONS_TABLE_NAME, engine, if_exists='replace', index=False)