import psycopg2
from psycopg2 import Error
import os
import pandas as pd

def setup_database():
    conn = None
    try:
        # First connect to default 'postgres' database
        conn = psycopg2.connect(
            dbname="postgres",
            user="postgres",
            password="password",  # Make sure to use your actual password
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create a new database
        db_name = "sales_db"
        cursor.execute(f"DROP DATABASE IF EXISTS {db_name}")
        cursor.execute(f"CREATE DATABASE {db_name}")
        print(f"Database '{db_name}' created successfully!")
        
        # Close connection to postgres database
        cursor.close()
        conn.close()
        
        # Connect to our newly created database
        conn = psycopg2.connect(
            dbname=db_name,
            user="postgres",
            password="password",  # Make sure to use your actual password
            host="localhost",
            port="5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create single table matching CSV structure
        cursor.execute("""
            CREATE TABLE sales_data (
                first_name VARCHAR(100),
                last_name VARCHAR(100),
                customer_id INTEGER,
                purchase_id INTEGER,
                item_purchased VARCHAR(100),
                item_id INTEGER,
                item_description VARCHAR(255),
                purchase_price DECIMAL(10,2),
                returned INTEGER
            )
        """)
        
        print("Table created successfully!")
        
        # Load data from CSV
        df = pd.read_csv('/home/jshutler/Desktop/code/evals-react-agent/sales_data.csv')
        
        # Insert data
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO sales_data (
                    first_name, last_name, customer_id, purchase_id,
                    item_purchased, item_id, item_description,
                    purchase_price, returned
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['first_name'], row['last_name'], row['customer_id'],
                row['purchase_id'], row['item_purchased'], row['item_id'],
                row['item_description'], row['purchase_price'], row['returned']
            ))
        
        print("Data imported successfully!")
        return True
        
    except (Exception, Error) as error:
        print(f"Error: {error}")
        return False
        
    finally:
        if conn:
            cursor.close()
            conn.close()

if __name__ == "__main__":
    if setup_database():
        print("Database setup completed successfully!")
    else:
        print("Database setup failed!")