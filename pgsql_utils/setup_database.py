import psycopg2
from psycopg2 import Error
import os
import pandas as pd

def test_connection_parameters():
    """Test different common PostgreSQL connection scenarios"""
    
    # Common connection scenarios to try
    connection_configs = [
        {
            "dbname": "postgres",
            "user": "postgres", 
            "password": "password",
            "host": "localhost",
            "port": "5432"
        },
        {
            "dbname": "postgres",
            "user": "postgres", 
            "password": "",  # No password
            "host": "localhost",
            "port": "5432"
        },
        {
            "dbname": "postgres",
            "user": os.getenv("USER", "jshutler"),  # Your system username
            "password": "",
            "host": "localhost",
            "port": "5432"
        },
        {
            "dbname": "template1",
            "user": "postgres", 
            "password": "password",
            "host": "localhost",
            "port": "5432"
        }
    ]
    
    for i, config in enumerate(connection_configs):
        try:
            print(f"Trying connection config {i+1}: user='{config['user']}', dbname='{config['dbname']}', password={'***' if config['password'] else 'None'}")
            conn = psycopg2.connect(**config)
            conn.close()
            print(f"✓ Connection successful with config {i+1}")
            return config
        except Exception as e:
            print(f"✗ Config {i+1} failed: {e}")
    
    return None

def setup_database():
    conn = None
    try:
        # First, test which connection parameters work
        print("Testing PostgreSQL connection parameters...")
        working_config = test_connection_parameters()
        
        if not working_config:
            print("\n❌ Could not establish any connection to PostgreSQL.")
            print("Please ensure:")
            print("1. PostgreSQL is installed and running")
            print("2. You know the correct username/password")
            print("3. PostgreSQL is running on port 5432")
            print("\nTo start PostgreSQL (if installed via Homebrew):")
            print("   brew services start postgresql")
            return False
        
        print(f"\n✓ Using working connection: {working_config}")
        
        # Connect with working parameters
        conn = psycopg2.connect(**working_config)
        conn.autocommit = True
        cursor = conn.cursor()

        # Create user if it doesn't exist
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname='jshutler'")
        if not cursor.fetchone():
            cursor.execute("CREATE USER jshutler WITH PASSWORD 'password'")
            print("Created user 'jshutler'")

        # Drop database if it exists, then create it fresh
        cursor.execute("SELECT 1 FROM pg_database WHERE datname='sales_db'")
        if cursor.fetchone():
            # Terminate existing connections to the database
            cursor.execute("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = 'sales_db' AND pid <> pg_backend_pid()
            """)
            cursor.execute("DROP DATABASE sales_db")
            print("Dropped existing database 'sales_db'")
        
        cursor.execute("CREATE DATABASE sales_db")
        cursor.execute("GRANT ALL PRIVILEGES ON DATABASE sales_db TO jshutler")
        print("Created database 'sales_db' and granted privileges")

        # Close initial connection
        cursor.close()
        conn.close()
        # Connect to the sales_db database as postgres to grant schema permissions
        conn = psycopg2.connect(
            dbname="sales_db",
            user="postgres",
            password="password",
            host="localhost", 
            port="5432"
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Grant schema permissions to jshutler user
        cursor.execute("GRANT ALL ON SCHEMA public TO jshutler")
        cursor.execute("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO jshutler")
        cursor.execute("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO jshutler")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO jshutler")
        cursor.execute("ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO jshutler")
        print("Granted schema permissions to jshutler user")
        
        # Close postgres connection
        cursor.close()
        conn.close()

        # Connect to the sales_db database as jshutler
        conn = psycopg2.connect(
            dbname="sales_db",
            user="jshutler",
            password="password",
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
        df = pd.read_csv('sales_data.csv')
        
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