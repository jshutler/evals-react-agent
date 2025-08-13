from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from typing import List, Dict, Any, Optional
import os
from db_constants import SALES_DB_CONNECTION_STRING
from pydantic import BaseModel, Field


class SalesDAO:
    """
    Data Access Object for sales database operations using SQLAlchemy
    """
    
    def __init__(self, connection_string: str):
        self.session: Optional[Session] = None
        self.engine = None
        self.connection_string = connection_string
        self.connect(connection_string)
    
    def __enter__(self):
        """Context manager entry"""
        self.connect(self.connection_string)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def get_engine(self, connection_string: str):
        """
        Create and return SQLAlchemy engine for sales database
        """
        try:
            engine = create_engine(
                connection_string,
                echo=False,  # Set to True for SQL query logging
                pool_size=5,
                max_overflow=10
            )
            
            print("Successfully created SQLAlchemy engine for sales database!")
            return engine
            
        except Exception as error:
            print(f"Error creating SQLAlchemy engine: {error}")
            raise error
    
    def connect(self, connection_string: str) -> bool:
        """Establish database connection"""
        try:
            self.engine = self.get_engine(connection_string)
            if self.engine:
                SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)
                self.session = SessionLocal()
                print("Successfully connected to sales database!")
                return True
            return False
        except Exception as error:
            raise error
    
    def disconnect(self):
        """Close database connection"""
        if self.session:
            self.session.close()
        if self.engine:
            self.engine.dispose()
        print("Database connection closed.")
    
    def get_all_tables(self) -> List[str]:
        """
        Get all table names in the database
        Returns:
            List[str]: List of table names
        """
        if not self.engine:
            return []
        
        try:
            inspector = inspect(self.engine)
            table_names = inspector.get_table_names()
            return table_names
        except Exception as error:
            print(f"Error getting table names: {error}")
            raise error
    
    def get_schema_for_table(self, table_name: str) -> Dict[str, Any]:
        """
        Get schema information for a specific table
        Args:
            table_name (str): Name of the table
        Returns:
            Dict[str, Any]: Schema information including columns, types, constraints
        """
        if not self.engine:
            return {}
        
        try:
            inspector = inspect(self.engine)
            
            # Get column information
            columns = inspector.get_columns(table_name)
            
            # Get primary keys
            primary_keys = inspector.get_pk_constraint(table_name)
            
            # Get foreign keys
            foreign_keys = inspector.get_foreign_keys(table_name)
            
            # Get indexes
            indexes = inspector.get_indexes(table_name)
            
            schema_info = {
                'table_name': table_name,
                'columns': columns,
                'primary_keys': primary_keys,
                'foreign_keys': foreign_keys,
                'indexes': indexes
            }
            
            return schema_info
            
        except Exception as error:
            print(f"Error getting schema for table {table_name}: {error}")
            raise error
    
    def run_sql(self, sql_query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Run arbitrary SQL query and return results
        Args:
            sql_query (str): SQL query to execute
            params (Dict[str, Any], optional): Query parameters
        Returns:
            List[Dict[str, Any]]: Query results as list of dictionaries
        """
        if not self.session:
            return []
        
        try:
            # Execute the query
            if params:
                result = self.session.execute(text(sql_query), params)
            else:
                result = self.session.execute(text(sql_query))
            
            # For SELECT queries, fetch results
            if sql_query.strip().upper().startswith('SELECT'):
                # Convert result to list of dictionaries
                columns = result.keys()
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                return rows
            else:
                # For INSERT, UPDATE, DELETE queries
                self.session.commit()
                return [{'rows_affected': result.rowcount}]
                
        except Exception as error:
            raise error


    

def main():
    """Example usage of the SalesDAO class"""
    
    # Using context manager (recommended)
    with SalesDAO(SALES_DB_CONNECTION_STRING) as dao:
        print("Connected to sales database successfully!")
        
        # Example 1: Get all tables
        print("\n--- All Tables ---")
        tables = dao.get_all_tables()
        for table in tables:
            print(f"  - {table}")
        
        # Example 2: Get schema for a specific table
        if tables:
            print(f"\n--- Schema for '{tables[0]}' table ---")
            schema = dao.get_schema_for_table(tables[0])
            print(f"Table: {schema.get('table_name')}")
            
            print("Columns:")
            for column in schema.get('columns', []):
                print(f"  - {column['name']}: {column['type']} (nullable: {column['nullable']})")
            
            print(f"Primary Keys: {schema.get('primary_keys', {}).get('constrained_columns', [])}")
        
        # Example 3: Run arbitrary SQL
        print("\n--- Running SQL Queries ---")
        
        # SELECT query
        select_result = dao.run_sql("SELECT COUNT(*) as total_count FROM transactions")
        print(f"Total transactions: {select_result[0]['total_count'] if select_result else 0}")
        
        # Another SELECT with parameters
        param_result = dao.run_sql(
            "SELECT * FROM transactions WHERE customer_id = :customer_id LIMIT 5",
            {'customer_id': 1}
        )
        print(f"Transactions for customer 1: {len(param_result)} records")
        print(param_result)

if __name__ == "__main__":
    main()
