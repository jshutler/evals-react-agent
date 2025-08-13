from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from connection import get_sales_db_session, Base
from sqlalchemy import Column, Integer, String, Date, Numeric, DateTime, Text

class Transaction(Base):
    """
    SQLAlchemy model for transactions table
    """
    __tablename__ = 'transactions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    transaction_date = Column(Date, nullable=False)
    customer_id = Column(Integer)
    product_id = Column(Integer)
    quantity = Column(Integer, nullable=False)
    unit_price = Column(Numeric(10, 2), nullable=False)
    total_amount = Column(Numeric(10, 2), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

class SalesDAO:
    """
    Data Access Object for sales database operations using SQLAlchemy
    """
    
    def __init__(self):
        self.session: Optional[Session] = None
    
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
    
    def connect(self) -> bool:
        """Establish database connection"""
        try:
            self.session = get_sales_db_session()
            if self.session:
                print("Successfully connected to sales database!")
                return True
            return False
        except Exception as error:
            print(f"Error connecting to database: {error}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.session:
            self.session.close()
            print("Database connection closed.")
    
    def commit(self):
        """Commit current transaction"""
        if self.session:
            self.session.commit()
    
    def rollback(self):
        """Rollback current transaction"""
        if self.session:
            self.session.rollback()
    
    # Transaction operations
    def get_all_transactions(self) -> List[Transaction]:
        """Get all transactions ordered by date descending"""
        if not self.session:
            return []
        
        try:
            return self.session.query(Transaction).order_by(desc(Transaction.transaction_date)).all()
        except Exception as error:
            print(f"Error fetching transactions: {error}")
            return []
    
    def get_transaction_by_id(self, transaction_id: int) -> Optional[Transaction]:
        """Get transaction by ID"""
        if not self.session:
            return None
        
        try:
            return self.session.query(Transaction).filter(Transaction.id == transaction_id).first()
        except Exception as error:
            print(f"Error fetching transaction {transaction_id}: {error}")
            return None
    
    def get_transactions_by_date_range(self, start_date: date, end_date: date) -> List[Transaction]:
        """Get transactions within a date range"""
        if not self.session:
            return []
        
        try:
            return self.session.query(Transaction).filter(
                Transaction.transaction_date.between(start_date, end_date)
            ).order_by(desc(Transaction.transaction_date)).all()
        except Exception as error:
            print(f"Error fetching transactions by date range: {error}")
            return []
    
    def get_transactions_by_customer(self, customer_id: int) -> List[Transaction]:
        """Get all transactions for a specific customer"""
        if not self.session:
            return []
        
        try:
            return self.session.query(Transaction).filter(
                Transaction.customer_id == customer_id
            ).order_by(desc(Transaction.transaction_date)).all()
        except Exception as error:
            print(f"Error fetching transactions for customer {customer_id}: {error}")
            return []
    
    def insert_transaction(self, transaction_data: Dict[str, Any]) -> Optional[Transaction]:
        """Insert a new transaction"""
        if not self.session:
            return None
        
        try:
            # Calculate total amount if not provided
            if 'total_amount' not in transaction_data:
                quantity = transaction_data.get('quantity', 0)
                unit_price = transaction_data.get('unit_price', 0)
                transaction_data['total_amount'] = Decimal(str(quantity)) * Decimal(str(unit_price))
            
            # Convert date string to date object if needed
            if isinstance(transaction_data.get('transaction_date'), str):
                transaction_data['transaction_date'] = datetime.strptime(
                    transaction_data['transaction_date'], '%Y-%m-%d'
                ).date()
            
            new_transaction = Transaction(**transaction_data)
            self.session.add(new_transaction)
            self.session.commit()
            
            print(f"Successfully inserted transaction with ID: {new_transaction.id}")
            return new_transaction
            
        except Exception as error:
            print(f"Error inserting transaction: {error}")
            self.session.rollback()
            return None
    
    def update_transaction(self, transaction_id: int, update_data: Dict[str, Any]) -> bool:
        """Update an existing transaction"""
        if not self.session:
            return False
        
        try:
            transaction = self.get_transaction_by_id(transaction_id)
            if not transaction:
                print(f"Transaction {transaction_id} not found")
                return False
            
            # Update fields
            for key, value in update_data.items():
                if hasattr(transaction, key):
                    setattr(transaction, key, value)
            
            # Recalculate total amount if quantity or unit_price changed
            if 'quantity' in update_data or 'unit_price' in update_data:
                transaction.total_amount = Decimal(str(transaction.quantity)) * Decimal(str(transaction.unit_price))
            
            self.session.commit()
            print(f"Successfully updated transaction {transaction_id}")
            return True
            
        except Exception as error:
            print(f"Error updating transaction {transaction_id}: {error}")
            self.session.rollback()
            return False
    
    def delete_transaction(self, transaction_id: int) -> bool:
        """Delete a transaction"""
        if not self.session:
            return False
        
        try:
            transaction = self.get_transaction_by_id(transaction_id)
            if not transaction:
                print(f"Transaction {transaction_id} not found")
                return False
            
            self.session.delete(transaction)
            self.session.commit()
            print(f"Successfully deleted transaction {transaction_id}")
            return True
            
        except Exception as error:
            print(f"Error deleting transaction {transaction_id}: {error}")
            self.session.rollback()
            return False
    
    # Analytics methods
    def get_transaction_count(self) -> int:
        """Get total number of transactions"""
        if not self.session:
            return 0
        
        try:
            return self.session.query(func.count(Transaction.id)).scalar()
        except Exception as error:
            print(f"Error getting transaction count: {error}")
            return 0
    
    def get_total_sales(self) -> Decimal:
        """Get total sales amount"""
        if not self.session:
            return Decimal('0')
        
        try:
            result = self.session.query(func.sum(Transaction.total_amount)).scalar()
            return result if result else Decimal('0')
        except Exception as error:
            print(f"Error getting total sales: {error}")
            return Decimal('0')
    
    def get_sales_by_date_range(self, start_date: date, end_date: date) -> Decimal:
        """Get total sales within a date range"""
        if not self.session:
            return Decimal('0')
        
        try:
            result = self.session.query(func.sum(Transaction.total_amount)).filter(
                Transaction.transaction_date.between(start_date, end_date)
            ).scalar()
            return result if result else Decimal('0')
        except Exception as error:
            print(f"Error getting sales by date range: {error}")
            return Decimal('0')
    
    def get_top_customers(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get top customers by total purchase amount"""
        if not self.session:
            return []
        
        try:
            results = self.session.query(
                Transaction.customer_id,
                func.sum(Transaction.total_amount).label('total_purchases'),
                func.count(Transaction.id).label('transaction_count')
            ).group_by(Transaction.customer_id).order_by(
                desc(func.sum(Transaction.total_amount))
            ).limit(limit).all()
            
            return [
                {
                    'customer_id': result.customer_id,
                    'total_purchases': result.total_purchases,
                    'transaction_count': result.transaction_count
                }
                for result in results
            ]
        except Exception as error:
            print(f"Error getting top customers: {error}")
            return []

def main():
    """Example usage of the SalesDAO class"""
    
    # Using context manager (recommended)
    with SalesDAO() as dao:
        print("Connected to sales database successfully!")
        
        # Example queries
        print("\n--- Database Operations ---")
        
        # Get transaction count
        count = dao.get_transaction_count()
        print(f"Total transactions: {count}")
        
        # Get total sales
        total_sales = dao.get_total_sales()
        print(f"Total sales: ${total_sales:,.2f}")
        
        # Get all transactions (limit to 5 for display)
        transactions = dao.get_all_transactions()
        if transactions:
            print(f"\nRecent transactions (showing first 5):")
            for i, transaction in enumerate(transactions[:5]):
                print(f"  {i+1}. ID: {transaction.id}, Date: {transaction.transaction_date}, "
                      f"Amount: ${transaction.total_amount}")
        
        # Example: Insert a new transaction
        print("\n--- Inserting new transaction ---")
        new_transaction = dao.insert_transaction({
            'transaction_date': '2024-01-15',
            'customer_id': 1,
            'product_id': 1,
            'quantity': 5,
            'unit_price': 29.99
        })
        
        if new_transaction:
            print(f"Successfully inserted transaction with ID: {new_transaction.id}")
        
        # Example: Get top customers
        print("\n--- Top Customers ---")
        top_customers = dao.get_top_customers(5)
        for customer in top_customers:
            print(f"Customer {customer['customer_id']}: ${customer['total_purchases']:,.2f} "
                  f"({customer['transaction_count']} transactions)")

if __name__ == "__main__":
    main()
