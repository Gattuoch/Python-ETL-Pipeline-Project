# src/schema/db_connection.py
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

class Database:
    def __init__(self, connection_url: str = None):
        """
        Initializes the Database connection.
        Uses DATABASE_URL from .env if no connection URL is passed.
        """
        if connection_url is None:
            connection_url = os.getenv("DATABASE_URL")
            if not connection_url:
                raise ValueError("❌ DATABASE_URL is not set in the .env file")
        
        try:
            self.connection_url = connection_url
            self.engine: Engine = create_engine(self.connection_url)
        except Exception as e:
            raise Exception(f"❌ Failed to create engine: {e}")

    def get_engine(self) -> Engine:
        """Returns the SQLAlchemy engine."""
        return self.engine

    def test_connection(self):
        """Test if the database connection works."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text("SELECT version();"))
                print("✅ Connected successfully to database!")
                for row in result:
                    print("PostgreSQL Version:", row[0])
        except Exception as e:
            print("❌ Database connection failed:", e)

# Test connection when running this file directly
if __name__ == "__main__":
    db = Database()
    db.test_connection()
