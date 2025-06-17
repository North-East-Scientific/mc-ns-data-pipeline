# database/connection.py
"""
Database connection and schema management
"""
import psycopg2
import logging
from sqlalchemy import create_engine
from sql.schema import (
    CREATE_LOTS_TABLE, 
    CREATE_LOT_DATA_TABLE, 
    CREATE_PROCESSING_HISTORY_TABLE, 
    CREATE_INDEXES
)
from config.settings import Config

class DatabaseManager:
    def __init__(self):
        self.config = Config.DB_CONFIG
    
    def get_connection(self):
        """Create a connection to the PostgreSQL database"""
        try:
            conn = psycopg2.connect(
                dbname=self.config['dbname'],
                user=self.config['user'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port']
            )
            return conn
        except Exception as e:
            logging.error(f"Database connection error: {e}")
            raise
    
    def get_sqlalchemy_engine(self):
        """Create SQLAlchemy engine for pandas DataFrame operations"""
        connection_string = f"postgresql+psycopg2://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['dbname']}"
        return create_engine(connection_string)
    
    def create_schema(self, conn):
        """Create the necessary database schema if it doesn't exist"""
        with conn.cursor() as cursor:
            # Create tables
            cursor.execute(CREATE_LOTS_TABLE)
            cursor.execute(CREATE_LOT_DATA_TABLE)
            cursor.execute(CREATE_PROCESSING_HISTORY_TABLE)
            
            # Create indexes
            for index_sql in CREATE_INDEXES:
                cursor.execute(index_sql)
            
            conn.commit()
            logging.info("Database schema created/confirmed")
