# database/operations.py
"""
Database operations for bulk loading and incremental updates
"""
import logging
from psycopg2.extras import execute_values
from sql.queries import (
    CHECK_LOT_EXISTS, INSERT_LOT, UPDATE_LOT, DELETE_LOT_DATA, 
    INSERT_LOT_DATA, INSERT_PROCESSING_HISTORY,
    GET_RECENT_PROCESSING_HISTORY, GET_RECENT_LOT_UPDATES
)

class DatabaseOperations:
    @staticmethod
    def check_lot_exists(cursor, lot_number):
        """Check if lot already exists in database"""
        cursor.execute(CHECK_LOT_EXISTS, (lot_number,))
        return cursor.fetchone()
    
    @staticmethod
    def insert_lot(cursor, lot_number, product_id, product_name, status):
        """Insert new lot information"""
        cursor.execute(INSERT_LOT, (lot_number, product_id, product_name, status))
    
    @staticmethod
    def update_lot(cursor, lot_number, product_id, product_name, status):
        """Update existing lot information"""
        cursor.execute(UPDATE_LOT, (product_id, product_name, status, lot_number))
    
    @staticmethod
    def delete_lot_data(cursor, lot_number):
        """Delete existing lot data for incremental replacement"""
        cursor.execute(DELETE_LOT_DATA, (lot_number,))
    
    @staticmethod
    def insert_lot_data(cursor, data_to_insert):
        """Insert lot data in batch"""
        execute_values(cursor, INSERT_LOT_DATA, data_to_insert)
    
    @staticmethod
    def log_processing_history(cursor, filename, lot_number, process_type, record_count, 
                             source_dir, target_dir, status, message):
        """Log file processing history"""
        cursor.execute(INSERT_PROCESSING_HISTORY, 
                      (filename, lot_number, process_type, record_count, 
                       source_dir, target_dir, status, message))
    
    @staticmethod
    def get_recent_processing_history(cursor):
        """Get recent file processing history for reports"""
        cursor.execute(GET_RECENT_PROCESSING_HISTORY)
        return cursor.fetchall()
    
    @staticmethod
    def get_recent_lot_updates(cursor):
        """Get recent lot updates for reports"""
        cursor.execute(GET_RECENT_LOT_UPDATES)
        return cursor.fetchall()
