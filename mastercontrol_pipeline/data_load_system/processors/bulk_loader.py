# processors/bulk_loader.py
"""
Main bulk loader class that orchestrates the loading process
"""
import os
import glob
import logging
from config.settings import Config
from database.connection import DatabaseManager
from processors.file_processor import FileProcessor
from utils.file_utils import FileUtils

class BulkLoader:
    def __init__(self):
        self.db_manager = DatabaseManager()
        self.file_processor = FileProcessor()
        self.file_utils = FileUtils()
    
    def setup_logging(self):
        """Setup logging configuration"""
        logging.basicConfig(
            filename=Config.LOG_FILE_PATH,
            level=getattr(logging, Config.LOG_LEVEL),
            format=Config.LOG_FORMAT
        )
    
    def bulk_load_initial_data(self):
        """Load all CSV files from the raw_data directory into the database"""
        # Setup
        self.setup_logging()
        self.file_utils.create_directories()
        
        # Database setup
        conn = self.db_manager.get_connection()
        self.db_manager.create_schema(conn)
        
        # Find CSV files
        csv_files = glob.glob(os.path.join(Config.DIR_STRUCTURE['raw_data'], '*.csv'))
        logging.info(f"Found {len(csv_files)} CSV files for initial bulk load")
        
        if not csv_files:
            logging.warning("No CSV files found in raw_data directory")
            conn.close()
            return 0, 0
        
        # Process files
        total_processed = 0
        total_records = 0
        
        for file_path in csv_files:
            record_count = self.file_processor.process_csv_file(file_path, conn, 'initial_load')
            total_records += record_count
            if record_count > 0:
                self.file_utils.archive_file(file_path, 'initial_load')
                total_processed += 1
        
        conn.close()
        
        # Log and print results
        result_message = f"Bulk load complete. Processed {total_processed}/{len(csv_files)} files, {total_records} total records"
        logging.info(result_message)
        print(result_message)
        
        return total_processed, total_records
    
    def process_new_data(self):
        """Process any new data files found in the new_data directory"""
        # Setup
        self.setup_logging()
        
        # Database setup
        conn = self.db_manager.get_connection()
        
        # Find CSV files
        csv_files = glob.glob(os.path.join(Config.DIR_STRUCTURE['new_data'], '*.csv'))
        logging.info(f"Found {len(csv_files)} CSV files for incremental load")
        
        # Process files
        total_processed = 0
        total_records = 0
        
        for file_path in csv_files:
            record_count = self.file_processor.process_csv_file(file_path, conn, 'incremental_load')
            total_records += record_count
            if record_count > 0:
                self.file_utils.archive_file(file_path, 'incremental_load')
                total_processed += 1
        
        conn.close()
        
        # Log and print results
        result_message = f"Incremental load complete. Processed {total_processed}/{len(csv_files)} files, {total_records} total records"
        logging.info(result_message)
        print(result_message)
        
        return total_processed, total_records
