# processors/file_processor.py
"""
File processing logic for CSV files - supports both bulk and incremental loading
"""
import os
import pandas as pd
import hashlib
import logging
from database.operations import DatabaseOperations
from config.settings import Config

class FileProcessor:
    @staticmethod
    def calculate_row_hash(row):
        """Calculate a hash for a row to identify unique records and changes"""
        key_data = f"{row['Structure Label']}|{row['Description']}|{row['Input Data Value']}|{row['Data Capture Time']}"
        return hashlib.md5(key_data.encode()).hexdigest()
    
    @staticmethod
    def extract_lot_info(df, filename_lot_number):
        """Extract lot information from DataFrame"""
        lot_number = filename_lot_number
        
        # Extract lot info from first row
        if 'Lot Number' in df.columns and len(df) > 0:
            extracted_lot_number = df['Lot Number'].iloc[0]
            if extracted_lot_number != filename_lot_number:
                logging.warning(f"Filename lot number ({filename_lot_number}) differs from content lot number ({extracted_lot_number})")
                lot_number = extracted_lot_number
        
        product_id = df['Product ID'].iloc[0] if 'Product ID' in df.columns and len(df) > 0 else ''
        product_name = df['Master Template Name'].iloc[0] if 'Master Template Name' in df.columns and len(df) > 0 else ''
        status = df['Production Record Status'].iloc[0] if 'Production Record Status' in df.columns and len(df) > 0 else ''
        
        return lot_number, product_id, product_name, status
    
    @staticmethod
    def prepare_data_for_insert(df, lot_number):
        """Prepare DataFrame data for database insertion"""
        data_to_insert = []
        for _, row in df.iterrows():
            data_to_insert.append((
                lot_number,
                row.get('Master Template Name', ''),
                row.get('Unit', ''),
                row.get('Operation', ''),
                row.get('Phase', ''),
                row.get('Data Capture Time', None),
                row.get('Structure Label', ''),
                row.get('Description', ''),
                row.get('Input Data Value', ''),
                row.get('Performed By', ''),
                row.get('Action Performed', ''),
                row.get('Captured Data Type', ''),
                row.get('data_hash', '')
            ))
        return data_to_insert
    
    def process_csv_file(self, file_path, conn, process_type='initial_load'):
        """Process a single CSV file and load it into the database"""
        filename = os.path.basename(file_path)
        filename_lot_number = os.path.splitext(filename)[0]
        db_ops = DatabaseOperations()

        try:
            # Read CSV file
            df = pd.read_csv(file_path)
            if df.empty:
                logging.warning(f"Empty CSV file: {file_path}")
                return 0

            record_count = len(df)
            logging.info(f"Processing {filename} with {record_count} records (process_type: {process_type})")

            # Extract lot information
            lot_number, product_id, product_name, status = self.extract_lot_info(df, filename_lot_number)

            # Add hash column for record uniqueness
            df['data_hash'] = df.apply(self.calculate_row_hash, axis=1)

            # Format date column
            if 'Data Capture Time' in df.columns:
                df['Data Capture Time'] = pd.to_datetime(df['Data Capture Time'], errors='coerce')

            with conn.cursor() as cursor:
                # Check if lot exists
                lot_exists = db_ops.check_lot_exists(cursor, lot_number)

                # Insert or update lots table
                if lot_exists:
                    db_ops.update_lot(cursor, lot_number, product_id, product_name, status)
                else:
                    db_ops.insert_lot(cursor, lot_number, product_id, product_name, status)

                # FULL REPLACEMENT LOGIC FOR INCREMENTAL LOAD
                if process_type == 'incremental_load':
                    db_ops.delete_lot_data(cursor, lot_number)
                    logging.info(f"Deleted existing data for lot {lot_number} from lot_data")

                # Prepare and insert lot data
                data_to_insert = self.prepare_data_for_insert(df, lot_number)
                db_ops.insert_lot_data(cursor, data_to_insert)

                # Log processing history
                db_ops.log_processing_history(
                    cursor, filename, lot_number, process_type, len(data_to_insert),
                    os.path.dirname(file_path), Config.DIR_STRUCTURE['processed_data'],
                    'success', f"Processed {len(data_to_insert)} records"
                )

                conn.commit()
                logging.info(f"Successfully processed {filename}, inserted {len(data_to_insert)} records")
                return len(data_to_insert)

        except Exception as e:
            conn.rollback()
            logging.error(f"Error processing {file_path}: {e}", exc_info=True)

            # Record failure
            with conn.cursor() as cursor:
                db_ops.log_processing_history(
                    cursor, filename, filename_lot_number, process_type, 0,
                    os.path.dirname(file_path), '', 'error', str(e)
                )
                conn.commit()
            return 0
