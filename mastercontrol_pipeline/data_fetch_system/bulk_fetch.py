# main.py
"""Main script orchestrating the bulk data extraction process."""

import argparse
import logging
import os
import pandas as pd
import numpy as np
from data_processor import DataProcessor
from checkpoint_manager import CheckpointManager
from status_tracker import StatusTracker
from utils import reformat_datetime
from config import (
    DEFAULT_END_ID, OUTPUT_DATA_DIR, LOG_FILE_PATH
)


def setup_logging():
    """Configure logging."""
    logging.basicConfig(
        filename=LOG_FILE_PATH,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def setup_argument_parser():
    """Set up command line argument parsing."""
    parser = argparse.ArgumentParser(
        description='Process production records with checkpoint capability'
    )
    parser.add_argument('--start', type=int, help='Starting production record ID')
    parser.add_argument('--end', type=int, help='Ending production record ID')
    parser.add_argument('--batch_size', type=int, default=100, help='Checkpoint batch size')
    return parser.parse_args()


class ProductionRecordPipeline:
    """Main pipeline for processing production records."""
    
    def __init__(self):
        self.data_processor = DataProcessor()
        self.status_tracker = StatusTracker()
        self.checkpoint_manager = CheckpointManager()
    
    def process_record(self, production_record_id):
        """Process a single production record."""
        try:
            logging.info(f"Processing production record ID: {production_record_id}")
            
            # Fetch production record data
            lot_number, filtered_record_data_df = self.data_processor.fetch_production_record_data(
                production_record_id
            )
            
            if filtered_record_data_df.shape[0] == 0:
                logging.warning(f"No data for production record ID: {production_record_id}")
                self.status_tracker.log_status(
                    production_record_id, None, "Fail", 
                    "fetch_production_record_data returned empty"
                )
                return 0
            
            # Fetch batch records
            filtered_data_capture_df = self.data_processor.fetch_batch_records_by_lot(lot_number)
            
            # Fetch structure metadata
            unit_df, operation_df, phase_df = self.data_processor.fetch_production_structure_metadata(
                production_record_id
            )
            
            # Try alternative API if first batch records call fails
            if filtered_data_capture_df.shape[0] == 0:
                filtered_data_capture_df = self.data_processor.fetch_data_capture_by_lot(lot_number)
                if filtered_data_capture_df.shape[0] == 0:
                    logging.warning(f"No data for lot number: {lot_number}")
                    self.status_tracker.log_status(
                        production_record_id, lot_number, "Fail",
                        "Both api_1 calls returned empty"
                    )
                    return 0
            
            # Merge and process data
            merged_df = self._merge_data(
                filtered_record_data_df, filtered_data_capture_df,
                unit_df, operation_df, phase_df
            )
            
            # Save to CSV
            file_name = f"{OUTPUT_DATA_DIR}{lot_number}.csv"
            merged_df.to_csv(file_name, index=False)
            
            # Log success
            self.status_tracker.log_status(production_record_id, lot_number, "Success")
            logging.info(f"Successfully processed production record ID {production_record_id}")
            
        except Exception as e:
            logging.error(f"Error processing production record ID {production_record_id}: {e}", exc_info=True)
            self.status_tracker.log_status(production_record_id, None, "Fail", str(e))
    
    def _merge_data(self, record_df, data_capture_df, unit_df, operation_df, phase_df):
        """Merge all data sources into final DataFrame."""
        merged_df = record_df.copy()
        
        # Add basic metadata
        merged_df.loc[:, 'Master Template Name'] = data_capture_df['productName'].unique()[0]
        merged_df.loc[:, 'Lot Number'] = data_capture_df['lotNumber'].unique()[0]
        merged_df.loc[:, 'Product ID'] = data_capture_df['productId'].unique()[0]
        merged_df.loc[:, 'Production Record Status'] = data_capture_df['status'].unique()[0]
        
        # Merge structure data with error handling
        merge_keys = ['masterTemplateId', 'unitProcedureId']
        
        try:
            merged_df = merged_df.merge(unit_df, how='left', on=merge_keys)
            try:
                merged_df = merged_df.merge(
                    operation_df, how='left', 
                    on=merge_keys + ['operationId']
                )
                try:
                    merged_df = merged_df.merge(
                        phase_df, how='left',
                        on=merge_keys + ['operationId', 'phaseId']
                    )
                except ValueError as e:
                    logging.warning(f"Phase merge error: {e}")
                    merged_df.loc[:, 'Phase'] = ""
            except ValueError as e:
                logging.warning(f"Operation merge error: {e}")
                merged_df.loc[:, 'Operation'] = ""
                merged_df.loc[:, 'Phase'] = ""
        except ValueError as e:
            logging.warning(f"Unit merge error: {e}")
            merged_df.loc[:, 'Unit'] = ""
            merged_df.loc[:, 'Operation'] = ""
            merged_df.loc[:, 'Phase'] = ""
        
        # Select and rename final columns
        final_columns = [
            'Master Template Name', 'Lot Number', 'Product ID', 'Unit', 'Operation', 'Phase',
            'dateTime', 'Production Record Status', 'orderLabel', 'title', 'value',
            'userName', 'actionTaken', 'dataCaptureName'
        ]
        
        final_df = merged_df[final_columns]
        final_df.columns = [
            'Master Template Name', 'Lot Number', 'Product ID', 'Unit', 'Operation', 'Phase',
            'Data Capture Time', 'Production Record Status', 'Structure Label', 'Description',
            'Input Data Value', 'Performed By', 'Action Performed', 'Captured Data Type'
        ]
        
        # Clean and format data
        final_df = final_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        final_df['Data Capture Time'] = final_df['Data Capture Time'].apply(reformat_datetime)
        
        # Filter out VOD_ users
        final_df = final_df[~final_df['Performed By'].str.startswith('VOD_')]
        
        return final_df
    
    def run(self, start_id, end_id, batch_size):
        """Run the complete pipeline."""
        logging.info(f"Starting processing from ID {start_id} to {end_id}")
        
        current_batch_start = start_id
        
        while current_batch_start <= end_id:
            current_batch_end = min(current_batch_start + batch_size - 1, end_id)
            
            logging.info(f"Processing batch from {current_batch_start} to {current_batch_end}")
            
            for production_record_id in range(current_batch_start, current_batch_end + 1):
                self.process_record(production_record_id)
            
            # Save checkpoint after each batch
            self.checkpoint_manager.save_checkpoint(current_batch_end)
            current_batch_start = current_batch_end + 1
        
        logging.info("Processing complete")


def main():
    """Entry point for the script."""
    setup_logging()
    args = setup_argument_parser()
    
    # Ensure output directory exists
    os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
    
    # Load checkpoint and determine start ID
    checkpoint = CheckpointManager.load_checkpoint()
    start_id = args.start if args.start is not None else (checkpoint['last_processed_id'] + 1)
    
    if start_id <= 0:
        start_id = 0
    
    end_id = args.end if args.end is not None else DEFAULT_END_ID
    batch_size = args.batch_size
    
    # Run pipeline
    pipeline = ProductionRecordPipeline()
    pipeline.run(start_id, end_id, batch_size)


main()
