# incremental_fetch.py
"""Main script orchestrating the data extraction process for incremental loads."""

import argparse
import logging
import os
import datetime
from datetime import timedelta, timezone

import numpy as np
import pandas as pd

from config import INCR_OUTPUT_DATA_DIR, INCR_LOG_FILE_PATH, BATCH_SIZE
from data_processor import DataProcessor
from checkpoint_manager import CheckpointManager
from status_tracker import StatusTracker
from utils import reformat_datetime


SIX_HOUR_WINDOW_CSV = '../six_hour_windows.csv'
SIX_HOURS_IN_SECONDS = 6 * 3600
FIRST_RUN_EPOCH = 1741564801


def setup_logging():
    """Configure logging."""
    logging.basicConfig(
        filename=INCR_LOG_FILE_PATH,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


class ProductionRecordPipeline:
    """Main pipeline for processing production records."""

    def __init__(self):
        self.data_processor = DataProcessor()
        self.status_tracker = StatusTracker()
        self.checkpoint_manager = CheckpointManager()

    def run(self, id_list, batch_size):
        """Run the complete pipeline using a list of IDs."""
        logging.info(f"Starting processing of {len(id_list)} records")
        for i in range(0, len(id_list), batch_size):
            batch_ids = id_list[i:i + batch_size]
            logging.info(f"Processing batch: {batch_ids}")
            for production_record_id in batch_ids:
                self.process_record(production_record_id)
            self.checkpoint_manager.save_checkpoint(batch_ids[-1])
        logging.info("Processing complete")

    def process_record(self, production_record_id):
        """Process a single production record."""
        try:
            logging.info(f"Processing production record ID: {production_record_id}")
            lot_number, record_df = self.data_processor.fetch_production_record_data(production_record_id)

            if record_df.empty:
                self._log_failure(production_record_id, "fetch_production_record_data returned empty")
                return

            capture_df = self.data_processor.fetch_batch_records_by_lot(lot_number)
            if capture_df.empty:
                capture_df = self.data_processor.fetch_data_capture_by_lot(lot_number)
                if capture_df.empty:
                    self._log_failure(production_record_id, "Both API calls returned empty", lot_number)
                    return

            unit_df, op_df, phase_df = self.data_processor.fetch_production_structure_metadata(production_record_id)
            merged_df = self._merge_data(record_df, capture_df, unit_df, op_df, phase_df)

            output_file = os.path.join(INCR_OUTPUT_DATA_DIR, f"{lot_number}.csv")
            merged_df.to_csv(output_file, index=False)

            self.status_tracker.log_status(production_record_id, lot_number, "Success")
            logging.info(f"Successfully processed production record ID {production_record_id}")

        except Exception as e:
            logging.error(f"Error processing production record ID {production_record_id}: {e}", exc_info=True)
            self.status_tracker.log_status(production_record_id, None, "Fail", str(e))

    def _log_failure(self, record_id, reason, lot_number=None):
        logging.warning(f"{reason} for record ID: {record_id}")
        self.status_tracker.log_status(record_id, lot_number, "Fail", reason)

    def _merge_data(self, record_df, capture_df, unit_df, op_df, phase_df):
        """Merge all data sources into final DataFrame."""
        merged_df = record_df.copy()

        # Add basic metadata
        merged_df['Master Template Name'] = capture_df['productName'].unique()[0]
        merged_df['Lot Number'] = capture_df['lotNumber'].unique()[0]
        merged_df['Product ID'] = capture_df['productId'].unique()[0]
        merged_df['Production Record Status'] = capture_df['status'].unique()[0]

        # Merge structure data with fallbacks
        try:
            merged_df = merged_df.merge(unit_df, on=['masterTemplateId', 'unitProcedureId'], how='left')
            merged_df = merged_df.merge(op_df, on=['masterTemplateId', 'unitProcedureId', 'operationId'], how='left')
            merged_df = merged_df.merge(
                phase_df, on=['masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId'], how='left'
            )
        except ValueError as e:
            logging.warning(f"Structure merge error: {e}")
            for col in ['Unit', 'Operation', 'Phase']:
                merged_df[col] = ""

        # Final column selection and rename
        final_df = merged_df[[
            'Master Template Name', 'Lot Number', 'Product ID', 'Unit', 'Operation', 'Phase',
            'dateTime', 'Production Record Status', 'orderLabel', 'title', 'value',
            'userName', 'actionTaken', 'dataCaptureName'
        ]]
        final_df.columns = [
            'Master Template Name', 'Lot Number', 'Product ID', 'Unit', 'Operation', 'Phase',
            'Data Capture Time', 'Production Record Status', 'Structure Label', 'Description',
            'Input Data Value', 'Performed By', 'Action Performed', 'Captured Data Type'
        ]

        # Clean and format
        final_df = final_df.apply(lambda x: x.str.strip() if x.dtype == 'object' else x)
        final_df['Data Capture Time'] = final_df['Data Capture Time'].apply(reformat_datetime)
        final_df = final_df[~final_df['Performed By'].str.startswith('VOD_')]

        return final_df


def append_next_six_hour_window(csv_file=SIX_HOUR_WINDOW_CSV):
    """Append a new 6-hour window to the CSV if enough time has passed."""
    now = datetime.datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    max_end_time = int(now.timestamp()) - SIX_HOURS_IN_SECONDS

    if os.path.exists(csv_file):
        df = pd.read_csv(csv_file)
        last_end_epoch = int(df["end_epoch"].iloc[-1])
        start_epoch = max(last_end_epoch, FIRST_RUN_EPOCH)
    else:
        start_epoch = FIRST_RUN_EPOCH
        df = pd.DataFrame(columns=["start_datetime", "end_datetime", "start_epoch", "end_epoch"])
        df.to_csv(csv_file, index=False)

    end_epoch = start_epoch + SIX_HOURS_IN_SECONDS
    if end_epoch > max_end_time:
        return None  # Not ready yet

    start_dt = datetime.datetime.fromtimestamp(start_epoch, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    end_dt = datetime.datetime.fromtimestamp(end_epoch, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    return {
        "start_datetime": start_dt.replace(":", "_"),
        "end_datetime": end_dt.replace(":", "_"),
        "start_epoch": start_epoch,
        "end_epoch": end_epoch
    }


def streamload_and_process_records():
    """Fetch production records and return metadata and IDs."""
    try:
        window = append_next_six_hour_window()
        if window is None:
            logging.info("Next 6-hour window is not ready (needs to lag by 6 hours).")
            return {}, []

        logging.info(f"Fetching production records for window {window['start_datetime']} to {window['end_datetime']}")
        ids = DataProcessor().fetch_production_record_ids(
            window['start_epoch'], window['end_epoch'],
            window['start_datetime'], window['end_datetime']
        )
        return window, ids

    except Exception as e:
        logging.error(f"Error in streamload_and_process_records: {e}", exc_info=True)
        return {}, []


def main():
    """Main entry point."""
    setup_logging()

    window, record_ids = streamload_and_process_records()
    if not record_ids:
        if window:
            df = pd.read_csv(SIX_HOUR_WINDOW_CSV)
            df = pd.concat([df, pd.DataFrame([window])], ignore_index=True)
            df.to_csv(SIX_HOUR_WINDOW_CSV, index=False)
        logging.info("No new production records found.")
        return

    os.makedirs(INCR_OUTPUT_DATA_DIR, exist_ok=True)
    checkpoint = CheckpointManager.load_checkpoint()
    pipeline = ProductionRecordPipeline()
    pipeline.run(record_ids, int(BATCH_SIZE))

    df = pd.read_csv(SIX_HOUR_WINDOW_CSV)
    df = pd.concat([df, pd.DataFrame([window])], ignore_index=True)
    df.to_csv(SIX_HOUR_WINDOW_CSV, index=False)

main()
