# status_tracker.py
"""Status tracking for processed records."""

import pandas as pd
import logging
import os
from config import STATUS_LOG_FILE


class StatusTracker:
    """Manages processing status tracking."""
    
    def __init__(self):
        self.tracking_df = self.load_tracking_df()
    
    def load_tracking_df(self):
        """
        Load the CSV file tracking the processing status of each production record.
        Returns a DataFrame initialized with required columns if the file doesn't exist.
        """
        if os.path.exists(STATUS_LOG_FILE):
            try:
                return pd.read_csv(STATUS_LOG_FILE)
            except Exception as e:
                logging.error(f"Error loading tracking file: {e}")
                return self._create_empty_tracking_df()
        else:
            return self._create_empty_tracking_df()
    
    def _create_empty_tracking_df(self):
        """Create empty tracking DataFrame with required columns."""
        return pd.DataFrame(columns=["Production Record ID", "Lot Number", "Status", "Reason"])
    
    def is_already_processed(self, production_record_id):
        """Check if a production record has already been successfully processed."""
        existing_entry = self.tracking_df[
            self.tracking_df["Production Record ID"] == production_record_id
        ]
        if not existing_entry.empty:
            return existing_entry.iloc[0]["Status"] == "Success"
        return False
    
    def log_status(self, production_record_id, lot_number, status, reason=""):
        """Log the processing status for a production record."""
        self.tracking_df.loc[len(self.tracking_df)] = [
            production_record_id, lot_number, status, reason
        ]
        self.save_tracking_df()
    
    def save_tracking_df(self):
        """Save the tracking DataFrame to CSV."""
        try:
            self.tracking_df.to_csv(STATUS_LOG_FILE, index=False)
        except Exception as e:
            logging.error(f"Error saving tracking file: {e}")
