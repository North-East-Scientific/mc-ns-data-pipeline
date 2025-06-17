# checkpoint_manager.py
"""Checkpoint management for resuming processing."""

import json
import datetime
import logging
import os
from config import CHECKPOINT_FILE


class CheckpointManager:
    """Manages checkpoint data for resuming processing."""
    
    @staticmethod
    def load_checkpoint():
        """
        Load the last saved checkpoint from file.
        Returns a dictionary containing the last processed production record ID and timestamp.
        """
        if os.path.exists(CHECKPOINT_FILE):
            try:
                with open(CHECKPOINT_FILE, 'r') as f:
                    checkpoint = json.load(f)
                    logging.info(f"Loaded checkpoint: {checkpoint}")
                    return checkpoint
            except Exception as e:
                logging.error(f"Error loading checkpoint: {e}")
                return CheckpointManager._default_checkpoint()
        else:
            return CheckpointManager._default_checkpoint()
    
    @staticmethod
    def save_checkpoint(last_id):
        """
        Save the current checkpoint to file.
        Stores the last processed production record ID and current timestamp.
        """
        checkpoint = {
            'last_processed_id': last_id,
            'timestamp': datetime.datetime.now().isoformat()
        }
        try:
            with open(CHECKPOINT_FILE, 'w') as f:
                json.dump(checkpoint, f)
            logging.info(f"Checkpoint saved: {checkpoint}")
        except Exception as e:
            logging.error(f"Error saving checkpoint: {e}")
    
    @staticmethod
    def _default_checkpoint():
        """Return default checkpoint when file doesn't exist."""
        return {
            'last_processed_id': -1, 
            'timestamp': datetime.datetime.now().isoformat()
        }
