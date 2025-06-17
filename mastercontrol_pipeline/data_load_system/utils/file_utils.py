# utils/file_utils.py
"""
File utility functions
"""
import os
import shutil
import logging
from datetime import datetime
from config.settings import Config

class FileUtils:
    @staticmethod
    def create_directories():
        """Create directory structure if it doesn't exist"""
        for dir_name in Config.DIR_STRUCTURE.values():
            os.makedirs(dir_name, exist_ok=True)
    
    @staticmethod
    def archive_file(file_path, process_type='initial_load'):
        """Archive the processed file with date-based directory structure"""
        filename = os.path.basename(file_path)
        today = datetime.now().strftime('%Y-%m-%d')
        
        # Create archive directory structure
        archive_path = os.path.join(Config.DIR_STRUCTURE['archive'], process_type, today)
        os.makedirs(archive_path, exist_ok=True)
        
        # Move file to processed directory
        processed_path = os.path.join(Config.DIR_STRUCTURE['processed_data'], filename)
        shutil.copy2(file_path, processed_path)
        
        # Copy to archive with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_filename = f"{os.path.splitext(filename)[0]}_{timestamp}{os.path.splitext(filename)[1]}"
        archive_full_path = os.path.join(archive_path, archive_filename)
        shutil.copy2(file_path, archive_full_path)

        # Delete original file after both copies are successful for incremental
        if process_type == 'incremental_load':
            os.remove(file_path)
            logging.info(f"Archived {filename} to {archive_full_path} and deleted original file")
        else:
            logging.info(f"Archived {filename} to {archive_full_path}")

        return processed_path
