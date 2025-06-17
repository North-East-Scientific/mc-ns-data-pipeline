# config/settings.py
"""
Configuration settings for bulk load system
"""
import os
import json
from dotenv import load_dotenv

load_dotenv()

class Config:
    # API credentials
    API_TOKEN = os.getenv("API_TOKEN")
    API_COOKIE = os.getenv("API_COOKIE")
    
    # Database configuration
    DB_CONFIG = json.loads(os.getenv("DB_CONFIG"))
    
    # Directory structure
    DIR_STRUCTURE = json.loads(os.getenv("DIR_STRUCTURE"))
    
    # File paths
    OUTPUT_DATA_DIR = os.getenv("OUTPUT_DATA_DIR", "../data/raw_bulk_data/")
    CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE_PATH", "../checkpoint.json")
    STATUS_LOG_FILE = os.getenv("PROCESS_STATUS_LOG_PATH", "../pipeline_status_log.csv")
    LOG_FILE_PATH = os.getenv("LOAD_LOG_FILE_PATH", "../load_log.log")

    # Processing settings
    BATCH_SIZE = int(os.getenv("DEFAULT_BATCH_SIZE", 100))
    
    # Logging configuration
    LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
    LOG_LEVEL = 'INFO'
