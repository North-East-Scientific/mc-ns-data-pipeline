# config.py
"""Configuration settings and environment variables."""

import os
from dotenv import load_dotenv

load_dotenv()

# API Configuration
API_TOKEN = os.getenv("API_TOKEN")
API_COOKIE = os.getenv("API_COOKIE")

# File paths
OUTPUT_DATA_DIR = os.getenv("OUTPUT_DATA_DIR", "../data/raw_bulk_data/")
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE_PATH", "../checkpoint.json")
STATUS_LOG_FILE = os.getenv("PROCESS_STATUS_LOG_PATH", "../pipeline_status_log.csv")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "../bulk_extraction_log.log")
RECORD_IDS_FILE_PATH = os.getenv("RECORD_IDS_FILE_PATH", "../data/record_ids/")

INCR_OUTPUT_DATA_DIR = os.getenv("INCR_OUTPUT_DATA_DIR", "../data/new_data/")
INCR_LOG_FILE_PATH = os.getenv("INCR_LOG_FILE_PATH", "../incr_extraction_log.log")


# Default constants
DEFAULT_END_ID = int(os.getenv("DEFAULT_END_ID", 60000))
BATCH_SIZE = os.getenv("DEFAULT_BATCH_SIZE", 100)

# API URLs
BASE_URL = os.getenv("BASE_URL")
API_ENDPOINTS = {
    'data_captures': f"{BASE_URL}/manufacturing/execution/production-record-data-captures",
    'batch_records': f"{BASE_URL}/manufacturing/execution/batch-records/production-records-list",
    'data_captures_by_lot': f"{BASE_URL}/manufacturing/execution/data-captures",
    'structures': f"{BASE_URL}/manufacturing/execution/production-records"
}

# Column definitions
REQUIRED_COLUMNS = {
    'data_capture': [
        "orderLabel", "productionRecordId", "masterTemplateId", "unitProcedureId",
        "operationId", "phaseId", "title", "value", "userName", "dateTime",
        "actionTaken", "dataCaptureName"
    ],
    'batch_record': [
        "lotNumber", "productId", "productName", "status"
    ],
    'structure': [
        'title', 'level', 'masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId'
    ]
}
