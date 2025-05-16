"""
Author: Ashish Agarwal
Date: 2025-04-08 10:20:19
Company: North East Scientific
Position: Senior Data Engineer
Contact: agarwal.ashish.singhal@gmail.com

Description: 
This script extracts production record data from the MasterControl API and saves it to CSV files.
It includes checkpointing functionality to resume processing after interruptions.

Usage:
Run the script with optional arguments:
python script_name.py [--start ID] [--end ID] [--batch_size SIZE]

Notes:
- The script saves checkpoint data every batch_size records
- Logs are saved to 'extraction_log.log'
- CSV output is saved to the 'test2/' directory
- Progress tracking is saved to 'pipeline_status_log.csv'
"""

import numpy as np
import pandas as pd
from pandas import json_normalize
import json
import requests
import re
import time
import datetime
import os
import logging
import argparse
from dateutil.parser import parse
from requests.exceptions import HTTPError, ConnectionError, Timeout
from dotenv import load_dotenv

load_dotenv()
API_TOKEN = os.getenv("API_TOKEN")
API_COOKIE = os.getenv("API_COOKIE")

# Config paths
OUTPUT_DATA_DIR = os.getenv("OUTPUT_DATA_DIR", "test2/")
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE_PATH", "checkpoint.json")
STATUS_LOG_FILE = os.getenv("PROCESS_STATUS_LOG_PATH", "pipeline_status_log.csv")
LOG_FILE_PATH = os.getenv("LOG_FILE_PATH", "extraction_log.log")

# Other default constants
DEFAULT_END_ID = int(os.getenv("DEFAULT_END_ID", 60000))

# Set up command line argument parsing
parser = argparse.ArgumentParser(description='Process production records with checkpoint capability')
parser.add_argument('--start', type=int, help='Starting production record ID')
parser.add_argument('--end', type=int, help='Ending production record ID')
parser.add_argument('--batch_size', type=int, default=100, help='Checkpoint batch size')
args = parser.parse_args()

# Configure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Default end value if not specified
productionRecordId_end = args.end if args.end is not None else DEFAULT_END_ID

# Ensure the output directory exists
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)

# Load or initialize the checkpoint
def load_checkpoint():
    """
    Load the last saved checkpoint from file.
    Returns a dictionary containing the last processed production record ID and timestamp.
    If the checkpoint file is not found or corrupted, returns a default initial checkpoint.
    """
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                checkpoint = json.load(f)
                logging.info(f"Loaded checkpoint: {checkpoint}")
                return checkpoint
        except Exception as e:
            logging.error(f"Error loading checkpoint: {e}")
            return {'last_processed_id': -1, 'timestamp': datetime.datetime.now().isoformat()}
    else:
        return {'last_processed_id': -1, 'timestamp': datetime.datetime.now().isoformat()}

# Save the current checkpoint
def save_checkpoint(last_id):
    """
    Save the current checkpoint to file.
    Stores the last processed production record ID and current timestamp for resuming later.
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

# Load or initialize tracking DataFrame
def load_tracking_df():
    """
    Load the CSV file tracking the processing status of each production record.
    Returns a DataFrame initialized with required columns if the file does not exist or is invalid.
    """
    if os.path.exists(STATUS_LOG_FILE):
        try:
            return pd.read_csv(STATUS_LOG_FILE)
        except Exception as e:
            logging.error(f"Error loading tracking file: {e}")
            return pd.DataFrame(columns=["Production Record ID", "Lot Number", "Status", "Reason"])
    else:
        return pd.DataFrame(columns=["Production Record ID", "Lot Number", "Status", "Reason"])

# Initialize the tracking DataFrame
tracking_df = load_tracking_df()

def fetch_production_record_data(productionRecordId):
    """
    Fetch production record data captures from the API for a given productionRecordId.
    Returns the lot number and a filtered DataFrame of data capture records.
    """
    payload = {}
    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Cookie': API_COOKIE
    }
    
    all_data = []  # List to accumulate data from all pages
    page = 0
    last_page = False

    while not last_page:
        url = f"https://mx.us-west-2.svc.mastercontrol.com/nes/v1/manufacturing/execution/production-record-data-captures?currentPage={page}&itemsPerPage=1000&productionRecordId={productionRecordId}" 
        
        response = perform_get_request(url, headers, payload)
        if response is None:
            logging.error(f"Failed to get response from API 2 for production record ID: {productionRecordId}")
            return 0, pd.DataFrame()
            
        data = json.loads(response.text)
        
        # Append the current page data to all_data
        all_data.extend(data.get('content', []))
        
        # Check if this is the last page
        last_page = data.get('last', True)
        
        # Increment page number for next iteration
        page += 1
    
    # Convert the accumulated data into a DataFrame
    record_data_df = json_normalize(all_data)

    CURRENT_COLUMNS2 = record_data_df.columns.tolist()
    REQUIRED_COLUMNS2= ["orderLabel",
                        "productionRecordId",
                        "masterTemplateId",
                        "unitProcedureId",
                        "operationId",
                        "phaseId",
                        "title",
                        "value",
                        "userName",
                        "dateTime",
                        "actionTaken",
                        "dataCaptureName"]

    if record_data_df.shape[0] == 0:
        return 0, pd.DataFrame()
    
    for col in REQUIRED_COLUMNS2:
        if col not in CURRENT_COLUMNS2:
            record_data_df[col] = ''
    
    
    record_data_df = record_data_df[record_data_df["current"] == True]

    # Check if the 'iterationNumber' column exists
    if 'iterationNumber' not in record_data_df.columns:
        record_data_df['iterationNumber'] = -99999  # Default value for missing 'iterationNumber'
    else:
        record_data_df['iterationNumber'] = record_data_df['iterationNumber'].fillna(-99999).astype(int)

    record_data_df['orderLabel'] = np.where(
        (record_data_df['orderLabel'] != '0') & (record_data_df['iterationNumber'] != -99999),
        record_data_df['orderLabel'] + ' - ' + record_data_df['iterationNumber'].astype(str),
        record_data_df['orderLabel']  
    )

    
    df2_required = record_data_df[REQUIRED_COLUMNS2]

    lotNumbers = df2_required[df2_required['dataCaptureName'] == 'BATCH_RECORD_CREATION']["title"].tolist()
    if len(lotNumbers) > 1:
        logging.info(f"Multiple lot numbers found for production record ID {productionRecordId}: {lotNumbers}")
    
    if not lotNumbers:
        return 0, df2_required
    
    return lotNumbers[0], df2_required


def fetch_production_structure_metadata(productionRecordId):
    """
    Fetch unit procedure, operation, and phase structure metadata for a given production record.
    Returns three DataFrames corresponding to unit, operation, and phase structures.
    """
    url = f"https://mx.us-west-2.svc.mastercontrol.com/nes/v1/manufacturing/execution/production-records/{productionRecordId}/structures"

    payload = {}
    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Cookie': API_COOKIE
    }
    response = perform_get_request(url, headers, payload)
    if response is None:
        logging.error(f"Failed to get response from API 3 for production record ID: {productionRecordId}")
        # Return empty DataFrames if API call failed
        empty_df = pd.DataFrame()
        return empty_df, empty_df, empty_df
        
    data = json.loads(response.text)

    # Flatten the JSON data
    df = json_normalize(data)

    structure_metadata_df = df[df['level'].isin(['UNIT_PROCEDURE', 'OPERATION', 'PHASE'])]
    REQUIRED_COLUMNS3 = ['title',
                         'level',
                        'masterTemplateId',
                         'unitProcedureId',
                         'operationId',
                         'phaseId'
                        ]

    for col in REQUIRED_COLUMNS3:
        if col not in structure_metadata_df.columns:
            structure_metadata_df[col] = ''
            
    df_required = structure_metadata_df[REQUIRED_COLUMNS3]

    unit_procedure_df = df_required[df_required['level'] == "UNIT_PROCEDURE"]
    operation_procedure_df = df_required[df_required['level'] == "OPERATION"]
    phase_procedure_df = df_required[df_required['level'] == "PHASE"]

    unit_procedure_df = unit_procedure_df.drop(['level', 'operationId', 'phaseId'], axis=1)
    unit_procedure_df.columns = ['Unit', 'masterTemplateId', 'unitProcedureId']
    unit_procedure_df = unit_procedure_df[['masterTemplateId', 'unitProcedureId', 'Unit']]
    
    
    operation_procedure_df = operation_procedure_df.drop(['level', 'phaseId'], axis=1)
    operation_procedure_df.columns = ['Operation', 'masterTemplateId', 'unitProcedureId', 'operationId']
    operation_procedure_df = operation_procedure_df[['masterTemplateId', 'unitProcedureId', 'operationId', 'Operation']]
    
    phase_procedure_df = phase_procedure_df.drop(['level'], axis=1)
    phase_procedure_df.columns = ['Phase', 'masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId']
    phase_procedure_df = phase_procedure_df[['masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId', 'Phase']]

    return unit_procedure_df, operation_procedure_df, phase_procedure_df


def perform_get_request(url, headers, payload, retries=3, delay=0.1):
    """
    Perform an HTTP GET request with retry logic.
    Returns the response object if successful, otherwise None.
    """
    for retry in range(retries):
        try:
            response = requests.get(url, headers=headers, data=payload, timeout=10)
            response.raise_for_status()
            return response
        except (HTTPError, ConnectionError, Timeout) as e:
            logging.warning(f"Request failed: {e}. for {url[-18:]} and retry : {retry}")
            time.sleep(delay)
    logging.error(f"Request failed after {retries} retries for URL: {url}")
    return None

def fetch_batch_records_by_lot(lotNumber):
    """
    Fetch batch records using lot number from the second variant of API 1.
    Returns a DataFrame containing filtered batch record fields.
    """
    payload = {}
    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Cookie': API_COOKIE
    }

    all_data = []  # List to accumulate data from all pages
    page = 0
    last_page = False

    while not last_page:
        url = f"https://mx.us-west-2.svc.mastercontrol.com/nes/v1/manufacturing/execution/batch-records/production-records-list?currentPage={page}&itemsPerPage=1000&sortColumn=create_date&sortDirection=desc&productName=&productId=&lotNumber={lotNumber}"
        
        response = perform_get_request(url, headers, payload)
        if response is None:
            logging.error(f"Failed to get response from API 1 (second) for lot number: {lotNumber}")
            return pd.DataFrame()
            
        data = json.loads(response.text)
        
        # Append the current page data to all_data
        all_data.extend(data.get('pageResult', {"content": []}).get("content", []))
        
        # Check if this is the last page
        last_page = data.get('last', True)
        
        # Increment page number for next iteration
        page += 1
    
    # Convert the accumulated data into a DataFrame
    df1 = json_normalize(all_data)
    
    REQUIRED_COLUMNS1= ["lotNumber",
                    "productId",
                    "productName",
                    "status",
                       ]
    
    for col in REQUIRED_COLUMNS1:
        if col not in df1.columns:
            df1[col] = ''
            
    filtered_data_capture_df = df1[REQUIRED_COLUMNS1]
    
    if filtered_data_capture_df.shape[0] > 0 and filtered_data_capture_df['status'].nunique() > 1:
        logging.info(f"Multiple statuses found for lot number {lotNumber}: {filtered_data_capture_df['status'].unique().tolist()}")

    return filtered_data_capture_df


def fetch_data_capture_by_lot(lotNumber):
    """
    Fetch production record metadata using lot number from the first variant of API 1.
    Returns a DataFrame containing filtered fields like product ID, lot number, and status.
    """
    payload = {}
    headers = {
        'Authorization': f'Bearer {API_TOKEN}',
        'Cookie': API_COOKIE
    }

    all_data = []  # List to accumulate data from all pages
    page = 0
    last_page = False

    while not last_page:
        url = f"https://mx.us-west-2.svc.mastercontrol.com/nes/v1/manufacturing/execution/data-captures?currentPage={page}&itemsPerPage=1000&lotNumbers={lotNumber}"
        
        response = perform_get_request(url, headers, payload)
        if response is None:
            logging.error(f"Failed to get response from API 1 (first) for lot number: {lotNumber}")
            return pd.DataFrame()
            
        data = json.loads(response.text)
        
        # Append the current page data to all_data
        all_data.extend(data.get('content', []))
        
        # Check if this is the last page
        last_page = data.get('last', True)
        
        # Increment page number for next iteration
        page += 1
    
    # Convert the accumulated data into a DataFrame
    data_capture_df = json_normalize(all_data)
    
    REQUIRED_COLUMNS1= ["masterTemplateName",
                    "productId",
                    "lotNumber",
                    "productionRecordStatus"
                       ]
    
    for col in REQUIRED_COLUMNS1:
        if col not in data_capture_df.columns:
            data_capture_df[col] = ''

    data_capture_df = data_capture_df.rename(columns={"masterTemplateName": "productName", "productionRecordStatus": "status"})

    REQUIRED_COLUMNS1_2 = ["lotNumber",
                    "productId",
                    "productName",
                    "status",
                       ]
    df1_required = data_capture_df[REQUIRED_COLUMNS1_2]
    
    if df1_required.shape[0] > 0 and df1_required['status'].nunique() > 1:
        logging.info(f"Multiple statuses found for lot number {lotNumber} (first API): {df1_required['status'].unique().tolist()}")

    return df1_required


def pipeline(productionRecordId):
    """
    Orchestrates the extraction, transformation, and saving of production record data.
    Performs API calls, merges data, writes CSV, and updates processing status.
    """
    global tracking_df

    try:
        # Check if we've already processed this ID
        existing_entry = tracking_df[tracking_df["Production Record ID"] == productionRecordId]
        if not existing_entry.empty:
            if existing_entry.iloc[0]["Status"] == "Success":
                logging.info(f"Skipping already successfully processed ID: {productionRecordId}")
                return 0
            else:
                logging.info(f"Retrying previously failed ID: {productionRecordId}")
        
        logging.info(f"Processing production record ID: {productionRecordId}")
        lotNumber, filtered_record_data_df = fetch_production_record_data(productionRecordId)

        if filtered_record_data_df.shape[0] == 0:
            logging.warning(f"Not getting any data for productionRecordId in fetch_production_record_data: {productionRecordId}")
            tracking_df.loc[len(tracking_df)] = [productionRecordId, None, "Fail", "fetch_production_record_data returned empty"]
            tracking_df.to_csv(STATUS_LOG_FILE, index=False)
            return 0

        filtered_data_capture_df = fetch_batch_records_by_lot(lotNumber)
        unit_procedure_df, operation_procedure_df, phase_procedure_df = fetch_production_structure_metadata(productionRecordId)

        merged_record_df = filtered_record_data_df

        if filtered_data_capture_df.shape[0] == 0:
            filtered_data_capture_df = fetch_data_capture_by_lot(lotNumber)
            if filtered_data_capture_df.shape[0] == 0:
                logging.warning(f"Not getting any data for lotNumber in both api_1 calls: {lotNumber}")
                tracking_df.loc[len(tracking_df)] = [productionRecordId, lotNumber, "Fail", "Both api_1 calls returned empty"]
                tracking_df.to_csv(STATUS_LOG_FILE, index=False)
                return 0

        merged_record_df.loc[:, 'Master Template Name'] = filtered_data_capture_df['productName'].unique()[0]
        merged_record_df.loc[:, 'Lot Number'] = filtered_data_capture_df['lotNumber'].unique()[0]
        merged_record_df.loc[:, 'Product ID'] = filtered_data_capture_df['productId'].unique()[0]
        merged_record_df.loc[:, 'Production Record Status'] = filtered_data_capture_df['status'].unique()[0]

        merged_df = merged_record_df
        try:
            merged_df = merged_record_df.merge(unit_procedure_df, how='left', on=['masterTemplateId', 'unitProcedureId']) 
            try:
                merged_df = merged_df.merge(operation_procedure_df, how='left', on=['masterTemplateId', 'unitProcedureId', 'operationId'])
                try:
                    merged_df = merged_df.merge(phase_procedure_df, how='left', on=['masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId'])
                except ValueError as e:
                    logging.warning(f"Merge error, failed at phase: {e}")
                    merged_df.loc[:, 'Phase'] = ""
            except ValueError as e:
                logging.warning(f"Merge error, failed at operation: {e}")
                merged_df.loc[:, 'Operation'] = ""
                merged_df.loc[:, 'Phase'] = ""
        except ValueError as e:
            logging.warning(f"Merge error, failed at unit: {e}")
            merged_df.loc[:, 'Unit'] = ""
            merged_df.loc[:, 'Operation'] = ""
            merged_df.loc[:, 'Phase'] = ""

        merged_df1 = merged_df[['Master Template Name', 'Lot Number', 'Product ID', 'Unit', 'Operation', 'Phase', 'dateTime', 'Production Record Status', 'orderLabel', 'title', 'value', 'userName', 'actionTaken', 'dataCaptureName']]
        merged_df1.columns = ['Master Template Name', 'Lot Number', 'Product ID', 'Unit', 'Operation',
               'Phase', 'Data Capture Time', 'Production Record Status', 'Structure Label', 'Description', 'Input Data Value',
               'Performed By', 'Action Performed', 'Captured Data Type']

        merged_df1 = merged_df1.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
        merged_df1['Data Capture Time'] = merged_df1['Data Capture Time'].apply(reformat_datetime)

        merged_df2 = merged_df1[~merged_df1['Performed By'].str.startswith('VOD_')]

        FILE_NAME = f"{OUTPUT_DATA_DIR}{lotNumber}.csv"
        merged_df2.to_csv(FILE_NAME, index=False)

        # Log success
        tracking_df.loc[len(tracking_df)] = [productionRecordId, lotNumber, "Success", ""]
        tracking_df.to_csv(STATUS_LOG_FILE, index=False)
        logging.info(f"Successfully processed production record ID {productionRecordId}, lot number {lotNumber}")

    except Exception as e:
        logging.error(f"Error processing productionRecordId {productionRecordId}: {e}", exc_info=True)
        tracking_df.loc[len(tracking_df)] = [productionRecordId, None, "Fail", str(e)]
        tracking_df.to_csv(STATUS_LOG_FILE, index=False)

    
def main():
    """
    Entry point for the script.
    Loads checkpoint, processes production records in batches, and logs progress.
    """
    # Load checkpoint
    checkpoint = load_checkpoint()
    start_id = args.start if args.start is not None else (checkpoint['last_processed_id'] + 1)
    
    if start_id <= 0:
        start_id = 0
        
    batch_size = args.batch_size
    
    logging.info(f"Starting processing from ID {start_id} to {productionRecordId_end}")
    
    # Process in batches to reduce checkpoint frequency
    current_batch_start = start_id
    
    while current_batch_start <= productionRecordId_end:
        current_batch_end = min(current_batch_start + batch_size - 1, productionRecordId_end)
        
        logging.info(f"Processing batch from {current_batch_start} to {current_batch_end}")
        
        for productionRecordId in range(current_batch_start, current_batch_end + 1):
            pipeline(productionRecordId)
            
        # Save checkpoint after each batch
        save_checkpoint(current_batch_end)
        current_batch_start = current_batch_end + 1
    
    logging.info("Processing complete")

# Function to reformat the datetime field
def reformat_datetime(date_str):
    """
    Reformat a datetime string to 'M/D/YYYY H:MM' format.
    Returns the reformatted string or the original string if parsing fails.
    """
    try:
        # Parse the date string using dateutil.parser.parse
        dt = parse(date_str)
        # Format the datetime object to the desired format without leading zeros
        return f"{dt.month}/{dt.day}/{dt.year} {dt.hour}:{dt.strftime('%M')}"
    except Exception as e:
        logging.error(f"Error reformatting datetime {date_str}: {e}")
        return date_str

if __name__ == "__main__":
    main()
