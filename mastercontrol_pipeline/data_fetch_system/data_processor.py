# data_processor.py
"""Data processing and transformation functions."""

import os
import pandas as pd
import numpy as np
import logging
import json
from pandas import json_normalize
from api_client import MasterControlAPIClient
from utils import ensure_required_columns
from config import REQUIRED_COLUMNS, API_ENDPOINTS, RECORD_IDS_FILE_PATH


class DataProcessor:
    """Handles data processing and transformation operations."""
    
    def __init__(self):
        self.api_client = MasterControlAPIClient()

    def fetch_production_record_ids(self, start_epoch, end_epoch, start_dt_str, end_dt_str):
        """
        Fetch production record ID of the lots that have been modified from the API.
        Returns a list containing unique Production Record IDs.
        """
        params = {'start': start_epoch, 'end':end_epoch}
        all_data = self.api_client.fetch_paginated_data(
            API_ENDPOINTS['data_captures'], params
        )
        
        if not all_data:
            logging.info(f"Failed to get data for time {start_epoch} - {end_epoch} i.e {start_dt_str} - {end_dt_str}")
            return []

        record_ids = list({item["productionRecordId"] for item in all_data})
        logging.info(f"Found {len(record_ids)} unique productionRecordIds.")

        try:
            os.makedirs(RECORD_IDS_FILE_PATH, exist_ok=True)
            output_filename = f"{RECORD_IDS_FILE_PATH}{start_dt_str}_{end_dt_str}.csv"
            pd.DataFrame(record_ids, columns=["productionRecordId"]).to_csv(output_filename, index=False)
            logging.info(f"Saved record IDs to {output_filename}")
        except Exception as e:
            logging.error(f"Failed to save CSV file: {e}", exc_info=True)
        
        return record_ids
    
    def fetch_production_record_data(self, production_record_id):
        """
        Fetch production record data captures from the API.
        Returns the lot number and a filtered DataFrame of data capture records.
        """
        params = {'productionRecordId': production_record_id}
        all_data = self.api_client.fetch_paginated_data(
            API_ENDPOINTS['data_captures'], params
        )
        
        if not all_data:
            logging.error(f"Failed to get data for production record ID: {production_record_id}")
            return 0, pd.DataFrame()
        
        record_data_df = json_normalize(all_data)
        
        if record_data_df.shape[0] == 0:
            return 0, pd.DataFrame()
        
        record_data_df = ensure_required_columns(record_data_df, REQUIRED_COLUMNS['data_capture'])
        record_data_df = record_data_df[record_data_df["current"] == True]
        
        # Handle iteration numbers
        if 'iterationNumber' not in record_data_df.columns:
            record_data_df['iterationNumber'] = -99999
        else:
            record_data_df['iterationNumber'] = record_data_df['iterationNumber'].fillna(-99999).astype(int)
        
        record_data_df['orderLabel'] = np.where(
            (record_data_df['orderLabel'] != '0') & (record_data_df['iterationNumber'] != -99999),
            record_data_df['orderLabel'] + ' - ' + record_data_df['iterationNumber'].astype(str),
            record_data_df['orderLabel']
        )
        
        df_required = record_data_df[REQUIRED_COLUMNS['data_capture']]
        
        # Extract lot numbers
        lot_numbers = df_required[
            df_required['dataCaptureName'] == 'BATCH_RECORD_CREATION'
        ]["title"].tolist()
        
        if len(lot_numbers) > 1:
            logging.info(f"Multiple lot numbers found for production record ID {production_record_id}: {lot_numbers}")
        
        if not lot_numbers:
            return 0, df_required
        
        return lot_numbers[0], df_required
    
    def fetch_batch_records_by_lot(self, lot_number):
        """Fetch batch records using lot number."""
        params = {
            'sortColumn': 'create_date',
            'sortDirection': 'desc',
            'productName': '',
            'productId': '',
            'lotNumber': lot_number
        }
        
        all_data = self.api_client.fetch_paginated_data(
            API_ENDPOINTS['batch_records'], params
        )
        
        if not all_data:
            logging.error(f"Failed to get batch records for lot number: {lot_number}")
            return pd.DataFrame()
        
        df = json_normalize(all_data)
        df = ensure_required_columns(df, REQUIRED_COLUMNS['batch_record'])
        
        filtered_df = df[REQUIRED_COLUMNS['batch_record']]
        
        if filtered_df.shape[0] > 0 and filtered_df['status'].nunique() > 1:
            logging.info(f"Multiple statuses found for lot number {lot_number}: {filtered_df['status'].unique().tolist()}")
        
        return filtered_df
    
    def fetch_data_capture_by_lot(self, lot_number):
        """Fetch production record metadata using lot number."""
        params = {'lotNumbers': lot_number}
        
        all_data = self.api_client.fetch_paginated_data(
            API_ENDPOINTS['data_captures_by_lot'], params
        )
        
        if not all_data:
            logging.error(f"Failed to get data captures for lot number: {lot_number}")
            return pd.DataFrame()
        
        data_capture_df = json_normalize(all_data)
        
        required_cols = ["masterTemplateName", "productId", "lotNumber", "productionRecordStatus"]
        data_capture_df = ensure_required_columns(data_capture_df, required_cols)
        
        data_capture_df = data_capture_df.rename(columns={
            "masterTemplateName": "productName",
            "productionRecordStatus": "status"
        })
        
        df_required = data_capture_df[REQUIRED_COLUMNS['batch_record']]
        
        if df_required.shape[0] > 0 and df_required['status'].nunique() > 1:
            logging.info(f"Multiple statuses found for lot number {lot_number}: {df_required['status'].unique().tolist()}")
        
        return df_required
    
    def fetch_production_structure_metadata(self, production_record_id):
        """
        Fetch unit procedure, operation, and phase structure metadata.
        Returns three DataFrames for unit, operation, and phase structures.
        """
        url = f"{API_ENDPOINTS['structures']}/{production_record_id}/structures"
        
        response = self.api_client.perform_get_request(url)
        if response is None:
            logging.error(f"Failed to get structure metadata for production record ID: {production_record_id}")
            empty_df = pd.DataFrame()
            return empty_df, empty_df, empty_df
        
        data = json.loads(response.text)
        df = json_normalize(data)
        
        structure_metadata_df = df[df['level'].isin(['UNIT_PROCEDURE', 'OPERATION', 'PHASE'])]
        structure_metadata_df = ensure_required_columns(structure_metadata_df, REQUIRED_COLUMNS['structure'])
        
        df_required = structure_metadata_df[REQUIRED_COLUMNS['structure']]
        
        # Split into different structure levels
        unit_procedure_df = self._process_unit_procedures(
            df_required[df_required['level'] == "UNIT_PROCEDURE"]
        )
        operation_procedure_df = self._process_operations(
            df_required[df_required['level'] == "OPERATION"]
        )
        phase_procedure_df = self._process_phases(
            df_required[df_required['level'] == "PHASE"]
        )
        
        return unit_procedure_df, operation_procedure_df, phase_procedure_df
    
    def _process_unit_procedures(self, df):
        """Process unit procedure data."""
        if df.empty:
            return pd.DataFrame()
        
        df = df.drop(['level', 'operationId', 'phaseId'], axis=1)
        df.columns = ['Unit', 'masterTemplateId', 'unitProcedureId']
        return df[['masterTemplateId', 'unitProcedureId', 'Unit']]
    
    def _process_operations(self, df):
        """Process operation data."""
        if df.empty:
            return pd.DataFrame()
        
        df = df.drop(['level', 'phaseId'], axis=1)
        df.columns = ['Operation', 'masterTemplateId', 'unitProcedureId', 'operationId']
        return df[['masterTemplateId', 'unitProcedureId', 'operationId', 'Operation']]
    
    def _process_phases(self, df):
        """Process phase data."""
        if df.empty:
            return pd.DataFrame()
        
        df = df.drop(['level'], axis=1)
        df.columns = ['Phase', 'masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId']
        return df[['masterTemplateId', 'unitProcedureId', 'operationId', 'phaseId', 'Phase']]
