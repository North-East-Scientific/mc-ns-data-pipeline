# utils/reporting.py
"""
Reporting utilities for data update tracking
"""
import os
import logging
from datetime import datetime
from database.operations import DatabaseOperations

class ReportGenerator:
    @staticmethod
    def generate_data_update_report(conn):
        """Generate a report of recent data updates"""
        db_ops = DatabaseOperations()
        
        with conn.cursor() as cursor:
            # Get recent file processing history
            history_records = db_ops.get_recent_processing_history(cursor)
            
            # Get recent lot updates  
            lot_updates = db_ops.get_recent_lot_updates(cursor)
        
        # Create report directory
        report_dir = 'reports/'
        os.makedirs(report_dir, exist_ok=True)
        
        # Generate report
        report_time = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(report_dir, f"data_update_report_{report_time}.csv")
        
        # Write history to report
        with open(report_file, 'w') as f:
            f.write("RECENT FILE PROCESSING HISTORY\n")
            f.write("filename,lot_number,process_type,processed_at,record_count,source_directory,target_directory,status,message\n")
            for record in history_records:
                f.write(','.join([str(field).replace(',', ';') for field in record]) + '\n')
            
            f.write("\nRECENT LOT UPDATES\n")
            f.write("lot_number,product_name,status,first_loaded,last_updated\n")
            for lot in lot_updates:
                f.write(','.join([str(field).replace(',', ';') for field in lot]) + '\n')
        
        logging.info(f"Generated data update report: {report_file}")
        return report_file
