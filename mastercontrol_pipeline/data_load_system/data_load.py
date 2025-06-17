# data_load.py
"""
Main bulk load script - with incremental support
"""
import argparse
from processors.bulk_loader import BulkLoader
from utils.reporting import ReportGenerator
from database.connection import DatabaseManager

def main():
    parser = argparse.ArgumentParser(description='PostgreSQL ETL System for MasterControl API Data')
    parser.add_argument('--bulk', action='store_true', help='Perform initial bulk load of data')
    parser.add_argument('--incremental', action='store_true', help='Process new data files once')
    parser.add_argument('--report', action='store_true', help='Generate a data update report')
    
    args = parser.parse_args()
    
    bulk_loader = BulkLoader()
    
    if args.bulk:
        bulk_loader.bulk_load_initial_data()
    
    if args.incremental:
        bulk_loader.process_new_data()
    
    if args.report:
        db_manager = DatabaseManager()
        conn = db_manager.get_connection()
        report_generator = ReportGenerator()
        report_generator.generate_data_update_report(conn)
        conn.close()
    
    # If no arguments provided, show help
    if not (args.bulk or args.incremental or args.report):
        parser.print_help()

main()
