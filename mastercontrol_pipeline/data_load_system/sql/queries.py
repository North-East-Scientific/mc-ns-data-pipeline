# sql/queries.py
"""
SQL queries for bulk load and incremental operations
"""

# Lot operations
CHECK_LOT_EXISTS = """
SELECT lot_number, first_loaded FROM lots WHERE lot_number = %s
"""

INSERT_LOT = """
INSERT INTO lots (lot_number, product_id, product_name, status, first_loaded, last_updated)
VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
"""

UPDATE_LOT = """
UPDATE lots 
SET product_id = %s, product_name = %s, status = %s, last_updated = CURRENT_TIMESTAMP
WHERE lot_number = %s
"""

# Lot data operations
DELETE_LOT_DATA = """
DELETE FROM lot_data WHERE lot_number = %s
"""

INSERT_LOT_DATA = """
INSERT INTO lot_data (
    lot_number, master_template_name, unit, operation, phase,
    data_capture_time, structure_label, description, input_data_value,
    performed_by, action_performed, captured_data_type, data_hash
) VALUES %s
"""

# Processing history
INSERT_PROCESSING_HISTORY = """
INSERT INTO file_processing_history 
(filename, lot_number, process_type, record_count, source_directory, target_directory, status, message)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""

# Reporting queries
GET_RECENT_PROCESSING_HISTORY = """
SELECT 
    filename, lot_number, process_type, processed_at, record_count, 
    source_directory, target_directory, status, message
FROM file_processing_history
WHERE processed_at > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY processed_at DESC
"""

GET_RECENT_LOT_UPDATES = """
SELECT lot_number, product_name, status, first_loaded, last_updated
FROM lots
WHERE last_updated > CURRENT_TIMESTAMP - INTERVAL '7 days'
ORDER BY last_updated DESC
"""
