# sql/schema.py
"""
Database schema for MasterControl Postgres load system
"""

CREATE_LOTS_TABLE = """
CREATE TABLE IF NOT EXISTS lots (
    lot_number VARCHAR(100) PRIMARY KEY,
    product_id VARCHAR(100),
    product_name TEXT,
    status VARCHAR(50),
    first_loaded TIMESTAMP,
    last_updated TIMESTAMP
);
"""

CREATE_LOT_DATA_TABLE = """
CREATE TABLE IF NOT EXISTS lot_data (
    id SERIAL PRIMARY KEY,
    lot_number VARCHAR(100) REFERENCES lots(lot_number),
    master_template_name TEXT,
    unit TEXT,
    operation TEXT,
    phase TEXT,
    data_capture_time TIMESTAMP,
    structure_label TEXT,
    description TEXT,
    input_data_value TEXT,
    performed_by TEXT,
    action_performed TEXT,
    captured_data_type TEXT,
    data_hash VARCHAR(64),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_PROCESSING_HISTORY_TABLE = """
CREATE TABLE IF NOT EXISTS file_processing_history (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255),
    lot_number VARCHAR(100),
    process_type VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_count INTEGER,
    source_directory VARCHAR(255),
    target_directory VARCHAR(255),
    status VARCHAR(50),
    message TEXT
);
"""

CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_lot_data_lot_number ON lot_data(lot_number);",
    "CREATE INDEX IF NOT EXISTS idx_lot_data_data_hash ON lot_data(data_hash);"
]
