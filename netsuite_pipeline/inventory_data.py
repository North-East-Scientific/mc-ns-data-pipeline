import os
import jaydebeapi
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

# Load environment variables
load_dotenv()

# JDBC / NetSuite credentials
ACCOUNT_ID = os.getenv("NS_ACCOUNT_ID")
PORT = os.getenv("NS_ACCOUNT_PORT")
ROLE_ID = os.getenv("NS_ROLE_ID")
NS_USERNAME = os.getenv("NS_USERNAME")
NS_PASSWORD = os.getenv("NS_PASSWORD")

# JARs (relative paths)
JAR_PATH1 = os.getenv("JAR_PATH1")
JAR_PATH2 = os.getenv("JAR_PATH2")

# PostgreSQL credentials
PG_DB = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")

# JDBC connection to NetSuite
netsuite_conn = jaydebeapi.connect(
    "com.netsuite.jdbc.openaccess.OpenAccessDriver",
    f"jdbc:ns://{ACCOUNT_ID}.connect.api.netsuite.com:{PORT};"
    f"ServerDataSource=NetSuite2.com;Encrypted=1;NegotiateSSLClose=false;"
    f"CustomProperties=(AccountID={ACCOUNT_ID};RoleID={ROLE_ID});",
    [NS_USERNAME, NS_PASSWORD],
    [JAR_PATH1, JAR_PATH2]
)

# PostgreSQL connection
pg_conn = psycopg2.connect(
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASSWORD,
    host=PG_HOST,
    port=PG_PORT
)
pg_cursor = pg_conn.cursor()

# Query NetSuite for inventory data
netsuite_query = """
SELECT
    l.fullname AS location,
    i.id AS item_id,
    i.displayname AS item_name,
    iil.quantityavailable, 
    iil.quantityonhand, 
    iil.quantitybackordered
FROM 
    inventoryitemlocations iil
JOIN 
    location l ON iil.location = l.id
JOIN 
    item i ON iil.item = i.id
WHERE 
    l.fullname = '2100 Warehouse Inventory';
"""

netsuite_cursor = netsuite_conn.cursor()
netsuite_cursor.execute(netsuite_query)
rows = netsuite_cursor.fetchall()

# Create or clear inventory table
pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS netsuite_inventory_data (
        location VARCHAR(255),
        item_id VARCHAR(255),
        item_name VARCHAR(255),
        quantity_available DECIMAL(10,2),
        quantity_on_hand DECIMAL(10,2),
        quantity_backordered DECIMAL(10,2),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
pg_cursor.execute("DELETE FROM netsuite_inventory_data;")

# Insert inventory data
pg_cursor.executemany("""
    INSERT INTO netsuite_inventory_data (
        location, item_id, item_name, quantity_available,
        quantity_on_hand, quantity_backordered
    ) VALUES (%s, %s, %s, %s, %s, %s)
""", rows)

# Create or clear dashboard inventory table
pg_cursor.execute("""
    CREATE TABLE IF NOT EXISTS netsuite_dashboard_inventory_data (
        location VARCHAR(255),
        item_id VARCHAR(255),
        item_name VARCHAR(255),
        quantity_available DECIMAL(10,2),
        quantity_on_hand DECIMAL(10,2),
        quantity_backordered DECIMAL(10,2),
        last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        dashboard_product_name VARCHAR(255)
    );
""")
pg_cursor.execute("DELETE FROM netsuite_dashboard_inventory_data;")

# Populate enriched dashboard table
pg_cursor.execute("""
    INSERT INTO netsuite_dashboard_inventory_data (
        location, item_id, item_name, quantity_available,
        quantity_on_hand, quantity_backordered, last_updated,
        dashboard_product_name
    )
    SELECT 
        a.location, a.item_id, a.item_name, a.quantity_available,
        a.quantity_on_hand, a.quantity_backordered, a.last_updated,
        b."Name" AS dashboard_product_name
    FROM 
        netsuite_inventory_data a
    INNER JOIN 
        dashboard_products b
    ON 
        a.item_id = b."Internal ID";
""")

# Finalize and close connections
pg_conn.commit()
pg_cursor.close()
pg_conn.close()
netsuite_cursor.close()
netsuite_conn.close()

print("Inventory data extraction and loading completed successfully.")
