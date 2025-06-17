import os
from datetime import datetime
import jaydebeapi
import psycopg2
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# JDBC / NetSuite Credentials
ACCOUNT_ID = os.getenv("NS_ACCOUNT_ID")
ROLE_ID = os.getenv("NS_ROLE_ID")
NS_USERNAME = os.getenv("NS_USERNAME")
NS_PASSWORD = os.getenv("NS_PASSWORD")

# JAR paths (relative)
JAR_PATH1 = os.getenv("JAR_PATH1")
JAR_PATH2 = os.getenv("JAR_PATH2")

# PostgreSQL Credentials
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

# Query NetSuite
netsuite_query = """
SELECT
    t.id AS transaction_id,
    t.Employee AS RepID,
    t.Entity AS Entity,
    e.name AS RepName,
    cust.name AS CustomerName,
    i.displayname AS item_name,
    i.id AS item_id,
    TO_CHAR(t.trandate, 'YYYY-MM-DD') AS sales_date,
    t.tranid AS doc_id,
    -SUM(tl.quantity) AS total_quantity,
    -SUM(CASE WHEN t.abbrevtype = 'INV' THEN tl.netamount ELSE 0 END) AS ForeignTotal_INV,
    SUM(CASE WHEN t.abbrevtype = 'CREDMEM' THEN tl.netamount ELSE 0 END) AS ForeignTotal_CREDMEM,
    -SUM(CASE WHEN t.abbrevtype IN ('CREDMEM', 'INV') THEN tl.netamount ELSE 0 END) AS TotalAmount,
    -SUM(tl.costestimate) AS TotalCostEstimate,
    SUM(tl.estgrossprofit) AS EstGrossProfit
FROM
    Transaction t
JOIN entitysubsidiaryrelationship e ON t.Employee = e.entity
JOIN entitysubsidiaryrelationship cust ON t.Entity = cust.entity
JOIN transactionLine tl ON t.id = tl.transaction
JOIN item i ON tl.item = i.id
JOIN Customer c ON c.ID = t.Entity
JOIN EntityAddress ea ON ea.nKey = c.DefaultShippingAddress
WHERE
    t.abbrevtype IN ('CREDMEM', 'INV')
    AND t.status NOT IN ('Voided', 'Closed')
    AND c.IsInactive = 'F'
GROUP BY
    t.id, t.Employee, t.Entity, e.name, cust.name, i.displayname, i.id, TO_CHAR(t.trandate, 'YYYY-MM-DD'),
    t.tranid
ORDER BY
    sales_date, RepName, item_name;
"""

netsuite_cursor = netsuite_conn.cursor()
netsuite_cursor.execute(netsuite_query)
rows = netsuite_cursor.fetchall()

# Clear existing data
pg_cursor.execute("DELETE FROM netsuite_sales_data;")

# Insert data
pg_cursor.executemany("""
    INSERT INTO netsuite_sales_data (
        transaction_id, rep_id, entity, rep_name, customer_name, item_name, item_id,
        sales_date, doc_id, total_quantity, foreign_total_inv,
        foreign_total_credmem, total_amount, total_cost_estimate,
        est_gross_profit
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", rows)

# Clear enriched table before inserting
pg_cursor.execute("DELETE FROM netsuite_dashboard_sales_data;")

# Populate dashboard table with normalized names
pg_cursor.execute("""
    INSERT INTO netsuite_dashboard_sales_data (
        transaction_id, rep_id, entity, rep_name, customer_name, item_name,
        item_id, sales_date, doc_id, total_quantity, foreign_total_inv,
        foreign_total_credmem, total_amount, total_cost_estimate,
        est_gross_profit, last_updated, dashboard_product_name
    )
    SELECT 
        a.transaction_id, a.rep_id, a.entity, 
        CASE
            WHEN a.rep_name LIKE '% - Northeast Scientific, Inc.' THEN
                LEFT(a.rep_name, LENGTH(a.rep_name) - LENGTH(' - Northeast Scientific, Inc.'))
            ELSE a.rep_name
        END AS rep_name,
        CASE
            WHEN a.customer_name LIKE '% - Northeast Scientific, Inc.' THEN
                LEFT(a.customer_name, LENGTH(a.customer_name) - LENGTH(' - Northeast Scientific, Inc.'))
            ELSE a.customer_name
        END AS customer_name,               
        a.item_name, a.item_id, a.sales_date, a.doc_id, a.total_quantity,
        a.foreign_total_inv, a.foreign_total_credmem, a.total_amount,
        a.total_cost_estimate, a.est_gross_profit, a.last_updated,
        b."Name" AS dashboard_product_name
    FROM 
        netsuite_sales_data a
    INNER JOIN 
        dashboard_products b
    ON 
        a.item_id = b."Internal ID"
    WHERE
        a.sales_date > DATE '2022-12-31';
""")

# Finalize
pg_conn.commit()
pg_cursor.close()
pg_conn.close()
netsuite_cursor.close()
netsuite_conn.close()
