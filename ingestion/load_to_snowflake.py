import os
from dotenv import load_dotenv
import snowflake.connector
import pandas as pd

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))

def get_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )
    return conn

def create_raw_tables(conn):
    cur = conn.cursor()
    try:
        cur.execute("USE SCHEMA RAW")

        cur.execute(
            """
            CREATE OR REPLACE TABLE TRANSACTIONS_RAW (
                TRANSACTION_ID STRING,
                CUSTOMER_ID STRING,
                PRODUCT_ID STRING,
                AMOUNT_CENTS NUMBER,
                CURRENCY STRING,
                STATUS STRING,
                CREATED_AT TIMESTAMP_NTZ
            )
            """
        )

        cur.execute(
            """
            CREATE OR REPLACE TABLE APP_EVENTS_RAW (
                EVENT_ID STRING,
                USER_ID STRING,
                EVENT_TYPE STRING,
                EVENT_TIMESTAMP TIMESTAMP_NTZ
            )
            """
        )
    finally:
        cur.close()

def load_csv_to_table(conn, csv_path, table_name):
    df = pd.read_csv(csv_path)
    cur = conn.cursor()
    try:
        cur.execute("USE SCHEMA RAW")
        for _, row in df.iterrows():
            cols = ",".join(df.columns)
            placeholders = ",".join(["%s"] * len(df.columns))
            cur.execute(
                f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
                list(row.values),
            )
    finally:
        cur.close()

def main():
    conn = get_connection()
    try:
        create_raw_tables(conn)

        base_dir = os.path.dirname(__file__)
        tx_path = os.path.join(base_dir, "data", "transactions.csv")
        ev_path = os.path.join(base_dir, "data", "app_events.csv")

        load_csv_to_table(conn, tx_path, "TRANSACTIONS_RAW")
        load_csv_to_table(conn, ev_path, "APP_EVENTS_RAW")
        print("Loaded data into RAW schema")
    finally:
        conn.close()

if __name__ == "__main__":
    main()
