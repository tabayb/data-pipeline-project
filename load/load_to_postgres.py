import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch

PROCESSED_PATH = "data/processed/superstore_sales_processed.csv"
TABLE_NAME = "analytics.sales"

DB_CONFIG = {
    "host": "127.0.0.1",          # ⚠️ важно: не localhost
    "port": 5432,
    "dbname": "data_pipeline_project",
    "user": "postgres",
    "password": os.getenv("PG_PASSWORD")
}


def main():
    if DB_CONFIG["password"] is None:
        raise ValueError("PG_PASSWORD environment variable is not set")

    df = pd.read_csv(PROCESSED_PATH)
    print("Data shape:", df.shape)

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS analytics;

        DROP TABLE IF EXISTS {TABLE_NAME};

        CREATE TABLE {TABLE_NAME} (
            row_id INT,
            order_id INT,
            order_date DATE,
            order_priority TEXT,
            order_quantity INT,
            sales NUMERIC,
            discount NUMERIC,
            ship_mode TEXT,
            profit NUMERIC,
            unit_price NUMERIC,
            shipping_cost NUMERIC,
            customer_name TEXT,
            province TEXT,
            region TEXT,
            customer_segment TEXT,
            product_category TEXT,
            product_sub_category TEXT,
            product_name TEXT,
            product_container TEXT,
            product_base_margin NUMERIC,
            ship_date DATE
        );
    """)

    insert_sql = f"""
        INSERT INTO {TABLE_NAME} VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s
        );
    """

    execute_batch(cur, insert_sql, df.values.tolist(), page_size=1000)

    conn.commit()

    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    rows = cur.fetchone()[0]
    print(f"Loaded {rows} rows into {TABLE_NAME}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
