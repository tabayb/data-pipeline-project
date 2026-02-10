import os
import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch
import yaml


def main():
    # ===== LOAD CONFIG =====
    with open("config.yaml") as f:
        config = yaml.safe_load(f)

    PROCESSED_PATH = config["paths"]["processed_data"]
    TABLE_NAME = "analytics.sales"

    DB_CONFIG = {
        "host": config["database"]["host"],
        "port": config["database"]["port"],
        "dbname": config["database"]["name"],
        "user": config["database"]["user"],
        "password": os.getenv("PG_PASSWORD"),
    }

    # ===== SAFETY CHECK =====
    if DB_CONFIG["password"] is None:
        raise ValueError("PG_PASSWORD environment variable is not set")

    # ===== READ DATA =====
    df = pd.read_csv(PROCESSED_PATH)
    print("Data shape (raw):", df.shape)

    # ===== CONNECT TO DB =====
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # ===== CREATE SCHEMA + TABLE (NO DROP) =====
    cur.execute(f"""
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            row_id INT PRIMARY KEY,
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

    # ===== WATERMARK (MAX row_id) =====
    cur.execute(f"SELECT COALESCE(MAX(row_id), 0) FROM {TABLE_NAME}")
    max_row_id = cur.fetchone()[0]
    print(f"Max row_id in DB: {max_row_id}")

    # ===== FILTER ONLY NEW ROWS =====
    df_new = df[df["row_id"] > max_row_id]
    print("Rows to insert after filtering:", df_new.shape)

    if df_new.empty:
        print("No new rows to insert. Incremental load skipped.")
        cur.close()
        conn.close()
        return

    # ===== INSERT ONLY =====
    insert_sql = f"""
        INSERT INTO {TABLE_NAME} (
            row_id,
            order_id,
            order_date,
            order_priority,
            order_quantity,
            sales,
            discount,
            ship_mode,
            profit,
            unit_price,
            shipping_cost,
            customer_name,
            province,
            region,
            customer_segment,
            product_category,
            product_sub_category,
            product_name,
            product_container,
            product_base_margin,
            ship_date
        ) VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s
        );
    """

    execute_batch(
        cur,
        insert_sql,
        df_new.values.tolist(),
        page_size=1000
    )

    conn.commit()

    # ===== FINAL CHECK =====
    cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    total_rows = cur.fetchone()[0]
    print(f"Total rows in {TABLE_NAME}: {total_rows}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
