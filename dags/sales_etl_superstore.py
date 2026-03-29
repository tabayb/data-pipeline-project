import sys
from pathlib import Path
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# ===== PATH FIX =====
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

DATA_URL = "https://raw.githubusercontent.com/tabayb/data-pipeline-project/main/data/raw/superstore_sales.csv"

RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"


@dag(
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["etl"],
)
def sales_etl_superstore():

    # ===== EXTRACT =====
    @task()
    def extract() -> str:
        RAW_DIR.mkdir(parents=True, exist_ok=True)

        out_path = RAW_DIR / "superstore_sales.csv"

        r = requests.get(DATA_URL, timeout=60)
        r.raise_for_status()

        text = r.content.decode("utf-8", errors="replace")
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        out_path.write_text(text, encoding="utf-8")

        print("EXTRACT DONE:", out_path)
        return str(out_path)

    # ===== TRANSFORM =====
    @task()
    def transform(file_path: str) -> str:
        import pandas as pd

        print("=== TRANSFORM START ===")
        print("INPUT FILE:", file_path)

        df = pd.read_csv(file_path)

        print("Rows before transform:", len(df))

        # ===== CLEAN COLUMNS =====
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )

        print("Columns:", list(df.columns))

        # ===== FIX MIXED DATE FORMATS =====
        def parse_date(col):
            # формат 1: MM/DD/YYYY (старые данные)
            parsed = pd.to_datetime(col, format="%m/%d/%Y", errors="coerce")

            # формат 2: DD-MM-YY (твои новые строки)
            mask = parsed.isna()
            parsed[mask] = pd.to_datetime(col[mask], format="%d-%m-%y", errors="coerce")

            return parsed

        df["order_date"] = parse_date(df["order_date"])
        df["ship_date"] = parse_date(df["ship_date"])

        # ===== DATA QUALITY CHECK =====
        null_order = df["order_date"].isna().sum()
        null_ship = df["ship_date"].isna().sum()

        print(f"NULL order_date: {null_order}")
        print(f"NULL ship_date: {null_ship}")

        # ===== SAVE BAD ROWS =====
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
        bad_rows = df[df["order_date"].isna() | df["ship_date"].isna()]

        if not bad_rows.empty:
            bad_path = PROCESSED_DIR / "bad_rows.csv"
            bad_rows.to_csv(bad_path, index=False)
            print(f"Bad rows saved: {len(bad_rows)} → {bad_path}")

        # ===== CLEAN DATA =====
        before = len(df)
        df = df.dropna(subset=["order_date", "ship_date"])
        after = len(df)

        print(f"Dropped rows: {before - after}")
        print("Rows after clean:", after)

        # ===== SAVE PROCESSED =====
        out_path = PROCESSED_DIR / "superstore_sales_processed.csv"
        df.to_csv(out_path, index=False)

        print("OUTPUT FILE:", out_path)
        print("=== TRANSFORM DONE ===")

        return str(out_path)

    # ===== LOAD =====
    @task()
    def load(file_path: str):
        try:
            from load.load_to_postgres import main as load_main

            print("LOAD INPUT:", file_path)

            load_main(file_path)

            print("LOAD DONE")

        except Exception as e:
            print("LOAD ERROR:", e)
            raise

    # ===== PIPELINE =====
    raw = extract()
    processed = transform(raw)
    load(processed)


sales_etl_superstore()
