import sys
from pathlib import Path
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

# ===== PATH FIX (ВАЖНО) =====
BASE_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(BASE_DIR))

DATA_URL = "https://raw.githubusercontent.com/curran/data/gh-pages/superstoreSales/superstoreSales.csv"

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

        # стандартизация колонок
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
            .str.replace("-", "_")
        )

        print("Columns:", list(df.columns))

        # даты
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

        # проверка NULL
        null_order = df["order_date"].isna().sum()
        null_ship = df["ship_date"].isna().sum()

        print(f"NULL order_date: {null_order}")
        print(f"NULL ship_date: {null_ship}")

        # очистка
        before = len(df)
        df = df.dropna(subset=["order_date", "ship_date"])
        after = len(df)

        print(f"Dropped rows: {before - after}")
        print("Rows after clean:", after)

        # сохраняем
        PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
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
