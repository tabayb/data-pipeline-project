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


@dag(
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["ingestion"],
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
    load(raw)


sales_etl_superstore()