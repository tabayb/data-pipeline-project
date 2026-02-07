from pathlib import Path
import requests

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

DATA_URL = "https://raw.githubusercontent.com/curran/data/gh-pages/superstoreSales/superstoreSales.csv"

BASE_DIR = Path("/workspaces/data-pipeline-project")
RAW_DIR = BASE_DIR / "data" / "raw"


@dag(
    schedule=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["etl"],
)
def sales_etl_superstore():

    @task()
    def extract() -> str:
        RAW_DIR.mkdir(parents=True, exist_ok=True)
        out_path = RAW_DIR / "superstore_sales.csv"

        r = requests.get(DATA_URL, timeout=60, allow_redirects=True)
        r.raise_for_status()

        # сохраняем как текст и нормализуем переносы строк, чтобы wc -l работал
        text = r.content.decode("utf-8", errors="replace")
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        out_path.write_text(text, encoding="utf-8")
        print("SAVED:", out_path)
        return str(out_path)

    extract()


sales_etl_superstore()

