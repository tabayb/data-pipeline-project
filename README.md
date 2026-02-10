# Data Pipeline Project

## Project Overview
This project demonstrates a simple end-to-end **batch data pipeline**
built to practice core **data engineering fundamentals**:

- data extraction
- data transformation
- data quality checks
- incremental loading into PostgreSQL

The pipeline processes historical sales data from a CSV file and loads
it into a PostgreSQL database using an **idempotent, insert-only incremental approach**.

The goal of the project is to simulate a production-like batch ETL workflow
and apply best practices around data reliability and history preservation.

---

## Tech Stack
- Python
- pandas
- PostgreSQL
- psycopg2
- YAML
- Git

---

## Project Structure
```text
.
├── README.md
├── config.yaml
├── requirements.txt
├── data/
│   ├── raw/
│   └── processed/
│       └── superstore_sales_processed.csv
└── src/
    ├── extract.py
    ├── transform.py
    ├── dq_checks.py
    └── load.py
```
## Data Source
The pipeline uses a static CSV file with historical sales data.
The dataset represents order line–level transactions and includes
dates, product information, customer attributes, and financial metrics.

**Raw data location:**
```
data/raw/superstore_sales.csv
```
**Processed data location:**
```
data/processed/superstore_sales_processed.csv
```

---
## Pipeline Logic:

The pipeline consists of the following steps:

1. **Extract**
   - Reads raw CSV data from the `data/raw` directory.

2. **Transform**
   - Cleans column names
   - Casts data types
   - Parses date fields
   - Applies basic data quality checks

3. **Load**
   - Loads data into PostgreSQL
   - Uses an incremental, insert-only strategy
   - Ensures idempotent execution

---
## How to Run:
1. Create and activate virtual environment
2. Install dependencies:
```
pip install -r requirements.txt
```
3. Run the pipeline:
```
python src/extract.py
python src/transform.py
python src/dq_checks.py
python src/load.py
```
The pipeline can be executed multiple times without creating duplicate records.