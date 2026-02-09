# Data Pipeline Project

## Project Overview
This project demonstrates a simple end-to-end batch data pipeline
built to practice core data engineering concepts:
- data extraction
- data transformation
- incremental loading into PostgreSQL

The pipeline processes historical sales data from a CSV file and loads
it into a PostgreSQL database using an idempotent (incremental) approach.

---

## Data Source
The source data is a static CSV file containing historical sales transactions.
The dataset represents order-level sales data and includes:

- order and shipping dates
- product and customer information
- sales, profit, and discount metrics

The CSV file is used to simulate a typical raw data source for a batch
ETL pipeline.

Raw data location:
