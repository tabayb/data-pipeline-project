import pandas as pd

RAW_PATH = "data/raw/superstore_sales.csv"
PROCESSED_PATH = "data/processed/superstore_sales_processed.csv"


def run_data_quality_checks(df):
    errors = []

    # dates should not be null
    if df["order_date"].isna().any():
        errors.append("order_date contains NULL values")

    if df["ship_date"].isna().any():
        errors.append("ship_date contains NULL values")

    # discount should be between 0 and 1
    if not df["discount"].between(0, 1).all():
        errors.append("discount out of range [0,1]")

    # ship date should not be earlier than order date
    if (df["ship_date"] < df["order_date"]).any():
        errors.append("ship_date earlier than order_date")

    if errors:
        raise ValueError("DATA QUALITY FAILED:\n" + "\n".join(errors))

    print("Data quality checks passed")


def main():
    # ===== EXTRACT =====
    df = pd.read_csv(RAW_PATH)

    # ===== TRANSFORM =====
    # standardize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    # convert dates
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["ship_date"] = pd.to_datetime(df["ship_date"], errors="coerce")

    print("=== COLUMNS ===")
    print(df.columns.tolist())

    print("\n=== DTYPES AFTER TRANSFORM ===")
    print(df.dtypes)

    print("\n=== NULL DATES CHECK ===")
    print(df[["order_date", "ship_date"]].isna().sum())

    # ===== DATA QUALITY =====
    run_data_quality_checks(df)

    # ===== LOAD =====
    df.to_csv(PROCESSED_PATH, index=False)
    print(f"\nSaved processed data to {PROCESSED_PATH}")


if __name__ == "__main__":
    main()
