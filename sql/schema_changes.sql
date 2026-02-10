-- Verify row_id uniqueness
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT row_id) AS distinct_rows
FROM analytics.sales;

-- Add primary key on row_id
ALTER TABLE analytics.sales
ADD CONSTRAINT sales_pk PRIMARY KEY (row_id);

-- Sanity checks
SELECT
    MIN(row_id) AS min_row_id,
    MAX(row_id) AS max_row_id
FROM analytics.sales;
