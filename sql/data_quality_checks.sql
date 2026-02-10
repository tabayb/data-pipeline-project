-- Checking for duplicate order_ids
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT order_id) AS distinct_orders
FROM analytics.sales;

-- Inspect duplicate order_ids
SELECT
    order_id,
    COUNT(*) AS cnt
FROM analytics.sales
GROUP BY order_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC
LIMIT 10;

-- Example duplicate inspection
SELECT *
FROM analytics.sales
WHERE order_id = 24132
ORDER BY order_date;
