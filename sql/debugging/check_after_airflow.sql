DROP TABLE analytics.fact_sales;
CREATE TABLE analytics.fact_sales (
    order_id INT,
    product_id INT,
    customer_id INT,
    order_date DATE,
    sales NUMERIC,
    profit NUMERIC,
    PRIMARY KEY (order_id, product_id)
);
SELECT COUNT(*)
FROM analytics.fact_sales;
SELECT COUNT(*)
FROM analytics.sales_stg;
SELECT COUNT(*)
FROM analytics.fact_sales;
DROP TABLE analytics.fact_sales;
UPDATE analytics.sales
SET sales = sales + 100
WHERE order_id = 3;
UPDATE analytics.sales
SET updated_at = NOW()
WHERE order_id = 3;
SELECT MAX(updated_at)
FROM analytics.fact_sales;
SELECT updated_at
FROM analytics.sales
WHERE order_id = 3;
UPDATE analytics.sales
SET updated_at = NOW() + interval '1 minute'
WHERE order_id = 3;
SELECT order_id,
    updated_at
FROM analytics.sales
WHERE order_id = 3;
-----
SELECT COUNT(*)
FROM analytics.dim_product;
1265
SELECT COUNT(*)
FROM analytics.dim_ship_mode;
4
SELECT COUNT(*)
FROM analytics.fact_sales;
SELECT order_id,
    COUNT(*)
FROM analytics.fact_sales
GROUP BY order_id
HAVING COUNT(*) > 1;
SELECT COUNT(*)
FROM analytics.fact_sales;
SELECT order_id,
    COUNT(DISTINCT product_id)
FROM analytics.fact_sales
GROUP BY order_id
HAVING COUNT(*) > 1;
SELECT order_id,
    product_id,
    COUNT(*)
FROM analytics.fact_sales
GROUP BY order_id,
    product_id
HAVING COUNT(*) > 1;