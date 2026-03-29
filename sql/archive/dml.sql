#--✅ 1. dim_customer (initial load)
WITH src AS (
  SELECT *
  FROM (
      SELECT customer_name,
        customer_segment,
        region,
        province,
        order_date,
        ROW_NUMBER() OVER (
          PARTITION BY customer_name
          ORDER BY order_date DESC
        ) as rn
      FROM analytics.sales_stg
    ) t
  WHERE rn = 1
)
INSERT INTO analytics.dim_customer (
    customer_name,
    customer_segment,
    region,
    province,
    valid_from,
    valid_to,
    is_current
  )
SELECT s.customer_name,
  s.customer_segment,
  s.region,
  s.province,
  s.order_date,
  -- 🔥 ключевой фикс
  NULL,
  TRUE
FROM src s;
#--dim_product (SCD3)--#
INSERT INTO analytics.dim_product (
    product_name,
    product_category,
    prev_product_category,
    product_sub_category,
    product_container
  )
SELECT DISTINCT ON (product_name) product_name,
  product_category,
  NULL,
  product_sub_category,
  product_container
FROM analytics.sales_stg
ORDER BY product_name;
#checking products
SELECT product_name,
  COUNT(*)
FROM analytics.dim_product
GROUP BY product_name
HAVING COUNT(*) > 1;
#--dim_date--#
INSERT INTO analytics.dim_date (
    date_id,
    full_date,
    year,
    month,
    day,
    weekday
  )
SELECT DISTINCT TO_CHAR(d, 'YYYYMMDD')::INT,
  d,
  EXTRACT(
    YEAR
    FROM d
  ),
  EXTRACT(
    MONTH
    FROM d
  ),
  EXTRACT(
    DAY
    FROM d
  ),
  EXTRACT(
    DOW
    FROM d
  )
FROM (
    SELECT order_date AS d
    FROM analytics.sales_stg
    UNION
    SELECT ship_date
    FROM analytics.sales_stg
  ) t;
#checking date
SELECT COUNT(*)
FROM analytics.dim_date;
#--dim_ship_mode--#
INSERT INTO analytics.dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode
FROM analytics.sales_stg;
SELECT *
FROM analytics.dim_ship_mode;
#--fact_sales--#
INSERT INTO analytics.fact_sales (
    order_id,
    product_id,
    customer_sk,
    order_date_id,
    ship_date_id,
    ship_mode_id,
    quantity,
    sales,
    profit,
    discount,
    shipping_cost
  )
SELECT f.order_id,
  p.product_id,
  d.customer_sk,
  dd1.date_id AS order_date_id,
  dd2.date_id AS ship_date_id,
  sm.ship_mode_id,
  f.order_quantity,
  f.sales,
  f.profit,
  f.discount,
  f.shipping_cost
FROM analytics.sales_stg f -- product
  JOIN analytics.dim_product p ON TRIM(f.product_name) = TRIM(p.product_name) -- customer (временно current)
  JOIN analytics.dim_customer d ON f.customer_name = d.customer_name
  AND d.is_current = TRUE -- dates
  JOIN analytics.dim_date dd1 ON f.order_date = dd1.full_date
  JOIN analytics.dim_date dd2 ON f.ship_date = dd2.full_date -- ship mode
  JOIN analytics.dim_ship_mode sm ON TRIM(f.ship_mode) = TRIM(sm.ship_mode);
--
SELECT COUNT(*) FROM analytics.dim_customer;
SELECT COUNT(*)
FROM analytics.fact_sales;
SELECT *
FROM analytics.sales_stg f
  LEFT JOIN analytics.dim_product p ON f.product_name = p.product_name
  LEFT JOIN analytics.dim_customer d ON f.customer_name = d.customer_name
  AND d.is_current = TRUE
  LEFT JOIN analytics.dim_date dd1 ON f.order_date = dd1.full_date
  LEFT JOIN analytics.dim_date dd2 ON f.ship_date = dd2.full_date
  LEFT JOIN analytics.dim_ship_mode sm ON f.ship_mode = sm.ship_mode
WHERE p.product_id IS NULL
  OR d.customer_sk IS NULL
  OR dd1.date_id IS NULL
  OR dd2.date_id IS NULL
  OR sm.ship_mode_id IS NULL
LIMIT 20;
TRUNCATE TABLE analytics.fact_sales;
SELECT COUNT(*)
FROM analytics.fact_sales;
--это тот же INSERT, но с фильтром
INSERT INTO analytics.fact_sales (
    order_id,
    product_id,
    customer_sk,
    order_date_id,
    ship_date_id,
    ship_mode_id,
    quantity,
    sales,
    profit,
    discount,
    shipping_cost
  )
SELECT f.order_id,
  p.product_id,
  d.customer_sk,
  dd1.date_id AS order_date_id,
  dd2.date_id AS ship_date_id,
  sm.ship_mode_id,
  f.order_quantity,
  f.sales,
  f.profit,
  f.discount,
  f.shipping_cost
FROM analytics.sales_stg f -- product
  JOIN analytics.dim_product p ON TRIM(f.product_name) = TRIM(p.product_name) -- customer
  JOIN analytics.dim_customer d ON f.customer_name = d.customer_name
  AND d.is_current = TRUE -- dates
  JOIN analytics.dim_date dd1 ON f.order_date = dd1.full_date
  JOIN analytics.dim_date dd2 ON f.ship_date = dd2.full_date -- ship mode
  JOIN analytics.dim_ship_mode sm ON TRIM(f.ship_mode) = TRIM(sm.ship_mode) -- 🔥 incremental фильтр
WHERE NOT EXISTS (
    SELECT 1
    FROM analytics.fact_sales f2
    WHERE f2.order_id = f.order_id
  );