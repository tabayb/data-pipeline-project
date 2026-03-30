###Data Quality CHECKS
#--1️⃣ Проверка количества строк--#
SELECT COUNT(*)
FROM analytics.sales;
SELECT COUNT(*)
FROM analytics.sales_stg;
#--2️⃣ Проверка NULL (критично!)--#
SELECT COUNT(*) FILTER (
        WHERE order_id IS NULL
    ) AS order_id_nulls,
    COUNT(*) FILTER (
        WHERE product_name IS NULL
    ) AS product_nulls,
    COUNT(*) FILTER (
        WHERE customer_name IS NULL
    ) AS customer_nulls
FROM analytics.sales_stg;
#--3️⃣ Проверка дубликатов--#
SELECT order_id,
    product_name,
    COUNT(*)
FROM analytics.sales_stg
GROUP BY order_id,
    product_name
HAVING COUNT(*) > 1;
#--4️⃣ Проверка метрик (очень важно)--#
SELECT SUM(order_quantity) AS qty,
    SUM(sales) AS sales,
    SUM(profit) AS profit
FROM analytics.sales;
SELECT SUM(order_quantity) AS qty,
    SUM(sales) AS sales,
    SUM(profit) AS profit
FROM analytics.sales_stg;
#--5️⃣ Проверка уникальности dimension ключей--#
SELECT customer_name,
    COUNT(*)
FROM analytics.sales_stg
GROUP BY customer_name
ORDER BY COUNT(*) DESC;
UPDATE analytics.sales_stg
SET customer_name = TRIM(LOWER(customer_name));
SELECT customer_name,
    COUNT(*)
FROM analytics.sales_stg
GROUP BY customer_name
HAVING COUNT(*) > 100;
#--🔥 ФИНАЛЬНЫЕ ПРОВЕРКИ (ОБЯЗАТЕЛЬНО) после insert fact--#
#--1️⃣ Количество строк
SELECT COUNT(*)
FROM analytics.fact_sales;
#--2️⃣ Метрики совпадают?
SELECT SUM(sales)
FROM analytics.sales_stg;
SELECT SUM(sales)
FROM analytics.fact_sales;
#--3️⃣ Потери строк (очень важно)
SELECT COUNT(*)
FROM analytics.sales_stg;
SELECT COUNT(*)
FROM analytics.fact_sales;
SELECT order_id,
    product_name,
    COUNT(*)
FROM analytics.sales_stg
GROUP BY order_id,
    product_name
HAVING COUNT(*) > 1;
SELECT *
FROM analytics.sales_stg
WHERE order_id IN ('32800', '37603');
select *
from analytics.fact_sales;
select *
from analytics.dim_customer;
--------------------------------------------------
--ШАГ 1 — загрузка STAGING (clean)
INSERT INTO analytics.sales_stg
SELECT DISTINCT ON (customer_name) order_id,
    order_date,
    ship_date,
    customer_name,
    customer_segment,
    region,
    province,
    product_name,
    product_category,
    product_sub_category,
    product_container,
    ship_mode,
    order_quantity,
    sales,
    profit,
    discount,
    shipping_cost
FROM analytics.sales
ORDER BY customer_name,
    order_date DESC;
select count(*)
from analytics.sales;
select count(*)
from analytics.sales_stg;
--dim_customer (initial load SCD2)
INSERT INTO analytics.dim_customer (
        customer_name,
        customer_segment,
        region,
        province,
        valid_from,
        valid_to,
        is_current
    )
SELECT customer_name,
    customer_segment,
    region,
    province,
    CURRENT_DATE,
    NULL,
    TRUE
FROM analytics.sales_stg;
--dim_product (ВАЖНО: из sales!)
INSERT INTO analytics.dim_product (
        product_name,
        product_category,
        product_sub_category,
        product_container
    )
SELECT DISTINCT ON (product_name) product_name,
    product_category,
    product_sub_category,
    product_container
FROM analytics.sales
ORDER BY product_name,
    order_date DESC;
SELECT COUNT(*)
FROM analytics.dim_product;
--dim_ship_mode
INSERT INTO analytics.dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode
FROM analytics.sales;
SELECT *
FROM analytics.dim_ship_mode;
--FACT (финальный шаг)
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
SELECT s.order_id,
    p.product_id,
    d.customer_id,
    od.date_id,
    sd.date_id,
    sm.ship_mode_id,
    s.order_quantity,
    s.sales,
    s.profit,
    s.discount,
    s.shipping_cost
FROM analytics.sales s -- 🔥 SCD2 (пока current, позже усложним)
    JOIN analytics.dim_customer d ON s.customer_name = d.customer_name
    AND d.is_current = TRUE
    JOIN analytics.dim_product p ON s.product_name = p.product_name
    JOIN analytics.dim_date od ON od.full_date = s.order_date
    JOIN analytics.dim_date sd ON sd.full_date = s.ship_date
    JOIN analytics.dim_ship_mode sm ON sm.ship_mode = s.ship_mode;
SELECT COUNT(*)
FROM analytics.sales;
SELECT COUNT(*)
FROM analytics.fact_sales;