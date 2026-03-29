-- select *
-- from analytics.sales
-- limit 10;
-- select order_id, product_name, count(*)
-- from analytics.sales
-- GROUP BY order_id, product_name
-- HAVING count(*) > 1;
select order_id,
    product_name,
    sum(order_quantity) as qnty,
    sum(sales) as sales,
    sum(profit) as profit
from analytics.sales
GROUP BY order_id,
    product_name;
#checking for duplicates
SELECT order_id,
    product_name,
    COUNT(*)
FROM analytics.sales_clean
GROUP BY order_id,
    product_name
HAVING COUNT(*) > 1;
#creating Staging Sales
drop table if EXISTS analytics.sales_stg;
create table analytics.sales_stg as
SELECT *
FROM analytics.sales;
select *
from analytics.sales_stg er (
        customer_name,
        #creating dim_customer(scd type 2)
        DROP TABLE analytics.dim_customer;
CREATE TABLE analytics.dim_customer AS
SELECT DISTINCT ON (customer_name) customer_name,
    customer_segment,
    region,
    province,
    CURRENT_DATE AS valid_from,
    NULL::DATE AS valid_to,
    TRUE AS is_current
FROM analytics.sales_stg
ORDER BY customer_name;
ALTER TABLE analytics.dim_customer
ADD COLUMN customer_id SERIAL PRIMARY KEY;
#first insert of dim_customer
INSERT INTO analytics.dim_customer customer_segment,
    region,
    province,
    valid_from,
    valid_to,
    is_current
)
SELECT DISTINCT customer_name,
    customer_segment,
    region,
    province,
    CURRENT_DATE,
    NULL::DATE,
    TRUE
FROM analytics.sales_stg;
select *
from analytics.dim_customer #creating dim_product (scd type 3)
    DROP TABLE IF EXISTS analytics.dim_product;
CREATE TABLE analytics.dim_product AS
SELECT DISTINCT ON (product_name) product_name,
    product_category,
    product_sub_category,
    product_container,
    NULL AS prev_product_category
FROM analytics.sales_stg
ORDER BY product_name;
ALTER TABLE analytics.dim_product
ADD COLUMN product_id SERIAL PRIMARY KEY;
#first insert of dim_product
INSERT INTO analytics.dim_product (
        product_name,
        product_category,
        product_sub_category,
        product_container,
        prev_product_category
    )
SELECT DISTINCT product_name,
    product_category,
    product_sub_category,
    product_container,
    NULL
FROM analytics.sales_stg;
select *
from analytics.dim_product;
#creating dim_date 
DROP TABLE IF EXISTS analytics.dim_date;
CREATE TABLE analytics.dim_date (
    date_id SERIAL PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    day INT,
    weekday INT
);
#first insert of data_dim
INSERT INTO analytics.dim_date (full_date, year, month, day, weekday)
SELECT DISTINCT d::DATE,
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
select *
from analytics.dim_date;
#creating dim_ship mode
DROP TABLE IF EXISTS analytics.dim_ship_mode;
CREATE TABLE analytics.dim_ship_mode (
    ship_mode_id SERIAL PRIMARY KEY,
    ship_mode TEXT
);
#insert to dim_ship_mode:
INSERT INTO analytics.dim_ship_mode (ship_mode)
SELECT DISTINCT ship_mode
FROM analytics.sales_stg;
select *
from analytics.dim_ship_mode;
#creating FACT SALES
DROP TABLE IF EXISTS analytics.fact_sales;
CREATE TABLE analytics.fact_sales AS
SELECT s.order_id,
    p.product_id,
    c.customer_id,
    d1.date_id AS order_date_id,
    d2.date_id AS ship_date_id,
    sm.ship_mode_id,
    SUM(s.order_quantity) AS quantity,
    SUM(s.sales) AS sales,
    SUM(s.profit) AS profit,
    SUM(s.discount) AS discount,
    SUM(s.shipping_cost) AS shipping_cost
FROM analytics.sales_stg s
    JOIN analytics.dim_customer c ON s.customer_name = c.customer_name
    AND c.is_current = TRUE
    JOIN analytics.dim_product p ON s.product_name = p.product_name
    JOIN analytics.dim_date d1 ON s.order_date = d1.full_date
    JOIN analytics.dim_date d2 ON s.ship_date = d2.full_date
    JOIN analytics.dim_ship_mode sm ON s.ship_mode = sm.ship_mode
GROUP BY s.order_id,
    p.product_id,
    c.customer_id,
    d1.date_id,
    d2.date_id,
    sm.ship_mode_id;
#adding PK
ALTER TABLE analytics.fact_sales
ADD PRIMARY KEY (order_id, product_id);
SELECT product_name,
    COUNT(*)
FROM analytics.dim_product
GROUP BY product_name
HAVING COUNT(*) > 1;
SELECT customer_name,
    COUNT(*)
FROM analytics.dim_customer
WHERE is_current = TRUE
GROUP BY customer_name
HAVING COUNT(*) > 1;