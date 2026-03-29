#creating STAGING RAW copy
CREATE TABLE analytics.sales_stg (
    order_id TEXT,
    order_date DATE,
    ship_date DATE,
    customer_name TEXT,
    customer_segment TEXT,
    region TEXT,
    province TEXT,
    product_name TEXT,
    product_category TEXT,
    product_sub_category TEXT,
    product_container TEXT,
    ship_mode TEXT,
    order_quantity INT,
    sales NUMERIC(12, 2),
    profit NUMERIC(12, 2),
    discount NUMERIC(5, 2),
    shipping_cost NUMERIC(12, 2)
);
#load from sales to Staging
INSERT INTO analytics.sales_stg (
        order_id,
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
    )
SELECT order_id,
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
FROM analytics.sales;
select *
from analytics.sales_stg;
#--✅ 1. dim_customer (SCD2)
CREATE TABLE analytics.dim_customer (
    customer_id BIGSERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    customer_segment TEXT,
    region TEXT,
    province TEXT,
    valid_from DATE NOT NULL,
    valid_to DATE,
    is_current BOOLEAN NOT NULL,
    UNIQUE (customer_name, valid_from)
);
#--✅ 2. dim_product (SCD3)
CREATE TABLE analytics.dim_product (
    product_id BIGSERIAL PRIMARY KEY,
    product_name TEXT NOT NULL,
    product_category TEXT,
    prev_product_category TEXT,
    product_sub_category TEXT,
    product_container TEXT,
    UNIQUE (product_name)
);
#--✅ 3. dim_date
CREATE TABLE analytics.dim_date (
    date_id INT PRIMARY KEY,
    full_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    weekday INT
);
#--✅ 4. dim_ship_mode
CREATE TABLE analytics.dim_ship_mode (
    ship_mode_id SMALLSERIAL PRIMARY KEY,
    ship_mode TEXT UNIQUE
);
#--✅ 5. fact_sales
CREATE TABLE analytics.fact_sales (
    fact_id BIGSERIAL PRIMARY KEY,
    order_id TEXT NOT NULL,
    product_id BIGINT NOT NULL,
    customer_sk BIGINT NOT NULL,
    order_date_id INT NOT NULL,
    ship_date_id INT NOT NULL,
    ship_mode_id INT NOT NULL,
    quantity INT,
    sales NUMERIC(12, 2),
    profit NUMERIC(12, 2),
    discount NUMERIC(5, 2),
    shipping_cost NUMERIC(12, 2),
    FOREIGN KEY (product_id) REFERENCES analytics.dim_product(product_id),
    FOREIGN KEY (customer_sk) REFERENCES analytics.dim_customer(customer_sk),
    FOREIGN KEY (order_date_id) REFERENCES analytics.dim_date(date_id),
    FOREIGN KEY (ship_date_id) REFERENCES analytics.dim_date(date_id),
    FOREIGN KEY (ship_mode_id) REFERENCES analytics.dim_ship_mode(ship_mode_id)
);