TRUNCATE TABLE analytics.sales_stg;
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