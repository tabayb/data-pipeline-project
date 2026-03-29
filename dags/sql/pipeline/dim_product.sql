TRUNCATE TABLE analytics.dim_product RESTART IDENTITY CASCADE;
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