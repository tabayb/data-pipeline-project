TRUNCATE TABLE analytics.fact_sales;
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
FROM analytics.sales s
    JOIN analytics.dim_customer d ON s.customer_name = d.customer_name
    AND d.is_current = TRUE -- пока так
    JOIN analytics.dim_product p ON s.product_name = p.product_name
    JOIN analytics.dim_date od ON od.full_date = s.order_date
    JOIN analytics.dim_date sd ON sd.full_date = s.ship_date
    JOIN analytics.dim_ship_mode sm ON sm.ship_mode = s.ship_mode;