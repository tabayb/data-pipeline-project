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
    CURRENT_DATE,
    NULL,
    TRUE
FROM analytics.sales_stg s
    LEFT JOIN analytics.dim_customer d ON s.customer_name = d.customer_name
    AND d.is_current = TRUE
WHERE d.customer_name IS NULL
    OR (
        s.customer_segment IS DISTINCT
        FROM d.customer_segment
            OR s.region IS DISTINCT
        FROM d.region
            OR s.province IS DISTINCT
        FROM d.province
    );