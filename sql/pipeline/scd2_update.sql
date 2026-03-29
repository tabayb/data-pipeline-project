UPDATE analytics.dim_customer d
SET valid_to = CURRENT_DATE,
    is_current = FALSE
FROM analytics.sales_stg s
WHERE d.customer_name = s.customer_name
    AND d.is_current = TRUE
    AND (
        s.customer_segment IS DISTINCT
        FROM d.customer_segment
            OR s.region IS DISTINCT
        FROM d.region
            OR s.province IS DISTINCT
        FROM d.province
    );