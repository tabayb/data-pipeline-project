{{ config(
    materialized='incremental',
    unique_key=['order_id', 'product_id']
) }}

WITH source_data AS (

    SELECT
        s.order_id,
        p.product_id,
        MAX(d.customer_id) as customer_id,
        MIN(s.order_date) as order_date,
        SUM(s.sales) as sales,
        SUM(s.profit) as profit,
        MAX(s.updated_at) as updated_at

    FROM {{ ref('stg_sales') }} s

    -- 🧠 JOIN сначала (правильный порядок SQL)
    JOIN (
        SELECT *
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY customer_name
                       ORDER BY is_current DESC
                   ) as rn
            FROM analytics.dim_customer
        ) t
        WHERE rn = 1
    ) d
    ON TRIM(LOWER(s.customer_name)) = TRIM(LOWER(d.customer_name))

    JOIN (
        SELECT DISTINCT product_name, product_id
        FROM analytics.dim_product
    ) p
    ON TRIM(LOWER(s.product_name)) = TRIM(LOWER(p.product_name))

    -- 🔥 incremental фильтр (исправленный)
    {% if is_incremental() %}
    WHERE s.updated_at > (
        SELECT COALESCE(MAX(updated_at), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}

    GROUP BY
        s.order_id,
        p.product_id

)

SELECT * FROM source_data
