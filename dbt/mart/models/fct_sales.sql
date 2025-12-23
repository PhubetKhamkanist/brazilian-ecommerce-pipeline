{{ config(
    materialized='table',
    schema='sales'
) }}

-- สร้างตารางข้อเท็จจริง (Fact Table) สำหรับการวิเคราะห์ยอดขาย
SELECT
    -- Surrogate Keys from Dimensions
    {{ dbt_utils.generate_surrogate_key(['c.customer_unique_id']) }} AS customer_key,
    {{ dbt_utils.generate_surrogate_key(['oi.product_id']) }} AS product_key,
    {{ dbt_utils.generate_surrogate_key(['oi.seller_id']) }} AS seller_key,
    TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INT AS order_date_key,

    -- Degenerate Dimension
    o.order_id,
    
    -- Measures
    oi.price,
    oi.freight_value,
    op.total_payment_value,
    r.avg_review_score,
    1 AS order_item_count -- แต่ละแถวใน order_items คือ 1 ชิ้น

FROM
    {{ source('core', 'orders') }} o
    JOIN {{ source('core', 'order_items') }} oi ON o.order_id = oi.order_id
    JOIN {{ source('core', 'customers') }} c ON o.customer_id = c.customer_id
    LEFT JOIN (
        -- Aggregate payments per order
        SELECT
            order_id,
            SUM(payment_value) AS total_payment_value
        FROM
            {{ source('core', 'order_payment') }}
        GROUP BY 1
    ) op ON o.order_id = op.order_id
    LEFT JOIN (
        -- Aggregate review scores per order
        SELECT
            order_id,
            AVG(review_score) AS avg_review_score
        FROM
            {{ source('core', 'order_reviews') }}
        GROUP BY 1
    ) r ON o.order_id = r.order_id

-- Filter for completed orders to analyze actual sales
WHERE
    o.order_status = 'delivered'