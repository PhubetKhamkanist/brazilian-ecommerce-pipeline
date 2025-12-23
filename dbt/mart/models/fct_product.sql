{{ config(
    materialized='table',
    schema='products'
) }}

-- สร้าง Fact Table วิเคราะห์ประสิทธิภาพสินค้า/ผู้ขาย
-- Grain: 1 แถว ต่อ 1 order_item (รายการสินค้าในออเดอร์)

SELECT
    -- Primary Key (Surrogate Key for this fact table)
    -- ดึงมาจาก core.order_items (MD5(order_id || order_item_id))
    oi.order_item_pk AS product_performance_pk, 

    -- Foreign Keys to Dimensions
    -- (ใช้ตรรกะเดียวกับ dim_product)
    {{ dbt_utils.generate_surrogate_key(['oi.product_id']) }} AS product_key,
    
    -- (ใช้ตรรกะเดียวกับ dim_seller)
    {{ dbt_utils.generate_surrogate_key(['oi.seller_id']) }} AS seller_key,
    
    -- (ใช้ตรรกะเดียวกับ dim_date, ใช้วันที่สั่งซื้อ)
    TO_CHAR(o.order_purchase_timestamp, 'YYYYMMDD')::INT AS order_date_key,

    -- Degenerate Dimensions (เก็บ Natural Key ไว้เผื่อวิเคราะห์)
    oi.order_id,
    oi.product_id,
    oi.seller_id,

    -- Measures
    1 AS units_sold,
    oi.price AS revenue,
    oi.freight_value,

    -- Calculated Measure: Shipping Time
    -- (ใช้การลบ DATE ธรรมดา สำหรับ PostgreSQL)
    (
        CAST(o.order_delivered_customer_date AS DATE)
         - CAST(o.order_purchase_timestamp AS DATE)
    ) AS shipping_time_days

FROM
    {{ source('core', 'order_items') }} oi
JOIN
    {{ source('core', 'orders') }} o
    ON oi.order_id = o.order_id
WHERE
    -- วิเคราะห์เฉพาะออเดอร์ที่ส่งเสร็จแล้วเท่านั้น
    -- (เพื่อให้แน่ใจว่า order_delivered_customer_date ไม่เป็น NULL)
    o.order_status = 'delivered'
    AND o.order_delivered_customer_date IS NOT NULL
    AND o.order_purchase_timestamp IS NOT NULL
