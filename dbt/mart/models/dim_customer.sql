-- models/dim_customer.sql

{{ config(
    materialized='table',
    schema='dims'
) }}

-- สร้างตารางมิติลูกค้า (Grain: 1 แถว ต่อ 1 customer_unique_id)
-- ใช้ window function เพื่อเลือกข้อมูลล่าสุด (city, state, zip) ของลูกค้านั้นๆ
WITH customer_ranked AS (
    SELECT
        c.customer_id,
        c.customer_unique_id,
        CAST(c.customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
        c.customer_city,
        c.customer_state,
        -- จัดลำดับลูกค้า โดยเอา customer_id ล่าสุด (ถ้าอิงตาม FK ในตาราง orders)
        -- หรือจะใช้ ROW_NUMBER() OVER (PARTITION BY c.customer_unique_id ORDER BY o.order_purchase_timestamp DESC) ก็ได้ถ้า Join ตาราง orders
        -- แต่วิธีที่ง่ายที่สุดคือการ GROUP BY
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id 
            ORDER BY c.customer_id DESC -- สมมติว่า customer_id ที่มาทีหลังคือข้อมูลล่าสุด
        ) as rn
    FROM
        {{ source('core', 'customers') }} c
    WHERE 
        c.customer_id IS NOT NULL AND c.customer_unique_id IS NOT NULL
)
SELECT
    -- 1. สร้าง PK (customer_key) ให้ตรงกับที่ fct_sales ใช้
    {{ dbt_utils.generate_surrogate_key(['customer_unique_id']) }} AS customer_key,
    
    -- 2. เก็บ Natural Key
    customer_unique_id,
    
    -- 3. เก็บ Dimensions (ข้อมูลล่าสุด)
    customer_zip_code_prefix,
    customer_city,
    customer_state
FROM
    customer_ranked
WHERE
    rn = 1 -- เลือกมาเฉพาะข้อมูลล่าสุดของลูกค้ารายนั้น