{{ config(
    materialized='table',
    schema='customers'
) }}

-- 1. สรุปยอดรวมการชำระเงิน (Total Spent) ต่อ 1 ออเดอร์
WITH order_payments AS (
    SELECT
        order_id,
        SUM(payment_value) AS total_payment_value
    FROM
        {{ source('core', 'order_payment') }}
    GROUP BY
        1
),

-- 2. สรุปจำนวนสินค้า (Total Items) ต่อ 1 ออเดอร์
order_items_agg AS (
    SELECT
        order_id,
        COUNT(order_item_id) AS total_items
    FROM
        {{ source('core', 'order_items') }}
    GROUP BY
        1
),

-- 3. สรุปรีวิว (Avg Score) ต่อ 1 ออเดอร์
order_reviews_agg AS (
    SELECT
        order_id,
        AVG(review_score) AS avg_review_score
    FROM
        {{ source('core', 'order_reviews') }}
    GROUP BY
        1
),

-- 4. รวบรวมข้อมูลออเดอร์, ลูกค้า, และยอดที่คำนวณไว้
customer_orders AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp,
        p.total_payment_value,
        i.total_items,
        r.avg_review_score
    FROM
        {{ source('core', 'orders') }} o
        JOIN {{ source('core', 'customers') }} c
            ON o.customer_id = c.customer_id
        LEFT JOIN order_payments p
            ON o.order_id = p.order_id
        LEFT JOIN order_items_agg i
            ON o.order_id = i.order_id
        LEFT JOIN order_reviews_agg r
            ON o.order_id = r.order_id
    WHERE
        -- ไม่นับออเดอร์ที่ถูกยกเลิก หรือยังไม่เสร็จ
        o.order_status NOT IN ('canceled', 'unavailable', 'created') 
),

-- 5. สรุปข้อมูลทั้งหมดในระดับลูกค้า (Grain: customer_unique_id)
final AS (
    SELECT
        customer_unique_id,

        -- Metrics
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(total_payment_value) AS total_spent,
        SUM(total_items) AS total_items_purchased,
        AVG(avg_review_score) AS avg_review_score, -- คะแนนรีวิวเฉลี่ย (ของออเดอร์ทั้งหมด)
        
        -- Date metrics
        MIN(order_purchase_timestamp) AS first_purchase_date,
        MAX(order_purchase_timestamp) AS last_purchase_date,
        
        -- Recency (✅ แก้ไขตรงนี้ - เปลี่ยน DATE_DIFF เป็นการลบ DATE ธรรมดา)
        (
            CAST({{ current_timestamp() }} AS DATE) 
            - CAST(MAX(order_purchase_timestamp) AS DATE)
        ) AS days_since_last_purchase

    FROM
        customer_orders
    GROUP BY
        1
)

-- 6. Join กับ Dimension เพื่อเอา Key และข้อมูลเสริม
SELECT
    -- Foreign Keys
    -- Key นี้จะตรงกับ dim_customer.customer_key
    {{ dbt_utils.generate_surrogate_key(['f.customer_unique_id']) }} AS customer_key, 
    TO_CHAR(f.first_purchase_date, 'YYYYMMDD')::INT AS first_order_date_key,
    TO_CHAR(f.last_purchase_date, 'YYYYMMDD')::INT AS last_order_date_key,

    -- Denormalized Dimensions (จาก DimCustomer)
    dc.customer_zip_code_prefix,
    dc.customer_city,
    dc.customer_state,
    
    -- Measures
    f.total_orders,
    COALESCE(f.total_spent, 0) AS total_spent,
    COALESCE(f.total_items_purchased, 0) AS total_items_purchased,
    f.avg_review_score,
    f.days_since_last_purchase
    
FROM
    final f
LEFT JOIN
    -- ใช้ ref ไปยัง dim_customer ที่มีอยู่
    {{ ref('dim_customer') }} dc
    ON dc.customer_key = {{ dbt_utils.generate_surrogate_key(['f.customer_unique_id']) }}

