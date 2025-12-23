{{ config(
    materialized='table',
    schema='dims'
) }}

-- สร้างตารางมิติสินค้า (Product Dimension) โดย Join ข้อมูลสินค้ากับตารางแปลชื่อหมวดหมู่
SELECT
    {{ dbt_utils.generate_surrogate_key(['p.product_id']) }} AS product_key,
    p.product_id,
    p.product_category_name,
    t.product_category_name_english
FROM
    {{ source('core', 'product') }} p
    LEFT JOIN {{ source('core', 'product_category_translation') }} t
    ON p.product_category_name = t.product_category_name
WHERE p.product_id IS NOT NULL