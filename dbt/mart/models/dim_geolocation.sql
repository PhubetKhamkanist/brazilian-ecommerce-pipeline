{{ config(
    materialized='table',
    schema='dims'
) }}

-- สร้างตารางมิติ Geolocation (Grain: 1 แถว ต่อ 1 zip_code_prefix)
-- ข้อมูล Geolocation จาก core layer อาจมี zip_code_prefix เดียวกัน
-- แต่มี lat/lng หลายค่า เราจึงทำการ Group By 
-- เพื่อหาค่าเฉลี่ยของ lat/lng ให้เหลือ 1 แถวต่อ zip_code

SELECT
    -- 1. สร้าง Primary Key (Surrogate Key)
    {{ dbt_utils.generate_surrogate_key(['geolocation_zip_code_prefix']) }} AS geolocation_key,
    
    -- 2. Natural Key
    geolocation_zip_code_prefix,
    
    -- 3. Dimensions (เลือกค่า city/state ที่พบบ่อยที่สุด หรือค่าแรก)
    -- ใช้ MAX() หรือ MIN() เพื่อให้ได้ค่าเดียวหลัง Group By
    MAX(geolocation_city) AS geolocation_city,
    MAX(geolocation_state) AS geolocation_state,
    
    -- 4. Measures (Aggregated)
    AVG(geolocation_lat) AS avg_latitude,
    AVG(geolocation_lng) AS avg_longitude
    
FROM
    {{ source('core', 'geolocation') }}
WHERE 
    geolocation_zip_code_prefix IS NOT NULL
GROUP BY
    geolocation_zip_code_prefix