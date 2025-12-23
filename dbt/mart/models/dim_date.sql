{{ config(
    materialized='table',
    schema='dims'
) }}

-- สร้างตารางมิติเวลา (Date Dimension) จากข้อมูลวันที่มีอยู่จริงในตาราง Orders
SELECT DISTINCT
    -- Surrogate key (e.g., 20171002)
    TO_CHAR(CAST(order_purchase_timestamp AS DATE), 'YYYYMMDD')::INT AS date_key,
    CAST(order_purchase_timestamp AS DATE) AS date_actual,
    EXTRACT(DAY FROM CAST(order_purchase_timestamp AS DATE)) AS day,
    EXTRACT(MONTH FROM CAST(order_purchase_timestamp AS DATE)) AS month,
    EXTRACT(YEAR FROM CAST(order_purchase_timestamp AS DATE)) AS year,
    TO_CHAR(CAST(order_purchase_timestamp AS DATE), 'Day') AS day_name,
    TO_CHAR(CAST(order_purchase_timestamp AS DATE), 'Month') AS month_name,
    EXTRACT(QUARTER FROM CAST(order_purchase_timestamp AS DATE)) AS quarter,
    EXTRACT(WEEK FROM CAST(order_purchase_timestamp AS DATE)) AS week_of_year
FROM
    {{ source('core', 'orders') }}