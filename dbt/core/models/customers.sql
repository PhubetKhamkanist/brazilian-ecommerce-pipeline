{{ config(materialized='table') }}

SELECT
    MD5(customer_id::text) AS customer_pk, -- ✅ อัปเดต
    customer_id,
    customer_unique_id,
    CAST(customer_zip_code_prefix AS INTEGER) AS customer_zip_code_prefix,
    customer_city,
    customer_state
FROM {{ source('staging', 'olist_customers_dataset') }}