{{ config(materialized='table') }}

SELECT
    MD5(seller_id::text) AS seller_pk, -- ✅ อัปเดต
    seller_id,
    CAST(seller_zip_code_prefix AS INTEGER) AS seller_zip_code_prefix,
    seller_city,
    seller_state
FROM {{ source('staging', 'olist_sellers_dataset') }}