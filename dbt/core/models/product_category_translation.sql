{{ config(materialized='table') }}

SELECT
    MD5(product_category_name::text) AS product_category_pk, -- ✅ อัปเดต
    product_category_name,
    product_category_name_english
FROM {{ source('staging', 'product_category_name_translation') }}