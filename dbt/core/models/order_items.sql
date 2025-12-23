{{ config(materialized='table') }}

SELECT
    MD5(order_id::text || order_item_id::text) AS order_item_pk, -- ✅ อัปเดต
    
    order_id,
    CAST(order_item_id AS SMALLINT) AS order_item_id,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    CAST(price AS DECIMAL(10, 2)) AS price,
    CAST(freight_value AS DECIMAL(10, 2)) AS freight_value
FROM {{ source('staging', 'olist_order_items_dataset') }}