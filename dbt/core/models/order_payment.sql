{{ config(materialized='table') }}

SELECT
    MD5(order_id::text || payment_sequential::text) AS order_payment_pk, -- ✅ อัปเดต
    
    order_id,
    CAST(payment_sequential AS SMALLINT) AS payment_sequential,
    payment_type,
    CAST(payment_installments AS SMALLINT) AS payment_installments,
    CAST(payment_value AS DECIMAL(10, 2)) AS payment_value
FROM {{ source('staging', 'olist_order_payments_dataset') }}