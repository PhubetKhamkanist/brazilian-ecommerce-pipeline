{{ config(materialized='table') }}

SELECT
    MD5(p.product_id::text) AS product_pk, -- ✅ อัปเดต
    p.product_id,
    
    -- This is the key change:
    -- We only keep the category name if it exists in the translation table.
    -- Otherwise, we set it to NULL.
    trans.product_category_name,

    CAST(p.product_name_lenght AS SMALLINT) AS product_name_length,
    CAST(p.product_description_lenght AS SMALLINT) AS product_description_length,
    CAST(p.product_photos_qty AS SMALLINT) AS product_photos_qty,
    CAST(p.product_weight_g AS INTEGER) AS product_weight_g,
    CAST(p.product_length_cm AS INTEGER) AS product_length_cm,
    CAST(p.product_height_cm AS INTEGER) AS product_height_cm,
    CAST(p.product_width_cm AS INTEGER) AS product_width_cm

FROM 
    {{ source('staging', 'olist_products_dataset') }} p
LEFT JOIN 
    -- We join to the translation table to check for valid categories
    {{ ref('product_category_translation') }} trans 
    ON p.product_category_name = trans.product_category_name