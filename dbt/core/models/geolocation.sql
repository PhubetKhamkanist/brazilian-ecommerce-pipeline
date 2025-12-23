{{ config(materialized='table') }}

SELECT
    CAST(geolocation_zip_code_prefix AS INTEGER) AS geolocation_zip_code_prefix,
    CAST(geolocation_lat AS DECIMAL(10, 8)) AS geolocation_lat,
    CAST(geolocation_lng AS DECIMAL(11, 8)) AS geolocation_lng,
    geolocation_city,
    geolocation_state
FROM {{ source('staging', 'olist_geolocation_dataset') }}