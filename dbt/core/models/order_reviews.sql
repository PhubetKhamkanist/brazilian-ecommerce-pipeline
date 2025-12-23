{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        *,
        -- สร้างลำดับสำหรับแต่ละ review_id โดยเรียงจากวันที่ตอบล่าสุด
        ROW_NUMBER() OVER(
            PARTITION BY review_id 
            ORDER BY review_answer_timestamp DESC
        ) as review_rank
    FROM
        {{ source('staging', 'olist_order_reviews_dataset') }}
),

final_reviews AS (
    -- เลือกมาเฉพาะแถวที่เป็นลำดับที่ 1 (แถวที่ไม่ซ้ำ)
    SELECT
        review_id,
        order_id,
        CAST(review_score AS SMALLINT) AS review_score,
        review_comment_title,
        review_comment_message,
        CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
        CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
    FROM
        source_data
    WHERE
        review_rank = 1
)

-- เพิ่ม Surrogate Key
SELECT 
    MD5(review_id::text) AS review_pk, -- ✅ อัปเดต
    *
FROM 
    final_reviews