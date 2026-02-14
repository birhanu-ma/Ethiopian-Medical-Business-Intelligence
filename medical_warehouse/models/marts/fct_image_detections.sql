{{ config(materialized='table', schema='analytics') }}

WITH raw_detections AS (
    -- Use 'raw_data' if you followed Option 1 above
    SELECT 
        message_id,
        image_category,
        detected_objects,
        confidence_score
    FROM {{ source('raw_data', 'image_analysis') }}
),

messages AS (
    SELECT 
        message_id,
        channel_key,
        date_key,
        view_count 
    FROM {{ ref('fct_messages') }}
)

SELECT
    -- Simple unique ID
    md5(cast(m.message_id as text) || d.image_category) as detection_pk,
    m.message_id,
    m.channel_key,
    m.date_key,
    d.image_category,
    d.detected_objects,
    d.confidence_score,
    m.view_count
FROM messages m
INNER JOIN raw_detections d ON m.message_id = d.message_id