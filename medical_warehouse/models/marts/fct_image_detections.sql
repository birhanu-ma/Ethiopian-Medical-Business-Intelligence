{{ config(materialized='table') }}

WITH raw_detections AS (
    SELECT 
        message_id,
        image_category,
        detected_objects,
        confidence_score
    FROM {{ source('processed', 'image_analysis') }}
),

messages AS (
    SELECT 
        message_id,
        channel_key,
        -- CHANGE: Use date_key because that is the name in fct_messages
        date_key, 
        view_count 
    FROM {{ ref('fct_messages') }}
)

SELECT
    -- Unique ID for each detection
    md5(cast(m.message_id as text) || d.image_category) AS detection_pk,
    m.message_id,
    m.channel_key,
    m.date_key,
    d.image_category,
    d.detected_objects,
    d.confidence_score,
    m.view_count
FROM messages m
INNER JOIN raw_detections d 
    ON m.message_id = d.message_id