{{ config(materialized='table') }}

WITH raw_detections AS (
    -- Updated to use the correct source
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
        message_date,  -- updated column name to match your stg/fct messages
        view_count 
    FROM {{ ref('fct_messages') }}
)

SELECT
    -- Unique ID for each detection
    md5(cast(m.message_id as text) || d.image_category) AS detection_pk,
    m.message_id,
    m.channel_key,
    m.message_date AS date_key,  -- keeping alias for consistency
    d.image_category,
    d.detected_objects,
    d.confidence_score,
    m.view_count
FROM messages m
INNER JOIN raw_detections d 
    ON m.message_id = d.message_id
