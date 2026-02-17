{{ config(materialized='view') }}

WITH raw_data AS (
    SELECT * FROM {{ source('raw', 'telegram_messages') }}
)

SELECT
    CAST(message_id AS INT) AS message_id,
    -- Mapping "channel_name" from JSON to "channel_key"
    CAST(channel_name AS TEXT) AS channel_key, 
    -- Mapping the timestamp string to a DATE type
    CAST(message_date AS DATE) AS message_date,
    TRIM(message_text) AS message_text,
    -- Mapping "has_media" from JSON to "has_image"
    COALESCE(has_media, FALSE) AS has_image,
    -- Mapping "views" from JSON to "view_count"
    COALESCE(CAST(views AS INT), 0) AS view_count
FROM raw_data
WHERE message_text IS NOT NULL 
  AND message_text != ''