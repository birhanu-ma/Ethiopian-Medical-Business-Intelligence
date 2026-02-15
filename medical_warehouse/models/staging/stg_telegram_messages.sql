{{ config(materialized='view') }}

WITH raw_data AS (

    SELECT * 
    FROM {{ source('raw', 'telegram_messages') }}  -- updated to match sources.yml

)

SELECT
    CAST(message_id AS INT) AS message_id,
    CAST(channel_key AS TEXT) AS channel_key,
    CAST(message_date AS DATE) AS message_date,  -- renamed to match your enriched Python upload
    TRIM(message_text) AS message_text,
    has_image,
    COALESCE(CAST(view_count AS INT), 0) AS view_count,
    CAST(tone_label AS TEXT) AS tone_label,
    CAST(content_category AS TEXT) AS content_category,
    CAST(final_category AS TEXT) AS final_category

FROM raw_data
WHERE message_text IS NOT NULL
  AND message_text != ''
