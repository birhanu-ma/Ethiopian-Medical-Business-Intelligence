WITH raw_data AS (
    SELECT * FROM {{ source('raw_data', 'fct_messages') }}
)

SELECT
    CAST(message_id AS INT) AS message_id,
    -- Keep this named channel_key so the Mart can find it
    CAST(channel_key AS TEXT) AS channel_key,
    -- Keep the raw date/timestamp available for the Mart's TO_CHAR function
    -- We'll call it 'message_date' to be safe
    CAST(date_key AS TEXT) AS message_date, 
    TRIM(message_text) AS message_text,
    COALESCE(CAST(view_count AS INT), 0) AS view_count,
    -- Use the column from your scraper
    has_image 
FROM raw_data
WHERE message_text IS NOT NULL 
  AND message_text != ''