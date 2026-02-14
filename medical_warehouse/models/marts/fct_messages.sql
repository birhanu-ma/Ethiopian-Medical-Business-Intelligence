{{ config(materialized='table') }}

SELECT
    m.message_id,
    m.channel_key, -- Now this matches the staging output
    -- Use the message_date we defined in staging
    -- Since your staging already has a date_key, we can also just select it directly:
    m.message_date AS date_key,
    m.message_text,
    m.view_count,
    m.has_image
FROM {{ ref('stg_telegram_messages') }} m