{{ config(materialized='table') }}

SELECT
    message_id,
    channel_key,
    message_date AS date_key,
    message_text,
    view_count,
    has_image
FROM {{ ref('stg_telegram_messages') }}