{{ config(materialized='table') }}

SELECT
    message_id,
    channel_key,
    message_date AS date_key,
    message_text,
    view_count,
    has_image,
    tone_label,
    content_category,
    final_category

FROM {{ ref('stg_telegram_messages') }}
