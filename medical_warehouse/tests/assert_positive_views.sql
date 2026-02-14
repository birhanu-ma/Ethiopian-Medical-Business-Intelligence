SELECT
    m.message_id,
    MD5(m.channel_name) AS channel_key,
    m.message_text,
    m.view_count,
    m.has_image
FROM {{ ref('stg_telegram_messages') }} m