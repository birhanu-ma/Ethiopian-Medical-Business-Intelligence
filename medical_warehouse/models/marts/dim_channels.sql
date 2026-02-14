{{ config(materialized='table') }}

SELECT
    -- Use the column name that actually comes from your stg_telegram_messages
    channel_key,
    -- Alias it as channel_name so the table structure is easy to read
    channel_key AS channel_name,
    COUNT(*) AS total_messages_contributed
FROM {{ ref('stg_telegram_messages') }}
GROUP BY 1, 2