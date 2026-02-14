{{ config(materialized='table') }}

SELECT
    CAST(TO_CHAR(d, 'YYYYMMDD') AS INT) AS date_key,
    d::date AS date_actual,
    EXTRACT(YEAR FROM d) AS year,
    TO_CHAR(d, 'Month') AS month_name,
    TO_CHAR(d, 'Day') AS day_name,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series('2023-01-01'::timestamp, '2025-12-31'::timestamp, '1 day'::interval) d