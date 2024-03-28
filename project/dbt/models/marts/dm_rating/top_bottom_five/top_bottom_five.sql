{{
    config(
        materialized='table',
        alias='top_bottom_five',
        pre_hook=[
            "TRUNCATE TABLE {{ this }}"
        ]
    )
}}

SELECT
    CONCAT('Top ', t.rn) AS label,
    COALESCE(
        NULLIF(m.title, ''),
        t.item)          AS movie,
    t.year_month         AS year_month

FROM
    {{ ref('temp_top_bottom_five_top') }} AS t
    LEFT  JOIN {{ ref('dim_product') }}   AS m ON t.item = m.asin

UNION ALL

SELECT
    CONCAT('Bottom ', b.rn) AS label,
    COALESCE(
        NULLIF(m.title, ''),
        b.item)             AS movie,
    b.year_month            AS year_month

FROM
    {{ ref('temp_top_bottom_five_bottom') }} AS b
    LEFT  JOIN {{ ref('dim_product') }}      AS m ON b.item = m.asin
