{{
    config(
        materialized='table',
        alias='top_five_increased_rating',
        pre_hook=[
            "TRUNCATE TABLE {{ this }}"
        ]
    )
}}

SELECT
    CONCAT(
        'Top ',
        t.top_rn) AS label,
    COALESCE(
        NULLIF(m.title, ''),
        t.item)   AS movie,
    t.year_month  AS year_month

FROM
    {{ ref('temp_top_five_increased_rating_top_five') }} AS t
    LEFT  JOIN {{ ref('dim_product') }}                  AS m ON t.item = m.asin
