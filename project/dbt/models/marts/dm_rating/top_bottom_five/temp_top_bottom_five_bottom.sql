{{
    config(
        materialized='ephemeral',
        alias='temp_top_bottom_five_bottom'
    )
}}

WITH sorted AS (

    SELECT
        ROW_NUMBER() OVER
            (PARTITION BY b.year_month
                ORDER BY b.avg_rating) AS rn,
        b.year_month                   AS year_month,
        b.item                         AS item

    FROM
        {{ ref('temp_top_bottom_five_base') }} AS b

)

SELECT
    s.item       AS item,
    s.rn         AS rn,
    s.year_month AS year_month

FROM
    sorted AS s

WHERE
    s.rn <= 5
