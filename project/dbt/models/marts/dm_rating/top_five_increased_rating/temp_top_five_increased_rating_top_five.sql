{{
    config(
        materialized='ephemeral',
        alias='temp_top_five_increased_rating_top_five'
    )
}}

WITH rating AS (

    SELECT
        s.year_month                             AS year_month,
        s.item                                   AS item,
        s.avg_rating                             AS avg_rating,
        s.lag_avg_rating                         AS lag_avg_rating,
        s.increase_amount                        AS increase_amount,
        s.rn                                     AS rn,
        ROW_NUMBER() OVER
            (PARTITION BY s.year_month
                ORDER BY s.increase_amount DESC) AS top_rn

    FROM
        {{ ref('temp_top_five_increased_rating_shifts') }} AS s

    WHERE
        s.rn <= 1

)

SELECT
    r.item       AS item,
    r.year_month AS year_month,
    r.top_rn     AS top_rn

FROM
    rating AS r

WHERE
    r.top_rn <= 5
