{{
    config(
        materialized='ephemeral',
        alias='temp_top_five_increased_rating_shifts'
    )
}}

WITH lag_added AS (

    SELECT
        b.year_month                                    AS year_month,
        b.item                                          AS item,
        b.avg_rating                                    AS avg_rating,
        LAG(b.avg_rating) OVER
            (PARTITION BY b.item ORDER BY b.year_month) AS lag_avg_rating

    FROM
        {{ ref('temp_top_five_increased_rating_base') }} AS b

), clean_set AS (

    SELECT
        l.year_month         AS year_month,
        l.item               AS item,
        l.avg_rating         AS avg_rating,
        l.lag_avg_rating     AS lag_avg_rating,
        l.avg_rating -
            l.lag_avg_rating AS increase_amount

    FROM
        lag_added AS l

    WHERE
        l.lag_avg_rating IS NOT NULL
        AND l.lag_avg_rating < l.avg_rating

)

SELECT
    cs.year_month                  AS year_month,
    cs.item                        AS item,
    cs.avg_rating                  AS avg_rating,
    cs.lag_avg_rating              AS lag_avg_rating,
    cs.increase_amount             AS increase_amount,
    ROW_NUMBER() OVER
        (PARTITION BY cs.item
            ORDER BY
                cs.increase_amount DESC,
                cs.year_month ASC) AS rn

FROM
    clean_set AS cs
