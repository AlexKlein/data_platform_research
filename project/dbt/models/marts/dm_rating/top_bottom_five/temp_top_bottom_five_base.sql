{{
    config(
        materialized='ephemeral',
        alias='temp_top_bottom_five_base'
    )
}}

SELECT
    AVG(r.rating)                              AS avg_rating,
    CONCAT(
        EXTRACT(YEAR FROM r.event_timestamp),
        '.',
        EXTRACT(MONTH FROM r.event_timestamp)) AS year_month,
    r.item                                     AS item

FROM
    {{ ref('fact_rating') }} AS r

GROUP BY
    CONCAT(
        EXTRACT(YEAR FROM r.event_timestamp),
        '.',
        EXTRACT(MONTH FROM r.event_timestamp)),
    r.item
