{{
    config(
        materialized='view',
        alias='ratings'
    )
}}

SELECT
    r.user_id         AS user_id,
    r.item            AS item,
    r.rating          AS rating,
    r.event_timestamp AS event_timestamp,
    r.created_at      AS created_at

FROM
    {{ source('raw_data', 'ratings') }} AS r
