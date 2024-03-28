{{
    config(
        materialized='incremental',
        unique_key = [
            'item',
            'user_id',
            'event_timestamp'
        ],
        alias='rating'
    )
}}

SELECT
    DISTINCT
    rt.user_id              AS user_id,
    rt.item                 AS item,
    rt.rating               AS rating,
    TIMESTAMP 'epoch' +
        rt.event_timestamp *
        interval '1 second' AS event_timestamp

FROM
    {{ ref('ratings') }} AS rt
