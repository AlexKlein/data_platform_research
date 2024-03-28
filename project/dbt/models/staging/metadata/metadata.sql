{{
    config(
        materialized='view',
        alias='metadata'
    )
}}

SELECT
    m.json_text  AS json_text,
    m.created_at AS created_at

FROM
    {{ source('raw_data', 'metadata') }} AS m
