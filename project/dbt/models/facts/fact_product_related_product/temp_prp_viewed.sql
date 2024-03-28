{{
    config(
        materialized='ephemeral',
        alias='temp_prp_viewed'
    )
}}

SELECT
    av.json_text::JSONB->>'asin'                       AS product_asin,
    JSONB_ARRAY_ELEMENTS_TEXT(
        av.json_text::JSONB->'related'->'also_viewed') AS related_asin,
    'also_viewed'                                      AS label

FROM
    {{ ref('metadata') }} AS av

WHERE
    av.json_text::JSONB->'related'->'also_viewed' IS NOT NULL
