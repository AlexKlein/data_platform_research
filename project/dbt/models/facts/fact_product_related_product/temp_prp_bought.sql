{{
    config(
        materialized='ephemeral',
        alias='temp_prp_bought'
    )
}}

SELECT
    bav.json_text::JSONb->>'asin'                             AS product_asin,
    JSONB_ARRAY_ELEMENTS_TEXT(
        bav.json_text::JSONB->'related'->'buy_after_viewing') AS related_asin,
    'buy_after_viewing'                                       AS label
FROM
    {{ ref('metadata') }} AS bav

WHERE
    bav.json_text::JSONB->'related'->'buy_after_viewing' IS NOT NULL
