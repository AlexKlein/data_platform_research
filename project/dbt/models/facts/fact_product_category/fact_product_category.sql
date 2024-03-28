{{
    config(
        materialized='incremental',
        unique_key = [
            'product_asin',
            'category_id'
        ],
        alias='fact_product_category'
    )
}}

WITH product_categories AS (

    SELECT
        mt.json_text::JSONB->>'asin'                AS asin,
        JSONB_ARRAY_ELEMENTS(
            JSONB_ARRAY_ELEMENTS(
                mt.json_text::JSONB->'categories')) AS category_name
    FROM
        {{ ref('metadata') }} AS mt

)

SELECT
    DISTINCT
    pc.asin        AS product_asin,
    ci.category_id AS category_id

FROM
    product_categories                   AS pc
    INNER JOIN {{ ref('dim_category') }} AS ci ON ci.category_name = pc.category_name::text