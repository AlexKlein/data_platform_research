{{
    config(
        materialized='incremental',
        unique_key = 'product_asin',
        alias='fact_product_related_product'
    )
}}

WITH product_related AS (

    SELECT
        av.product_asin AS product_asin,
        av.related_asin AS related_asin,
        av.label        AS label

    FROM
        {{ ref('temp_prp_viewed') }} AS av

    UNION

    SELECT
        b.product_asin AS product_asin,
        b.related_asin AS related_asin,
        b.label        AS label

    FROM
        {{ ref('temp_prp_bought') }} AS b

)

SELECT
    DISTINCT
    pr.product_asin AS product_asin,
    pr.related_asin AS related_asin,
    pr.label        AS label

FROM
    product_related AS pr
