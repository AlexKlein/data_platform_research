{{
    config(
        materialized='incremental',
        unique_key = [
            'product_asin',
            'category_id'
        ],
        alias='fact_sales_rank'
    )
}}

WITH ranked_products AS (

    SELECT
        mt.json_text::JSONB->>'asin'          AS asin,
        jsonb_each(
            mt.json_text::JSONB->'salesRank') AS sales_rank

    FROM
        {{ ref('metadata') }} AS mt

), parsed_ranked_products AS (

    SELECT
        rp.asin  AS asin,
        REGEXP_REPLACE(
            rp.sales_rank::TEXT,
            '[\(\)\"]',
            '',
            'g') AS cleaned_sales_rank

    FROM
        ranked_products AS rp

), split_ranked_products AS (

    SELECT
        asin                                     AS asin,
        SPLIT_PART(
            prp.cleaned_sales_rank, ',', 1)      AS category_name,
        SPLIT_PART(
            prp.cleaned_sales_rank, ',', 2)::INT AS rank
    FROM
        parsed_ranked_products AS prp
)

SELECT
    sr.asin        AS product_asin,
    ci.category_id AS category_id,
    sr.rank        AS rank

FROM
    split_ranked_products                AS sr
    INNER JOIN {{ ref('dim_category') }} AS ci ON ci.category_name = sr.category_name
