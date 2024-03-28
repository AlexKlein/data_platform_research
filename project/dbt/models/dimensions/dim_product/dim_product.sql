{{
    config(
        materialized='incremental',
        unique_key = 'asin',
        alias='dim_product '
    )
}}

WITH flat_json AS (

    SELECT
        mt.json_text::JSON AS p

    FROM
        {{ ref('metadata') }} AS mt

)

SELECT
    ft.p->>'asin'             AS asin,
    ft.p->>'description'      AS description,
    ft.p->>'title'            AS title,
    (ft.p->>'price')::NUMERIC AS price,
    ft.p->>'imUrl'            AS imUrl

FROM
    flat_json AS ft
