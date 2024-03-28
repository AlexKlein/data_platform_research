{{
    config(
        materialized='incremental',
        unique_key = 'category_name',
        alias='dim_category'
    )
}}

WITH flat_json AS (

    SELECT
        mt.json_text::JSONB AS p

    FROM
        {{ ref('metadata') }} AS mt

),

category_elements AS (

    SELECT
        DISTINCT
        JSONB_ARRAY_ELEMENTS_TEXT(
            JSONB_ARRAY_ELEMENTS(
                ft.p->'categories')) AS category_name

    FROM
        flat_json AS ft
)

SELECT
    DISTINCT
    ROW_NUMBER() OVER () AS category_id,
    ce.category_name     AS category_name

FROM
    category_elements AS ce
