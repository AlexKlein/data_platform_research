CREATE SCHEMA IF NOT EXISTS core_dwh;

CREATE TABLE IF NOT EXISTS core_dwh.dim_product (
    asin        VARCHAR,
    description TEXT,
    title       VARCHAR,
    price       NUMERIC,
    imUrl       VARCHAR
);

CREATE TABLE IF NOT EXISTS core_dwh.dim_category (
    category_id   SERIAL,
    category_name VARCHAR
);

CREATE TABLE IF NOT EXISTS core_dwh.fact_sales_rank (
    product_asin VARCHAR,
    category_id  INT,
    rank         INT
);

CREATE TABLE IF NOT EXISTS core_dwh.fact_product_category (
    product_asin VARCHAR,
    category_id  INT
);

CREATE TABLE IF NOT EXISTS core_dwh.fact_product_related_product (
    product_asin VARCHAR,
    related_asin VARCHAR
);

CREATE TABLE IF NOT EXISTS core_dwh.fact_rating (
    user_id         VARCHAR(128),
    item            VARCHAR(32),
    rating          NUMERIC,
    event_timestamp TIMESTAMP
);
