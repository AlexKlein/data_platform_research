CREATE SCHEMA IF NOT EXISTS staging;

CREATE OR REPLACE VIEW staging.metadata
AS
    SELECT
        m.json_text  AS json_text,
        m.created_at AS created_at

    FROM
        raw_data.metadata AS m
;

CREATE OR REPLACE VIEW staging.ratings
AS
    SELECT
        r.user_id         AS user_id,
        r.item            AS item,
        r.rating          AS rating,
        r.event_timestamp AS event_timestamp,
        r.created_at      AS created_at

    FROM
        raw_data.ratings AS r
;