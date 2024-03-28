CREATE SCHEMA IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.metadata (
    json_text  TEXT,
    created_at DATE
);

CREATE TABLE IF NOT EXISTS raw_data.ratings (
    user_id         VARCHAR(128),
    item            VARCHAR(32),
    rating          NUMERIC,
    event_timestamp NUMERIC,
    created_at      DATE
);
