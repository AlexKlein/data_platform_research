CREATE SCHEMA IF NOT EXISTS dm_rating;

CREATE TABLE IF NOT EXISTS dm_rating.top_bottom_five (
    label      VARCHAR(128),
    movie      VARCHAR(256),
    year_month VARCHAR(8)
);

CREATE TABLE IF NOT EXISTS dm_rating.top_five_increased_rating (
    label      VARCHAR(128),
    movie      VARCHAR(256),
    year_month VARCHAR(8)
);
