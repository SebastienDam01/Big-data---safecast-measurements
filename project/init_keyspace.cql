DROP KEYSPACE IF EXISTS damsebas_safecast;

-- Creating a keyspace
CREATE KEYSPACE "damsebas_safecast"
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2};

-- Using it
USE damsebas_safecast;

-- Table to hold all the measurements
CREATE TABLE measurements
(
    captured_time TIMEUUID,
    latitude FLOAT,
    longitude FLOAT,
    value FLOAT,
    unit VARCHAR,
    location_name VARCHAR,
    device_id INT,
    md5sum VARCHAR,
    height INT,
    surface INT,
    radiation INT,
    uploaded_time TIMEUUID,
    loader_id INT,

    PRIMARY KEY (md5sum)
);

-- Table to hold the areas most radioactive
CREATE TABLE area_most_radio
(
    latitude FLOAT,
    longitude FLOAT,
    value FLOAT,
    unit VARCHAR,
    md5sum VARCHAR,

    PRIMARY KEY (md5sum, value)
);
