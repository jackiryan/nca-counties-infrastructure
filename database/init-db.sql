-- Create database
CREATE DATABASE ar_climate_data;
\c ar_climate_data

-- Enable PostGIS
CREATE EXTENSION postgis;

-- Create counties table
CREATE TABLE counties (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255),
    state_name VARCHAR(255),
    state_abbr CHAR(2),
    fips VARCHAR(5),
    geom GEOMETRY(MultiPolygon, 4326)
);

-- Create climate variables table
CREATE TABLE climate_variables (
    id SERIAL PRIMARY KEY,
    county_id INTEGER REFERENCES counties(id),
    gwl NUMERIC(2,1),
    pr_above_nonzero_99th FLOAT,
    prmax1day FLOAT,
    prmax5yr FLOAT,
    tavg FLOAT,
    tmax1day FLOAT,
    tmax_days_ge_100f FLOAT,
    tmax_days_ge_105f FLOAT,
    tmax_days_ge_95f FLOAT,
    tmean_jja FLOAT,
    tmin_days_ge_70f FLOAT,
    tmin_days_le_0f FLOAT,
    tmin_days_le_32f FLOAT,
    tmin_jja FLOAT,
    pr_annual FLOAT,
    pr_days_above_nonzero_99th FLOAT,
    UNIQUE (county_id, gwl)
);

-- Create spatial index
CREATE INDEX counties_geom_idx ON counties USING GIST (geom);

-- Create index on commonly queried fields
CREATE INDEX climate_vars_county_gwl_idx ON climate_variables (county_id, gwl);