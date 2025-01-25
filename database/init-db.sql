-- Create database
-- CREATE DATABASE ar_climate_data;
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

CREATE OR REPLACE
    FUNCTION get_climate_tiles(z integer, x integer, y integer, query_params json DEFAULT '{}'::json)
    RETURNS bytea AS $$
DECLARE
    mvt bytea;
    gwl_param numeric;
BEGIN
    -- Extract GWL from query params, default to 2.0
    gwl_param := COALESCE((query_params->>'gwl')::numeric, 2.0);
    
    SELECT INTO mvt ST_AsMVT(tile, 'climate', 4096, 'geom') FROM (
        SELECT 
            c.id,
            c.name,
            c.state_abbr,
            cv.pr_above_nonzero_99th,
            cv.prmax1day,
            cv.prmax5yr,
            cv.tavg,
            cv.tmax1day,
            cv.tmax_days_ge_100f,
            cv.tmax_days_ge_105f,
            cv.tmax_days_ge_95f,
            cv.tmean_jja,
            cv.tmin_days_ge_70f,
            cv.tmin_days_le_0f,
            cv.tmin_days_le_32f,
            cv.tmin_jja,
            cv.pr_annual,
            cv.pr_days_above_nonzero_99th,
            cv.gwl,
            ST_AsMVTGeom(
                c.geom,
                ST_TileEnvelope(z, x, y),
                4096,
                64,
                true
            ) as geom
        FROM counties c
        JOIN climate_variables cv ON c.id = cv.county_id
        WHERE c.geom && ST_TileEnvelope(z, x, y)
        AND cv.gwl = gwl_param
    ) as tile WHERE geom IS NOT NULL;

    RETURN mvt;
END
$$ LANGUAGE plpgsql IMMUTABLE STRICT PARALLEL SAFE;

-- Add TileJSON metadata
DO $do$ BEGIN
    EXECUTE 'COMMENT ON FUNCTION get_climate_tiles IS $tj$' || $$
    {
        "description": "Climate projection data by county",
        "vector_layers": [
            {
                "id": "climate",
                "fields": {
                    "name": "String",
                    "state_abbr": "String",
                    "pr_above_nonzero_99th": "Number",
                    "prmax1day": "Number",
                    "prmax5yr": "Number",
                    "tavg": "Number",
                    "tmax1day": "Number",
                    "tmax_days_ge_100f": "Number",
                    "tmax_days_ge_105f": "Number",
                    "tmax_days_ge_95f": "Number",
                    "tmean_jja": "Number",
                    "tmin_days_ge_70f": "Number",
                    "tmin_days_le_0f": "Number",
                    "tmin_days_le_32f": "Number",
                    "tmin_jja": "Number",
                    "pr_annual": "Number",
                    "pr_days_above_nonzero_99th": "Number",
                    "gwl": "Number"
                }
            }
        ],
        "minzoom": 0,
        "maxzoom": 22
    }
    $$::json || '$tj$';
END $do$;