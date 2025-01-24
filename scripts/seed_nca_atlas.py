#!/usr/bin/env python3
import json
import psycopg2
from psycopg2.extras import execute_values


def load_geojson_data(connection, gwl_files):
    cursor = connection.cursor()

    # Load the first file to get county geometries and metadata
    with open(gwl_files[0], "r") as f:
        data = json.load(f)

    # Insert counties first
    counties_data = []
    for feature in data["features"]:
        counties_data.append(
            (
                feature["properties"]["NAME"],
                feature["properties"]["STATE_NAME"],
                feature["properties"]["STATE_ABBR"],
                feature["properties"]["FIPS"],
                json.dumps(feature["geometry"]),
            )
        )

    cursor.execute(
        """
        CREATE TEMP TABLE temp_counties (
            name VARCHAR(255),
            state_name VARCHAR(255),
            state_abbr CHAR(2),
            fips VARCHAR(5),
            geom_json JSON
        )
    """
    )

    execute_values(
        cursor,
        """
        INSERT INTO temp_counties (name, state_name, state_abbr, fips, geom_json)
        VALUES %s
    """,
        counties_data,
    )

    # Insert into final counties table with proper geometry
    cursor.execute(
        """
        INSERT INTO counties (name, state_name, state_abbr, fips, geom)
        SELECT 
            name, 
            state_name, 
            state_abbr, 
            fips,
            ST_SetSRID(ST_GeomFromGeoJSON(geom_json::text), 4326)
        FROM temp_counties
        RETURNING id, fips
    """
    )

    # Get county_id to fips mapping
    fips_to_id = dict(cursor.fetchall())

    # Now load climate variables for each GWL
    for gwl_file, gwl_value in gwl_files.items():
        with open(gwl_file, "r") as f:
            data = json.load(f)

        climate_data = []
        for feature in data["features"]:
            props = feature["properties"]
            county_id = fips_to_id[props["FIPS"]]

            climate_data.append(
                (
                    county_id,
                    gwl_value,
                    props.get("pr_above_nonzero_99th_GWL2"),
                    props.get("prmax1day_GWL2"),
                    props.get("prmax5yr_GWL2"),
                    props.get("tavg_GWL2"),
                    props.get("tmax1day_GWL2"),
                    props.get("tmax_days_ge_100f_GWL2"),
                    props.get("tmax_days_ge_105f_GWL2"),
                    props.get("tmax_days_ge_95f_GWL2"),
                    props.get("tmean_jja_GWL2"),
                    props.get("tmin_days_ge_70f_GWL2"),
                    props.get("tmin_days_le_0f_GWL2"),
                    props.get("tmin_days_le_32f_GWL2"),
                    props.get("tmin_jja_GWL2"),
                    props.get("pr_annual_GWL2"),
                    props.get("pr_days_above_nonzero_99th_GWL2"),
                )
            )

        execute_values(
            cursor,
            """
            INSERT INTO climate_variables (
                county_id, gwl,
                pr_above_nonzero_99th, prmax1day, prmax5yr,
                tavg, tmax1day,
                tmax_days_ge_100f, tmax_days_ge_105f, tmax_days_ge_95f,
                tmean_jja,
                tmin_days_ge_70f, tmin_days_le_0f, tmin_days_le_32f,
                tmin_jja, pr_annual, pr_days_above_nonzero_99th
            )
            VALUES %s
        """,
            climate_data,
        )

    connection.commit()
    cursor.close()


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(
            "Usage: python3 load-data.py <gwl_file1> <gwl_value1> [<gwl_file2> <gwl_value2> ...]"
        )
        sys.exit(1)

    # Create dictionary of files and their GWL values
    gwl_files = {}
    for i in range(1, len(sys.argv), 2):
        gwl_files[sys.argv[i]] = float(sys.argv[i + 1])

    with psycopg2.connect(
        dbname="climate_data", user="postgres", password="postgres", host="localhost"
    ) as conn:
        load_geojson_data(conn, gwl_files)
