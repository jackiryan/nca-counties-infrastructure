#!/usr/bin/env python3
"""
MIT License

Copyright (c) 2025 Jacqueline Ryan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
import argparse
import json
from typing import Dict, List, Tuple
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.extensions import connection as PostgresConnection


def load_geojson_data(
    connection: PostgresConnection, gwl_files: Dict[str, float]
) -> None:
    cursor = connection.cursor()

    # Load the first file to get county geometries and metadata
    with open(next(iter(gwl_files)), "r") as f:
        data = json.load(f)

    # Insert counties first
    counties_data: List[Tuple[str, str, str, str, str]] = [
        (
            feature["properties"]["NAME"],
            feature["properties"]["STATE_NAME"],
            feature["properties"]["STATE_ABBR"],
            feature["properties"]["FIPS"],
            json.dumps(feature["geometry"]),
        )
        for feature in data["features"]
    ]

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

    # Get FIPS to county_id mapping
    fips_to_id = {fips: id for id, fips in cursor.fetchall()}

    # Now load climate variables for each GWL
    for gwl_file, gwl_value in gwl_files.items():
        with open(gwl_file, "r") as f:
            data = json.load(f)

        climate_data: List[Tuple] = [
            (
                fips_to_id[props["FIPS"]],
                gwl_value,
                props.get(f"pr_above_nonzero_99th_GWL{int(gwl_value)}"),
                props.get(f"prmax1day_GWL{int(gwl_value)}"),
                props.get(f"prmax5yr_GWL{int(gwl_value)}"),
                props.get(f"tavg_GWL{int(gwl_value)}"),
                props.get(f"tmax1day_GWL{int(gwl_value)}"),
                props.get(f"tmax_days_ge_100f_GWL{int(gwl_value)}"),
                props.get(f"tmax_days_ge_105f_GWL{int(gwl_value)}"),
                props.get(f"tmax_days_ge_95f_GWL{int(gwl_value)}"),
                props.get(f"tmean_jja_GWL{int(gwl_value)}"),
                props.get(f"tmin_days_ge_70f_GWL{int(gwl_value)}"),
                props.get(f"tmin_days_le_0f_GWL{int(gwl_value)}"),
                props.get(f"tmin_days_le_32f_GWL{int(gwl_value)}"),
                props.get(f"tmin_jja_GWL{int(gwl_value)}"),
                props.get(f"pr_annual_GWL{int(gwl_value)}"),
                props.get(f"pr_days_above_nonzero_99th_GWL{int(gwl_value)}"),
            )
            for feature in data["features"]
            if (props := feature["properties"])
        ]

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


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Load NCA Atlas climate data into PostgreSQL"
    )
    parser.add_argument(
        "--files",
        nargs="+",
        metavar=("FILE GWL"),
        help="GeoJSON files and their corresponding GWL values (e.g., file1.json 1.5 file2.json 2.0)",
    )
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--dbname", default="climate_data", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="postgres", help="Database password")

    args = parser.parse_args()

    if not args.files or len(args.files) % 2 != 0:
        parser.error(
            "Must provide file/GWL pairs (e.g., file1.geojson 1.5 file2.geojson 2.0)"
        )

    # Create dictionary of files and their GWL values
    gwl_files = {
        args.files[i]: float(args.files[i + 1]) for i in range(0, len(args.files), 2)
    }

    with psycopg2.connect(
        dbname=args.dbname,
        user=args.user,
        password=args.password,
        host=args.host,
    ) as conn:
        load_geojson_data(conn, gwl_files)


if __name__ == "__main__":
    main()
