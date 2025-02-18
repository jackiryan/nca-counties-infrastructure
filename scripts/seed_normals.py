#!/usr/bin/env python3
import argparse
import json
import psycopg2


def create_table(cur):
    # Create table if it doesn't exist.
    create_table_query = """
    CREATE TABLE IF NOT EXISTS climate_normals (
        county_fips VARCHAR(5) PRIMARY KEY,
        tavg NUMERIC,
        tmax_days_ge_100f NUMERIC,
        tmean_jja NUMERIC,
        tmin_days_ge_70f NUMERIC,
        tmin_days_le_0f NUMERIC,
        tmin_days_le_32f NUMERIC,
        tmin_jja NUMERIC,
        pr_annual NUMERIC
    );
    """
    cur.execute(create_table_query)


def insert_data(cur, record):
    # Insert record into the table.
    # This query uses the ON CONFLICT clause to perform an upsert (update if the county_fips exists).
    insert_query = """
    INSERT INTO climate_normals (
        county_fips, tavg, tmax_days_ge_100f, tmean_jja,
        tmin_days_ge_70f, tmin_days_le_0f, tmin_days_le_32f,
        tmin_jja, pr_annual
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (county_fips) DO UPDATE SET
        tavg = EXCLUDED.tavg,
        tmax_days_ge_100f = EXCLUDED.tmax_days_ge_100f,
        tmean_jja = EXCLUDED.tmean_jja,
        tmin_days_ge_70f = EXCLUDED.tmin_days_ge_70f,
        tmin_days_le_0f = EXCLUDED.tmin_days_le_0f,
        tmin_days_le_32f = EXCLUDED.tmin_days_le_32f,
        tmin_jja = EXCLUDED.tmin_jja,
        pr_annual = EXCLUDED.pr_annual;
    """
    cur.execute(
        insert_query,
        (
            record["county_id"],  # gets converted to county_fips for specificity
            record["tavg"],
            record["tmax_days_ge_100f"],
            record["tmean_jja"],
            record["tmin_days_ge_70f"],
            record["tmin_days_le_0f"],
            record["tmin_days_le_32f"],
            record["tmin_jja"],
            record["pr_annual"],
        ),
    )


def main():
    parser = argparse.ArgumentParser(
        description="Load NCA Atlas climate data into PostgreSQL"
    )
    parser.add_argument(
        "file",
        metavar=("NORMALS FILE"),
        help="JSON file containing 1991-2020 county level climate normals.",
    )
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--dbname", default="climate_data", help="Database name")
    parser.add_argument("--user", default="postgres", help="Database user")
    parser.add_argument("--password", default="postgres", help="Database password")

    args = parser.parse_args()

    conn_params = {
        "dbname": args.dbname,
        "user": args.user,
        "password": args.password,
        "host": args.host,
        "port": 5432,
    }

    try:
        # Connect to the PostgreSQL database.
        conn = psycopg2.connect(**conn_params)
        cur = conn.cursor()

        create_table(cur)
        conn.commit()

        with open(args.file, "r") as f:
            data = json.load(f)

        # Insert each record.
        for record in data:
            insert_data(cur, record)

        # Commit changes and close connection.
        conn.commit()
        cur.close()
        conn.close()
        print("Data inserted successfully.")

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
