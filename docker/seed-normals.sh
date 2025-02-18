#!/bin/bash
set -e

until pg_isready; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

/usr/local/bin/seed_normals.py /data/outputs/us_climate_normals_1991-2020.json
    --dbname ar_climate_data --host localhost --user postgres --password ${POSTGRES_PASSWORD}