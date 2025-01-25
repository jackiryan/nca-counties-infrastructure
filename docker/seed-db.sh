#!/bin/bash
set -e

until pg_isready; do
    echo "Waiting for PostgreSQL to start..."
    sleep 1
done

/usr/local/bin/seed_nca_atlas.py --files \
    /data/sources/NCA_Atlas_Figures_Beta_Counties_view_-3211749018570635702.geojson 1.0 \
    /data/sources/NCA_Atlas_GWL_2C_8110955878002816395.geojson 2.0 \
    /data/sources/NCA_Atlas_Global_Warming_Level_5_deg_F_-5719833167378735320.geojson 3.0 \
    /data/sources/NCA_Atlas_Global_Warming_Level_7_deg_F_6460790492253038117.geojson 4.0 \
    --dbname ar_climate_data --host localhost --user postgres --password ${POSTGRES_PASSWORD}