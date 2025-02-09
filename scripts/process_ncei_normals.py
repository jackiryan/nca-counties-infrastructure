#!/usr/bin/env python3
import geopandas as gpd
import rasterio
from rasterio.warp import transform_bounds
from rasterstats import zonal_stats
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
from sqlalchemy import create_engine
import numpy as np
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_county_geometries(connection_string: str) -> gpd.GeoDataFrame:
    """
    Retrieve county geometries from the PostgreSQL database.

    Args:
        connection_string: PostgreSQL connection string

    Returns:
        GeoDataFrame containing county geometries and metadata
    """
    try:
        engine = create_engine(connection_string)
        query = """
            SELECT id, name, state_name, state_abbr, fips, geom 
            FROM counties;
        """
        gdf = gpd.read_postgis(query, engine, geom_col="geom")
        logger.info(f"Retrieved {len(gdf)} county geometries from database")
        return gdf
    except Exception as e:
        logger.error(f"Error retrieving county geometries: {e}")
        raise


def process_raster(
    raster_path: str, counties: gpd.GeoDataFrame, var_name: str
) -> pd.DataFrame:
    """
    Calculate zonal statistics for each county from the input raster.

    Args:
        raster_path: Path to the input raster file
        counties: GeoDataFrame containing county geometries
        var_name: Name of the climate variable being processed

    Returns:
        DataFrame containing county IDs and their average values
    """
    try:
        with rasterio.open(raster_path) as src:
            # Ensure geometries are in same CRS as raster
            counties_proj = counties.to_crs(src.crs)

            # Calculate zonal statistics
            stats = zonal_stats(
                counties_proj.geometry,
                raster_path,
                stats=["mean"],
                nodata=src.nodata,
                all_touched=False,  # Only include pixels with centroids within polygon
            )

            # Create results dataframe
            results = pd.DataFrame(
                {"county_id": counties.id, var_name: [s["mean"] for s in stats]}
            )

            logger.info(f"Processed {var_name} raster for {len(results)} counties")
            return results

    except Exception as e:
        logger.error(f"Error processing raster {raster_path}: {e}")
        raise


def save_results(connection_string: str, results: pd.DataFrame):
    """
    Save computed climate normals to the database.

    Args:
        connection_string: PostgreSQL connection string
        results: DataFrame containing computed climate normals
    """
    try:
        # Create climate_normals table if it doesn't exist
        with psycopg2.connect(connection_string) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS climate_normals (
                        id SERIAL PRIMARY KEY,
                        county_id INTEGER REFERENCES counties(id),
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
                        pr_days_above_nonzero_99th FLOAT
                    );
                """
                )

                # Insert results
                columns = results.columns.tolist()
                values = [tuple(x) for x in results.values]

                insert_query = f"""
                    INSERT INTO climate_normals ({', '.join(columns)})
                    VALUES %s
                    ON CONFLICT (county_id) DO UPDATE
                    SET {', '.join(f"{col} = EXCLUDED.{col}" for col in columns[1:])};
                """

                execute_values(cur, insert_query, values)

        logger.info(f"Successfully saved results to database")

    except Exception as e:
        logger.error(f"Error saving results to database: {e}")
        raise


def main():
    """
    Main function to process climate normals for all variables.
    """
    # Configuration
    connection_string = (
        "postgresql://postgres:${POSTGRES_PASSWORD}@localhost/ar_climate_data"
    )
    raster_dir = "data/outputs/"

    # Map of climate variables to their corresponding raster files
    raster_files = {
        "pr_above_nonzero_99th": "pr_above_nonzero_99th.tif",
        "prmax1day": "prmax1day.tif",
        "tavg": "tavg.tif",
        # Add other variables as needed
    }

    try:
        # Get county geometries
        counties = get_county_geometries(connection_string)

        # Process each raster and collect results
        all_results = []
        for var_name, raster_file in raster_files.items():
            raster_path = os.path.join(raster_dir, raster_file)
            if not os.path.exists(raster_path):
                logger.warning(f"Raster file not found: {raster_path}")
                continue

            results = process_raster(raster_path, counties, var_name)
            all_results.append(results)

        # Merge all results
        final_results = all_results[0]
        for df in all_results[1:]:
            final_results = final_results.merge(df, on="county_id")

        # Save to database
        save_results(connection_string, final_results)

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise


if __name__ == "__main__":
    main()
