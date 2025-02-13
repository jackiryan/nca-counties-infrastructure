#!/usr/bin/env python3
import geopandas as gpd
import json
import rasterio
from rasterio.warp import transform_bounds
from rasterstats import zonal_stats
import pandas as pd
import os
from shapely.geometry import shape
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_county_geometries(gwl_file: str) -> gpd.GeoDataFrame:
    """
    Retrieve county geometries from a GWL JSON file.

    Args:
        gwl_file: json file

    Returns:
        GeoDataFrame containing county geometries and metadata
    """
    # Load the first file to get county geometries and metadata
    with open(gwl_file, "r") as f:
        data = json.load(f)

    # Insert counties first
    counties_data = [
        (
            feature["properties"]["NAME"],
            feature["properties"]["STATE_NAME"],
            feature["properties"]["STATE_ABBR"],
            feature["properties"]["FIPS"],
            json.dumps(feature["geometry"]),
        )
        for feature in data["features"]
    ]
    cols = ["NAME", "STATE_NAME", "STATE_ABBR", "FIPS", "geometry_json"]
    df = pd.DataFrame(counties_data, columns=cols)

    # Step 2: Parse geometry from the JSON string
    df["geometry"] = df["geometry_json"].apply(lambda g: shape(json.loads(g)))

    # Step 3: Convert to a GeoDataFrame in EPSG:4326
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")

    # Optionally, drop the original geometry_json column
    gdf.drop(columns="geometry_json", inplace=True)
    return gdf


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
                {"county_id": counties.FIPS, var_name: [s["mean"] for s in stats]}
            )

            logger.info(f"Processed {var_name} raster for {len(results)} counties")
            return results

    except Exception as e:
        logger.error(f"Error processing raster {raster_path}: {e}")
        raise


def main():
    """
    Main function to process climate normals for all variables.
    """
    # Configuration
    gwl_file = (
        "data/sources/NCA_Atlas_Figures_Beta_Counties_view_-3211749018570635702.geojson"
    )
    raster_dir = "data/outputs/"

    # Map of climate variables to their corresponding raster files
    raster_files = {
        "pr_annual": "annual_prcp_grid_10km.tif",
        "tavg": "annual_temp_grid_10km.tif",
        "tmean_jja": "tmean_jja_grid_10km.tif",
        "tmin_days_ge_70f": "avgnds_lt70f_grid_10km.tif",
        "tmin_days_le_0f": "avgnds_lt0f_grid_10km.tif",
        "tmin_days_le_32f": "avgnds_lt32f_grid_10km.tif",
        "tmin_jja": "jja_tmin_grid_10km.tif",
    }

    try:
        # Get county geometries
        counties = get_county_geometries(gwl_file)

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
        raw_results = all_results[0]
        for df in all_results[1:]:
            raw_results = raw_results.merge(df, on="county_id")

        counties_gdf = counties.merge(raw_results, left_on="FIPS", right_on="county_id")

        if "tmin_days_le_0f" in counties_gdf.columns:
            counties_gdf.loc[counties_gdf["tmin_days_le_0f"] < 0, "tmin_days_le_0f"] = 0
        if "tmin_days_le_32f" in counties_gdf.columns:
            counties_gdf.loc[
                counties_gdf["tmin_days_le_32f"] < 0, "tmin_days_le_32f"
            ] = 0

        # Invert the values in tmin_days_ge_70f: replace with 365.25 - current_value
        if "tmin_days_ge_70f" in counties_gdf.columns:
            counties_gdf["tmin_days_ge_70f"] = 365.25 - counties_gdf["tmin_days_ge_70f"]

        output_geojson = "final_county_normals.geojson"
        counties_gdf.to_file(output_geojson, driver="GeoJSON")
        logger.info(f"Exported final results to {output_geojson}")

    except Exception as e:
        logger.error(f"Error in main function: {e}")
        raise


if __name__ == "__main__":
    main()
