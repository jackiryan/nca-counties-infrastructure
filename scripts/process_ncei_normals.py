#!/usr/bin/env python3
from functools import reduce
import geopandas as gpd
import json
import rasterio
from rasterio.warp import transform_bounds
from rasterstats import zonal_stats
import pandas as pd
import os
from shapely.geometry import shape


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
                all_touched=True,  # Only include pixels with centroids within polygon
            )

            # Create results dataframe
            results = pd.DataFrame(
                {"county_id": counties.FIPS, var_name: [s["mean"] for s in stats]}
            )

            print(f"Processed {var_name} raster for {len(results)} counties")
            return results

    except Exception as e:
        print(f"Error processing raster {raster_path}: {e}")
        raise


def main() -> None:
    """
    Main function to process climate normals for all variables.
    """
    # Configuration
    gwl_file = (
        "data/sources/NCA_Atlas_Figures_Beta_Counties_view_-3211749018570635702.geojson"
    )
    raster_dir = "data/outputs/"

    # Map of climate variables to their corresponding raster files
    variables = [
        "tavg",
        "tmax_days_ge_100f",
        "tmean_jja",
        "tmin_days_ge_70f",
        "tmin_days_le_0f",
        "tmin_days_le_32f",
        "tmin_jja",
        "pr_annual",
    ]
    raster_files: dict[str, list[str]] = {}
    for var in variables:
        raster_files[var] = [
            f"{var}_conus_grid_10km.tif",
            f"{var}_alaska_grid_10km.tif",
            f"{var}_hawaii_grid_10km.tif",
            f"{var}_puerto_rico_grid_10km.tif",
        ]

    try:
        # Get county geometries
        counties = get_county_geometries(gwl_file)

        # Process each raster and collect results
        all_results = []
        for var_name, raster_file_set in raster_files.items():
            results = []
            for ndx, raster_file in enumerate(raster_file_set):
                raster_path = os.path.join(raster_dir, raster_file)
                if not os.path.exists(raster_path):
                    print(f"Raster file not found: {raster_path}")
                    continue

                if ndx == 0:
                    results.append(
                        process_raster(
                            raster_path,
                            counties[~counties["FIPS"].isin(["02", "15", "72"])],
                            var_name,
                        )
                    )
                elif ndx == 1:
                    results.append(
                        process_raster(
                            raster_path,
                            counties[counties["FIPS"].isin(["02"])],
                            var_name,
                        )
                    )
                elif ndx == 2:
                    results.append(
                        process_raster(
                            raster_path,
                            counties[counties["FIPS"].isin(["15"])],
                            var_name,
                        )
                    )
                elif ndx == 3:
                    results.append(
                        process_raster(
                            raster_path,
                            counties[counties["FIPS"].isin(["72"])],
                            var_name,
                        )
                    )
                merged_results = pd.concat(results, ignore_index=True)
            all_results.append(merged_results)

        # Merge all results
        raw_results = all_results[0]
        for df in all_results[1:]:
            raw_results = raw_results.merge(df, on="county_id")

        counties_gdf = raw_results

        if "tmin_days_le_0f" in counties_gdf.columns:
            counties_gdf.loc[counties_gdf["tmin_days_le_0f"] < 0, "tmin_days_le_0f"] = 0
        if "tmin_days_le_32f" in counties_gdf.columns:
            counties_gdf.loc[
                counties_gdf["tmin_days_le_32f"] < 0, "tmin_days_le_32f"
            ] = 0

        # Invert the values in tmin_days_ge_70f: replace with 365.25 - current_value
        if "tmin_days_ge_70f" in counties_gdf.columns:
            counties_gdf["tmin_days_ge_70f"] = 365.25 - counties_gdf["tmin_days_ge_70f"]

        output_json = "data/outputs/us_climate_normals_1991-2020.json"
        counties_gdf.to_json(output_json, orient="records", indent=4)
        print(f"Exported final results to {output_json}")

    except Exception as e:
        print(f"Error in main function: {e}")
        raise


if __name__ == "__main__":
    main()
