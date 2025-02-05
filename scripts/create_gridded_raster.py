#!/usr/bin/env python3
"""
MIT License

Copyright (c) 2025 Jacqueline Ryan, ReadyPlayerEmma

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import argparse
from pathlib import Path
import sys
import pandas as pd
import numpy as np
import rasterio
from rasterio.transform import from_origin
from pyproj import Transformer
import geopandas as gpd
from rasterio.features import geometry_mask

# Import the OrdinaryKriging class from PyKrige
from pykrige.ok import OrdinaryKriging
from pykrige.uk import UniversalKriging


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Create a gridded raster of a given measurement from weather station data."
    )
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit(1)
    parser.add_argument(
        "input",
        type=Path,
        help="Path to input CSV file containing station data",
    )
    parser.add_argument("output", type=Path, help="Path for output GeoTIFF file")
    parser.add_argument(
        "-m",
        "--measurement",
        type=str,
        default="ANN-TAVG-NORMAL",
        help="Name of the measurement to use for creating the raster",
    )
    parser.add_argument(
        "-r",
        "--resolution",
        type=float,
        default=20000,
        help="Grid cell size in meters (default: 5000 meters or 5km)",
    )
    parser.add_argument(
        "--variogram_model",
        type=str,
        default="spherical",
        help="Variogram model to use (e.g., 'spherical', 'exponential', etc.)",
    )
    parser.add_argument(
        "--interp_method",
        type=str,
        default="universal",  # Changed default to universal
        choices=["ordinary", "universal"],
        help="Interpolation method to use (ordinary or universal kriging)",
    )
    parser.add_argument(
        "--drift_terms",
        type=str,
        nargs="+",
        default=["regional_linear", "point_log"],
        choices=["regional_linear", "point_log", "external_Z", "specified"],
        help="Drift terms for universal kriging",
    )
    parser.add_argument(
        "--nlags",
        type=int,
        default=20,
        help="Number of lags to use for variogram calculation",
    )
    return parser.parse_args()


def load_and_clean_data(input_file: Path, variable_name: str) -> pd.DataFrame:
    """
    Load and clean station data from CSV file.
    """
    df = pd.read_csv(input_file)

    if f"comp_flag_{variable_name}" not in df.columns:
        raise ValueError(
            f"Expected a completeness flag column 'comp_flag_{variable_name}' "
            f"but it was not found in the CSV. Check your data."
        )

    # Filter out stations that don't record this measurement
    df = df.dropna(subset=[variable_name])
    # Only completeness flag values of (C)omplete, (S)tandard, or (R)epresentative should be used
    df = df[df[f"comp_flag_{variable_name}"].isin(["C", "S", "R"])]
    # Exclude the typical no-data value
    df = df[df[variable_name] != 9999]

    print(f"Total stations after filtering: {len(df)}")
    nstations_complete = df[f"comp_flag_{variable_name}"].value_counts().to_string()
    print(f"Stations by completeness flag:\n{nstations_complete}")

    return df


def transform_coordinates(df: pd.DataFrame) -> tuple:
    """
    Transform station coordinates from WGS84 to CONUS Albers (EPSG:5072).
    """
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:5072", always_xy=True)

    # Transform station coordinates
    x_stations, y_stations = transformer.transform(
        df["LONGITUDE"].values, df["LATITUDE"].values
    )

    # Define the bounds in Albers projection (experimentally derived for CONUS)
    x_min, y_min = transformer.transform(-125, 20)
    x_max, y_max = transformer.transform(-60, 50)

    return x_stations, y_stations, x_min, x_max, y_min, y_max


def create_grid(bounds: tuple, resolution: float) -> tuple:
    """
    Creates a meshgrid of x, y coordinates in ascending order:
    - x: from x_min to x_max
    - y: from y_min to y_max

    Returns (X_mesh, Y_mesh) arrays.
    """
    x_min, x_max, y_min, y_max = bounds

    # x ascending from left (min) to right (max)
    grid_x = np.arange(x_min, x_max + resolution, resolution)
    # y ascending from bottom (min) to top (max)
    grid_y = np.arange(y_min, y_max + resolution, resolution)

    # Create 2D meshgrid
    xx, yy = np.meshgrid(grid_x, grid_y)
    return xx, yy


def filter_close_points(x, y, values, min_distance=1000):  # distance in meters
    points = np.column_stack((x, y))
    filtered_indices = []
    for i in range(len(points)):
        if i in filtered_indices:
            continue
        distances = np.linalg.norm(points[i] - points, axis=1)
        close_points = np.where(distances < min_distance)[0]
        if len(close_points) > 1:  # if there are other points too close
            # Keep only the first point from the cluster
            filtered_indices.extend(close_points[1:])

    mask = ~np.isin(np.arange(len(x)), filtered_indices)
    return x[mask], y[mask], values[mask]


class KrigingInterpolator:
    """
    A wrapper around PyKrige's kriging implementations.
    Supports both ordinary and universal kriging methods.
    """

    def __init__(
        self,
        method="universal",
        variogram_model="spherical",
        drift_terms=["regional_linear"],
        nlags=20,  # Using 20 instead of default 6 for better variogram estimation
        weight=True,  # Enable distance-based variogram point weighting
        **kwargs,
    ):
        self.method = method
        self.variogram_model = variogram_model
        self.drift_terms = drift_terms
        self.nlags = nlags
        self.weight = weight
        self.kwargs = kwargs

    def interpolate(self, x, y, values, gridx, gridy):
        """
        Perform kriging interpolation.
        - gridx, gridy must be strictly ascending 1D arrays
        - Returns interpolated 2D array z in shape (len(gridy), len(gridx))
        """
        if self.method == "ordinary":
            krig = OrdinaryKriging(
                x,
                y,
                values,
                variogram_model=self.variogram_model,
                nlags=self.nlags,
                weight=self.weight,
                **self.kwargs,
            )
        elif self.method == "universal":
            krig = UniversalKriging(
                x,
                y,
                values,
                variogram_model=self.variogram_model,
                drift_terms=self.drift_terms,
                nlags=self.nlags,
                weight=self.weight,
                **self.kwargs,
            )
        else:
            raise ValueError(f"Interpolation method {self.method} not supported")

        # Execute kriging
        z, ss = krig.execute("grid", gridx, gridy)

        return z, ss

    @staticmethod
    def calculate_drift_components(x, y):
        """
        Calculate additional drift components based on geographical features.
        This can be customized based on known trends in your data.
        """
        # Elevation-based drift (if we add DEMs to ancillary files)
        # elevation = get_elevation(x, y)
        # drift_elevation = elevation / np.max(elevation)

        # Distance from coast (if relevant)
        # coast_distance = calculate_coast_distance(x, y)
        # drift_coastal = np.exp(-coast_distance / scale_factor)

        # Latitude-based temperature gradient
        drift_latitude = (y - np.min(y)) / (np.max(y) - np.min(y))

        return drift_latitude


def interpolate_measurement(
    points: np.ndarray,
    values: np.ndarray,
    grid_coords: tuple,
    method: str,
    variogram_model: str,
) -> np.ndarray:
    """
    Interpolate values onto the grid using the specified kriging method.

    grid_coords is assumed to be (X_mesh, Y_mesh) from create_grid()
    where each is a 2D array. We'll extract unique x and y from them,
    in ascending order, then flip the result so row=0 corresponds
    to y_max (north-up).
    """
    # Extract ascending 1D arrays
    unique_x = np.unique(grid_coords[0])  # ascending x
    unique_y = np.unique(grid_coords[1])  # ascending y

    x = points[:, 0]
    y = points[:, 1]

    # Instantiate the kriging interpolator.
    krig = KrigingInterpolator(method=method, variogram_model=variogram_model)
    z, ss = krig.interpolate(x, y, values, unique_x, unique_y)

    # PyKrige returns z with z[0, :] at the smallest y (i.e., y_min).
    # If we want row=0 to correspond to y_max for a north-up raster,
    # we flip the array vertically.
    z = np.flipud(z)

    return z  # If you want the kriging variance, you could also return ss.


def clip_to_lower48(
    grid_data: np.ndarray, transform: rasterio.transform.Affine, clip_file: str
) -> np.ndarray:
    """
    Clip the grid data to the lower 48 boundary using the provided geopackage file.
    Pixels outside the boundary are set to np.nan.
    """
    gdf = gpd.read_file(clip_file)
    # Ensure the clipping geometry is in EPSG:5072
    if gdf.crs != "EPSG:5072":
        gdf = gdf.to_crs("EPSG:5072")
    geoms = [geom for geom in gdf.geometry]

    # Create a mask: with invert=True, pixels inside the geometries are True.
    mask = geometry_mask(
        geoms, out_shape=grid_data.shape, transform=transform, invert=True
    )
    clipped_data = np.where(mask, grid_data, np.nan)
    return clipped_data


def write_geotiff(
    output_file: Path, grid_data: np.ndarray, transform: rasterio.transform.Affine
):
    """
    Write gridded data to a GeoTIFF file using the given transform.
    """
    # Make sure the data is in a floating type for nodata=np.nan
    if not np.issubdtype(grid_data.dtype, np.floating):
        grid_data = grid_data.astype("float32")

    with rasterio.open(
        output_file,
        "w",
        driver="GTiff",
        height=grid_data.shape[0],
        width=grid_data.shape[1],
        count=1,
        dtype=grid_data.dtype,
        crs="EPSG:5072",
        transform=transform,
        nodata=np.nan,
    ) as dst:
        dst.write(grid_data, 1)
        # You can store any custom tags you like
        dst.update_tags(
            TIFFTAG_DATETIME=pd.Timestamp.now().strftime("%Y:%m:%d %H:%M:%S"),
            TIFFTAG_DOCUMENTNAME="1991-2020 Climate Normal Grid",
            TIFFTAG_SOFTWARE="create_gridded_raster.py",
            units="degrees Fahrenheit",
            source="NOAA NCEI 1991-2020 Climate Normals",
            projection="CONUS Albers Equal Area (EPSG:5072)",
        )


def create_measurement_grid(
    input_file: Path,
    output_file: Path,
    variable_name: str = "ANN-TAVG-NORMAL",
    resolution: float = 5000,
    interp_method: str = "ordinary",
    variogram_model: str = "spherical",
):
    """
    Create a gridded raster of a given measurement from weather station data using kriging.
    """
    # Load and clean data
    df = load_and_clean_data(input_file, variable_name)

    # Transform coordinates
    x_stations, y_stations, x_min, x_max, y_min, y_max = transform_coordinates(df)

    x_filtered, y_filtered, values_filtered = filter_close_points(
        x_stations, y_stations, df[variable_name].values, min_distance=resolution
    )

    # Create grid (2D mesh in ascending order for x and y)
    grid_x_mesh, grid_y_mesh = create_grid((x_min, x_max, y_min, y_max), resolution)

    # Prepare coordinates and values for interpolation
    points = np.column_stack((x_filtered, y_filtered))
    values = values_filtered

    # Perform interpolation using ordinary kriging
    grid_temp = interpolate_measurement(
        points,
        values,
        (grid_x_mesh, grid_y_mesh),
        method=interp_method,
        variogram_model=variogram_model,
    )

    # Create an affine transform consistent with top-left = (x_min, y_max)
    # Since we flipped the data after kriging, row=0 corresponds to y_max.
    transform_affine = from_origin(x_min, y_max, resolution, resolution)

    # Clip the grid to the lower 48 boundary
    clip_file = "data/ancillary/nation_5m_lower48_epsg5072.gpkg"
    grid_temp_clipped = clip_to_lower48(grid_temp, transform_affine, clip_file)

    # Write the clipped grid to GeoTIFF
    write_geotiff(output_file, grid_temp_clipped, transform_affine)

    print(f"Successfully created gridded raster of {variable_name}: {output_file}")


def main():
    """Main entry point of the script."""
    args = parse_arguments()
    create_measurement_grid(
        args.input,
        args.output,
        variable_name=args.measurement,
        resolution=args.resolution,
        interp_method=args.interp_method,
        variogram_model=args.variogram_model,
    )


if __name__ == "__main__":
    main()
