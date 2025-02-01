import argparse
from pathlib import Path
import pandas as pd
import numpy as np
from scipy.interpolate import griddata
import rasterio
from rasterio.transform import from_origin
from pyproj import Transformer


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Create a gridded raster of a given measurement from weather station data."
    )
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
        default=5000,
        help="Grid cell size in meters (default: 5000 meters or 5km)",
    )
    return parser.parse_args()


def load_and_clean_data(input_file: Path, variable_name: str) -> pd.DataFrame:
    """
    Load and clean station data from CSV file.
    """
    df = pd.read_csv(input_file)

    # Filter out stations that don't record this measurement
    df = df.dropna(subset=[variable_name])
    # Only completeness flag values of (C)omplete, (S)tandard, or (R)epresentative should be used
    df = df[df[f"comp_flag_{variable_name}"].isin(["C", "S", "R"])]
    # 9999 is typically a nodata value, it should be caught by the comp_flag though
    df = df[df[variable_name] != 9999]

    print(f"Total stations after filtering: {len(df)}")
    nstations_complete = df[f"comp_flag_{variable_name}"].value_counts().to_string()
    print(f"Stations by completeness flag:\n{nstations_complete}")

    return df


def transform_coordinates(df: pd.DataFrame) -> tuple:
    """
    Transform station coordinates from WGS84 to CONUS Albers.
    """
    transformer = Transformer.from_crs("EPSG:4326", "EPSG:5072", always_xy=True)

    # Transform station coordinates
    x_stations, y_stations = transformer.transform(
        df["LONGITUDE"].values, df["LATITUDE"].values
    )

    # Define the bounds in Albers projection, these coordinates for
    # the corners of the CONUS are experimentally derived
    x_min, y_min = transformer.transform(-125, 20)
    x_max, y_max = transformer.transform(-60, 50)

    return x_stations, y_stations, x_min, x_max, y_min, y_max


def create_grid(bounds: tuple, resolution: float) -> tuple:
    x_min, x_max, y_min, y_max = bounds
    grid_x = np.arange(x_min, x_max + resolution, resolution)
    grid_y = np.arange(y_max, y_min - resolution, -resolution)
    return np.meshgrid(grid_x, grid_y)


def interpolate_temperatures(
    points: np.ndarray, values: np.ndarray, grid_coords: tuple
) -> np.ndarray:
    return griddata(points, values, grid_coords, method="cubic", fill_value=np.nan)


def write_geotiff(
    output_file: Path, grid_data: np.ndarray, transform: rasterio.transform.Affine
):
    """
    Write gridded data to GeoTIFF file.
    """
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
):
    """
    Create a gridded raster of a given measurement from weather station data.
    Uses the latest version of the CONUS Albers Equal Area projection (EPSG:5072).
    """
    # Load and clean data
    df = load_and_clean_data(input_file, variable_name)

    # Transform coordinates
    x_stations, y_stations, x_min, x_max, y_min, y_max = transform_coordinates(df)

    # Create grid
    grid_x_mesh, grid_y_mesh = create_grid((x_min, x_max, y_min, y_max), resolution)

    # Prepare coordinates and values for interpolation
    points = np.column_stack((x_stations, y_stations))
    values = df[variable_name].values

    # Perform interpolation
    grid_temp = interpolate_temperatures(points, values, (grid_x_mesh, grid_y_mesh))

    # Create geotransform
    transform = from_origin(x_min, y_max, resolution, resolution)

    # Write to GeoTIFF
    write_geotiff(output_file, grid_temp, transform)

    print(f"Successfully created gridded raster of {variable_name}: {output_file}")


def main():
    """Main entry point of the script."""
    args = parse_arguments()
    create_measurement_grid(args.input, args.output, args.measurement, args.resolution)


if __name__ == "__main__":
    main()
