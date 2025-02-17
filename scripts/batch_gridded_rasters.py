import subprocess
import os
import sys

FIELD_MAP: dict[str, str] = {
    "pr_above_nonzero_99th": "--",
    "prmax1day": "--",
    "prmax5yr": "--",
    "tavg": "ANN-TAVG-NORMAL",
    "tmax1day": "--",
    "tmax_days_ge_100f": "ANN-TMAX-AVGNDS-GRTH100",
    "tmax_days_ge_105f": "--",
    "tmax_days_ge_95f": "--",
    "tmean_jja": "JJA-TAVG-NORMAL",
    "tmin_days_ge_70f": "ANN-TMIN-AVGNDS-LSTH070",
    "tmin_days_le_0f": "ANN-TMIN-AVGNDS-LSTH000",
    "tmin_days_le_32f": "ANN-TMIN-AVGNDS-LSTH032",
    "tmin_jja": "JJA-TMIN-NORMAL",
    "pr_annual": "ANN-PRCP-NORMAL",
    "pr_days_above_nonzero_99th": "--",
}


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(script_dir, "create_gridded_raster.py")
    input_csv = os.path.abspath(os.path.join(script_dir, "../../all_stations.csv"))
    output_dir = os.path.abspath(os.path.join(script_dir, "../data/outputs"))

    os.makedirs(output_dir, exist_ok=True)

    component = "conus"
    for json_field, csv_field in FIELD_MAP.items():
        if csv_field == "--":
            continue

        output_path = os.path.join(
            output_dir, f"{json_field}_{component}_grid_10km.tif"
        )

        try:
            subprocess.run(
                [
                    sys.executable,  # Use the current Python interpreter
                    script_path,
                    input_csv,
                    output_path,
                    "-m",
                    csv_field,
                    "--resolution",
                    "10000",
                    "--component",
                    component.upper(),
                ],
                check=True,
            )
        except Exception as e:
            print(f"Error executing {script_path}: {e}")


if __name__ == "__main__":
    main()
