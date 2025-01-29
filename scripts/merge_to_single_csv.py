#!/usr/bin/env python

import argparse
from pathlib import Path

import pandas as pd


def combine_csv_as_wide_table(folder: Path, output_csv: Path, recursive=False):
    """
    Scans all CSV files in 'folder' (optionally recursively).
    Each CSV is assumed to have:
       - Exactly 1 header row
       - Exactly 1 data row
       - The first column = 'STATION' with a unique station ID
    We merge them into a single "wide" CSV file, writing to 'output_csv'.
    """

    # Gather CSV files
    csv_files = folder.rglob("*.csv") if recursive else folder.glob("*.csv")
    csv_files = list(csv_files)

    if not csv_files:
        print(f"No CSV files found in '{folder}'. Exiting.")
        return

    # We will build a list of dictionaries. Each dict = one station's row of data.
    records = []

    for csv_file in csv_files:
        try:
            # Read just 1 row into a DataFrame
            df = pd.read_csv(
                csv_file, nrows=1
            )  # By default, header=0 -> first row is header
        except Exception as e:
            print(f"Skipping file {csv_file}, error reading CSV: {e}")
            continue

        if df.empty:
            print(f"Warning: {csv_file} is empty or has no data row. Skipping.")
            continue

        # We assume there's exactly 1 data row, so just convert that row to a dictionary
        row_dict = df.iloc[0].to_dict()

        # Optionally, we can store the file path or filename if needed
        # row_dict['source_file'] = csv_file.name

        # Append to our records
        records.append(row_dict)

    if not records:
        print("No usable records found.")
        return

    # Convert our list of dictionaries to a big DataFrame
    # Missing columns will become NaN automatically
    wide_df = pd.DataFrame(records)

    # Ensure 'STATION' is the first column (and is unique)
    if "STATION" in wide_df.columns:
        wide_df.set_index("STATION", inplace=True)  # Makes STATION the row index

    # Write out to CSV
    wide_df.to_csv(output_csv, index=bool("STATION" not in wide_df.columns))
    print(f"Combined table written to '{output_csv}'.")
    print(f"Total records: {len(wide_df)}")
    print(f"Total columns: {len(wide_df.columns)}")


def main():
    parser = argparse.ArgumentParser(
        description="Combine single-row CSV files into one wide table."
    )
    parser.add_argument("folder", help="Folder containing the CSV files.")
    parser.add_argument("output", help="Output CSV file path.")
    parser.add_argument(
        "--recursive", action="store_true", help="Search subfolders recursively."
    )
    args = parser.parse_args()

    folder_path = Path(args.folder)
    if not folder_path.is_dir():
        print(f"Error: '{folder_path}' is not a directory.")
        return

    output_csv = Path(args.output)

    combine_csv_as_wide_table(folder_path, output_csv, recursive=args.recursive)


if __name__ == "__main__":
    main()
