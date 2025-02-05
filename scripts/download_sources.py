#!/usr/bin/env python3
"""
MIT License

Copyright (c) 2025 Jacqueline Ryan

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""
"""Download data sources for NCA counties database."""
import argparse
import boto3
from pathlib import Path
import requests
import tarfile


def download_noaa_normals(src_dir: Path) -> None:
    URL = "https://noaa-normals-pds.s3.amazonaws.com/normals-annualseasonal/1991-2020/archive/us-climate-normals_1991-2020_v1.0.1_annualseasonal_multivariate_by-station_c20230404.tar.gz"
    FILENAME = URL.split("/")[-1]

    src_dir.mkdir(parents=True, exist_ok=True)

    filepath = src_dir / FILENAME

    # Only download if it hasn't been done already to avoid excess egress costs for NOAA
    # 1991-2020 climate normals do not change over time
    if filepath.exists():
        print(f"File {FILENAME} already exists in {src_dir}")
        return

    print(f"Downloading {FILENAME}...")
    response = requests.get(URL, stream=True)
    response.raise_for_status()

    with open(filepath, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Extracting {FILENAME}...")
    with tarfile.open(filepath, "r:gz") as tar:
        tar.extractall(path=src_dir)

    print("Download and extraction complete")


def sync_s3_bucket(src_dir: Path) -> None:
    s3 = boto3.client("s3")
    bucket = "ar-db25"
    prefix = "ar-parent/nca-atlas/"

    src_dir.mkdir(parents=True, exist_ok=True)

    # Sync the bucket key to the sources directory
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):  # Skip directories
                continue

            localpath = src_dir / Path(key).relative_to(prefix)
            localpath.parent.mkdir(parents=True, exist_ok=True)

            if not localpath.exists() or obj["Size"] != localpath.stat().st_size:
                print(f"Downloading {key}...")
                localpath.parent.mkdir(parents=True, exist_ok=True)
                s3.download_file(bucket, key, str(localpath))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download NCEI Normals (CSV) and NCA Atlas GeoJSON files"
    )
    parser.add_argument(
        "--no-aws",
        action="store_true",
        help="skip downloading NCA Atlas data from AWS.",
    )
    args = parser.parse_args()
    SOURCE_DIR = Path("data/sources")
    try:
        download_noaa_normals(SOURCE_DIR)
        if not args.no_aws:
            sync_s3_bucket(SOURCE_DIR)
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
