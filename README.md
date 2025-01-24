# nca-counties-infrastructure

This repository contains infrastructure for downloading, pre-processing, and seeding a PostGIS database for serving a backend that provides county-level climate data for the United States. This work is part of the American Resiliency community and is associated with [NCACounties](https://github.com/jackiryan/NCACounties), the frontend component of this project.

## Installation

### Download data sources

The `download_sources.py` script fetches all required data sources associated with this project. This step is not included in the Dockerfile itself to maintain accessibility for those without AWS access. You will need to be a user on the American Resiliency AWS account to download the NCA Atlas geojson files directly, otherwise you will need to download them manually from the following website: [NCA Atlas Climate Data](https://atlas.globalchange.gov/pages/data). If you have generated an access key for your user on AWS, you can set up the environment for the script like so:

```bash
# Follow the instructions to enter your access key and secret key, region is us-east-2, format can be skipped
aws configure
python3 -m venv venv
source venv/bin/activate
python3 -m pip install -r requirements.txt
scripts/download_sources.py
```

All source data files should now be located in the `data/sources` directory (i.e., from the top level directory of this repository). If you downloaded the files manually, you should make sure to place them in this directory for the next step.

### Build Docker image

If you are on a Linux or MacOS system, simply run the build script after completing the previous section:

```bash
./build.sh
```

If you mess up and need to start over, run the clean script:

```bash
./clean.sh
```

A Windows compatible version of these steps will be available later.

## Contact

Jacqueline Ryan [email](mailto:jacquiepi@protonmail.com)