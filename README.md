# Process EK80 echosounder data

## Description

This repository contains scripts for processing and visualizing echosounder data
collected by Simrad echosounders, using the library
[echopype](https://github.com/OSOceanAcoustics/echopype). All data is stored
using the [Zarr format](https://zarr.dev/).

Four scripts make up the core of the data processing pipeline:

- `convert_raw.py`: convert raw data (`.raw`) that was collected by the
  echosounder into Zarr (`.zarr`) format, then save it to disk in the `echodata_zarr`
  subdirectory.
- `calc_sv.py`: Using the converted `.zarr` files, calculate Sv (volume
  backscattering strength) and save it to disk in the `Sv_zarr` subdirectory.
- `calc_mvbs.py`: Using the Sv data, calculate Mean Volume Backscattering
  Strength (MVBS) and save it to disk in the `MVBS_zarr` subdirectory.
- `plot_mvbs.py`: Read MVBS data, then plot the results.

In addition, there are two other scripts for convenience: 
- `detect_bottom.py`: Detect the bottom of the water column using the Sv data. Results are saved as a
netData (`.nc`) file in the `depth` subdirectory.
- `depth_csv.py`: Subsample the depth netCDF data, saving as a CSV file.

## Requirements

Python 3.12 or 3.13. This requirement is due to echopype. As of 11/14/2025,
the virtual environment will not build properly under Python 3.14.

Package `echopype` v0.11 or later. Version 0.10 does not include some functions
that these scripts use.

Zarr version 3 or later is required because that is the only version that
`echopype` v0.11 supports.

## Directory structure

Assuming that the raw echosounder data lies in `~/Data/ek80`, following the
workflow outlined below will result in the following directory structure:

```
~/Data/ek80/
├── processed
│   ├── depth
│   │   ├── 250520WF-D20250520-T172150_depth.nc
│   │   ├── 250520WF-D20250520-T172443_depth.nc
│   │   ├── 250520WF-D20250520-T172739_depth.nc
│   │   ├── 250520WF-D20250520-T173008_depth.nc
│   │   ├──       ...
│   ├── echodata_zarr
│   │   ├── 250520WF-D20250520-T172150.zarr
│   │   ├── 250520WF-D20250520-T172443.zarr
│   │   ├── 250520WF-D20250520-T172739.zarr
│   │   ├── 250520WF-D20250520-T173008.zarr
│   │   ├──       ...
│   ├── MVBS_zarr
│       ├── 250520WF-D20250520-T172150_Sv.zarr
│       ├── 250520WF-D20250520-T172443_Sv.zarr
│       ├── 250520WF-D20250520-T172739_Sv.zarr
│       ├── 250520WF-D20250520-T173008_Sv.zarr
│       ├──       ...
└── raw
    ├── 250520WF-D20250520-T172150.raw
    ├── 250520WF-D20250520-T172443.raw
    ├── 250520WF-D20250520-T172739.raw
    ├── 250520WF-D20250520-T173008.raw
    ├──           ...
```

## Workflow

### Install uv

While one can use pip to create and activate a virtual environment, it is far
faster to use [uv](https://docs.astral.sh/uv/) instead. Follow the [installation
instructions](https://docs.astral.sh/uv/getting-started/installation/) on the uv
website.

### Download the repository

Download the repository to a convenient location on your computer, then
change directory into it.

```shell
git clone https://github.com/westernflyer/ek80.git
cd ek80
```

### Create and sync the virtual environment

```shell
# Create the virtual environment. By default, it will be stored in .venv:
uv venv
# Activate the virtual environment:
source .venv/bin/activate
# Install the required packages into the virtual environment:
uv sync
```

### Convert raw data and save in Zarr format

Assuming that the raw data is stored in `~/Data/ek80/raw`, the following would 
convert it all and put the results in directory 
`~/Data/ek80/processed/echodata_zarr/`:

```shell
python3 convert_raw.py ~/Data/ek80/raw/*.raw
```

### Calculate Sv

The following would calculate Sv from the results of the previous step and put 
the results in directory `~/Data/ek80/processed/Sv_zarr/`:

```shell
python3 calc_sv.py ~/Data/ek80/processed/echodata_zarr/*.zarr
```

### Calculate MVBS

The script `calc_mvbs.py` may emit warnings about performance issues or 
supporting consolidated metadata in the future. For now, these warnings
can be ignored.

The following would calculate MVBS from the results of the previous step and 
put the results in directory `~/Data/ek80/processed/MVBS_zarr/`.

```shell
python3 calc_mvbs.py ~/Data/ek80/processed/Sv_zarr/*.zarr
```

### Plot MVBS

Finally, select a day and plot it. For example, for 2025-Apr-30:

```shell
python3 plot_mvbs.py  ~/Data/ek80/processed/MVBS_zarr/250430WF*.zarr
```

## Other scripts

- `detect_bottom.py`: Detect the bottom of the water column using the Sv data. Results are saved as a netData (`.nc`) file in the `depth` subdirectory.
- `depth_csv.py`: Subsample the depth netCDF data, saving as a CSV file.
- 'uploader.py': Upload raw data to the WFF S3 archive bucket.

# Copyright

Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>

See the file LICENSE.txt for your rights.
