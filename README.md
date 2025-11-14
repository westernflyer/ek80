# Process EK80 echosounder data

## Description

This repository contains scripts for processing and visualizing echosounder data
collected by Simrad echosounders, using the library
[echopype](https://github.com/OSOceanAcoustics/echopype). All data is stored
using the [Zarr format](https://zarr.dev/).

The repository consists of 4 scripts, which make up a data processing pipeline:

- `convert_raw.py`: convert raw data (`.raw`) that was collected by the
  echosounder into Zarr format, then save it to disk;
- `calc_sv.py`: Using the converted files, calculate Sv (volume
  backscattering strength) and save it to disk;
- `calc_mvbs.py`: Using the Sv data, calculate Mean Volume Backscattering
  Strength (MVBS) and save it to disk;
- `plot_mvbs.py`: Read MVBS data, then plot the results.

## Requirements

Python 3.12 or 3.13. This requirement is due to echopype. As of 11/14/2025,
Python 3.14 does not work because the virtual environment does not build
properly.

Package `echopype` v0.11 or later. Version 0.10 does not include some functions
that these scripts use.

Zarr version 3 or later is required because that is the only version that
`echopype` v0.11 supports.

## Workflow

### Directory structure

Assuming that the raw echosounder data lies in `~/Data/ek80`, following the
workflow outlined below will result in the following directory
structure:

```
~/Data/ek80/
├── 250416WF-D20250416-T191232.raw
├── 250416WF-D20250416-T191355.raw
├── 250416WF-D20250416-T191516.raw
├── ...
├── echodata_zarr
│   ├── 250416WF-D20250416-T191232.zarr
│   ├── 250416WF-D20250416-T191355.zarr
│   ├── 250416WF-D20250416-T191516.zarr
│   ├── ...
└── Sv_zarr
│   ├── 250416WF-D20250416-T191232_Sv.zarr
│   ├── 250416WF-D20250416-T191355_Sv.zarr
│   ├── 250416WF-D20250416-T191516_Sv.zarr
│   ├── ...
└── MVBS_zarr
│   ├── 250416WF-D20250416-T191232_MVBS.zarr
│   ├── 250416WF-D20250416-T191355_MVBS.zarr
│   ├── 250416WF-D20250416-T191516_MVBS.zarr
│   ├── ...
```

### Set up a virtual environment

```shell
python -m venv ek80-venv
source ek80-venv/bin/activate
pip install -r requirements.txt
```

### Convert raw data and save in Zarr format

Assuming that the raw data is stored in `~/Data/ek80`, this would convert it
all and put it in `~/Data/ek80/echodata_zarr`:

```shell
python3 -m convert_raw ~/Data/ek80/*.raw
```

### Calculate Sv

This would calculate Sv from the results of the previous step and put it in
`~/Data/ek80/Sv_zarr`:

```shell
python3 -m calc_sv ~/Data/ek80/Sv_zarr/*.zarr
```

### Calculate MVBS

This would calculate MVBS from the results of the previous step and put it in
`~/Data/ek80/MVBS_zarr`:

```shell
python3 -m calc_mvbs ~/Data/ek80/MVBS_zarr/*.zarr
```

### Plot MVBS

Finally, select a day and plot it. For example, for 2025-Apr-30:

```shell
python3 -m plot_mvbs  ~/Data/ek80/MVBS_zarr/250430WF*.zarr
```

# Copyright

Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>

See the file LICENSE.txt for your rights.
