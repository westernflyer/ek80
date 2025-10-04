# Process EK80 echosounder data

## Description

This repository contains scripts for processing and visualizing echosounder data
collected by Simrad echosounders, using the library
[echopype](https://github.com/OSOceanAcoustics/echopype). All data is stored
using the [Zarr format](https://zarr.dev/).

The repository consists of 3 scripts, which make up a data processing pipeline:

- `convert_raw.py`: convert raw data (`.raw`) that was collected by the echosounder
   into Zarr format, then save it to disk;
- `calc_sv.py`: Using the converted files, calculate Sv (volume
  backscattering strength) and save it to disk;
- `plot_mvbs.py`: Read the Sv data, calculate MVBS, then plot the results.

## Requirements

Python 3.10-3.12. This requirement is due to echopype. 

## Data structure

Although it is now required, the processing pipeline works most efficiently if
the data directory structure is organized in the following way:

```aiignore
/path/to/root-id
    raw
        idstamp1.raw
        idstamp2.raw
        ...
    converted
        idstamp1.zarr
        idstamp2.zarr
        ...
    sv
        idstamp1.sv
        idstamp2.sv
        ...
```

where `root-id` is a unique identifier for a particular deployment of the
echosounder. This is typically the date and time of the deployment.

- The directory `/path/to/root-id` is the root directory for the deployment;
- The directory `raw` contains the raw data files (`.raw`), as downloaded from the
  echosounder;
- The directory `converted` contains the converted data as converted 
  by `convert_raw`;
- The directory `sv` contains the Sv data as calculated by `calc_sv`.

The scripts are set up such that when used with the `--root-dir` option, the
output of one script is easily used as input for the next.

Here's an example of a typical directory structure:
```aiignore
/home/ek80/250416WF/
├── raw
│   ├── 250416WF-D20250416-T191232.raw
│   ├── 250416WF-D20250416-T191355.raw
│   ├── 250416WF-D20250416-T191516.raw
│   ├── ...
├── converted
│   ├── 250416WF-D20250416-T191232.zarr
│   ├── 250416WF-D20250416-T191355.zarr
│   ├── 250416WF-D20250416-T191516.zarr
│   ├── ...
└── sv
    ├── 250416WF-D20250416-T191232.sv
    ├── 250416WF-D20250416-T191355.sv
    ├── 250416WF-D20250416-T191516.sv
    ├── ...
```

Here, `250416WF` is `root-id`, the unique identifier for the deployment. The
path `/home/ek80/250416WF/` is the root directory, and would be used as the
value for the `--root-dir` option.

## Workflow

### Set up virtual environment

```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Convert raw data and save in Zarr format

Assume that the root directory is `/home/ek80/250416WF/`. Then the
raw files should be in directory `/home/ek80/250416WF/raw/`. The
following would then convert them all to Zarr format, putting the results in the
directory `/home/ek80/250416WF/converted/`:

```shell
python3 -m convert_raw --root-dir=/home/ek80/250416WF/
```


### Calculate Sv

To calculate Sv from the results of the previous step, run:

```shell
python3 -m calc_sv --root-dir=/home/ek80/250416WF/
```

Note that this uses the same `--root-dir` option as the previous step. The
results are saved in the directory `/home/ek80/250416WF/sv/`.

### Plot MVBS

Finally, using the results from the previous step, plot the MVBS:

```shell
python3 -m plot_mvbs --root-dir=/home/ek80/250416WF/
```

Again, note that this uses the same `--root-dir` option as the previous step.


# Copyright

Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>

See the file LICENSE.txt for your rights.
