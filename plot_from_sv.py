"""
Process and compute Mean Volume Backscattering Strength (MVBS) from Sv data stored in Zarr format.

This script reads Sv data from Zarr files, subsamples the data into complete bins, computes MVBS
for each subsample, then concatenates the results. It then produces plots for the
38 kHz and 200 kHz echograms.

The script ensures directories for storing processed data are created. The
processing iteratively handles incomplete bins across files by leveraging
leftover data from prior iterations to ensure consistency in resampling.

Raises:
    RuntimeError: If no MVBS datasets are computed, indicating input files may be misconfigured.
"""
import argparse
import glob
import os.path
import warnings
from pathlib import Path

import echopype as ep
import matplotlib.pyplot as plt
import xarray as xr

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)

# Path to the root data directory from command line
parser = argparse.ArgumentParser(description="Process and plot MVBS from Sv Zarr data.")
parser.add_argument(
    "zarr_inputs",
    nargs="*",
    help="One or more .zarr directories or glob patterns to read. "
         "If omitted, defaults to ./Sv_zarr/*.zarr under the current directory.",
)
parser.add_argument(
    "--ping-bin",
    dest="ping_bin",
    default="5s",
    help="Ping time bin size for MVBS computation. Default: 5s",
)
parser.add_argument(
    "--range-bin",
    dest="range_bin",
    default="0.5m",
    help="Range bin size for MVBS computation. Default: 0.5m",
)
args = parser.parse_args()

if args.zarr_inputs:
    zarr_input_dirs = args.zarr_inputs
else:
    zarr_input_dirs = [os.path.join(os.getcwd(), "Sv_zarr/*.zarr")]

Sv_zarr_path_list = []
for pattern in zarr_input_dirs:
    matches = [Path(p) for p in glob.glob(str(pattern))]
    if not matches:
        print(f"Warning: no matches for pattern: {pattern}")
    for m in matches:
        if m.name.endswith(".zarr") and m.is_dir():
            Sv_zarr_path_list.append(m)

if not Sv_zarr_path_list:
    raise SystemExit("No .zarr inputs found from provided patterns.")

print(f"Found {len(Sv_zarr_path_list)} Sv zarr directories")
print(f"Subsampling into ping bins of size {args.ping_bin}, "
      f"and range bins of size {args.range_bin}")

Sv_zarr_path_list.sort()

# Initialize leftover ds_Sv zarr as None
leftover_ds_Sv = None

# Accumulate MVBS datasets in memory instead of writing to disk
mvbs_parts = []

# Iterate through all Sv Zarr Paths
for file_path in Sv_zarr_path_list:

    # Open ds_Sv from disk Zarr
    print(f"Calculating MVBS from Sv file {file_path}")
    ds_Sv = xr.open_zarr(file_path)

    # Concat leftover Sv with current Sv
    if leftover_ds_Sv is not None:
        concat_ds_Sv = xr.concat([leftover_ds_Sv, ds_Sv], dim="ping_time")
    else:
        concat_ds_Sv = ds_Sv

    # Create a Resample object for subsampling into user-specified ping bins
    resampled_data = concat_ds_Sv.resample(ping_time=args.ping_bin, skipna=True)

    # Determine the start index of the last incomplete bin
    cutoff_index = max(group.start for group in resampled_data.groups.values())

    # Split data into complete and incomplete bins:

    # Take data up to the last complete bin
    complete_bins_Sv = concat_ds_Sv.isel(ping_time=slice(0, cutoff_index))

    # Keep remaining data for next iteration
    leftover_bin_Sv = concat_ds_Sv.isel(ping_time=slice(cutoff_index, -1))

    # Compute MVBS on current subset
    ds_MVBS = ep.commongrid.compute_MVBS(
        complete_bins_Sv,
        range_var="depth",
        range_bin=args.range_bin,
        ping_time_bin=args.ping_bin,
    )

    # Accumulate MVBS in memory
    mvbs_parts.append(ds_MVBS)

if not mvbs_parts:
    raise RuntimeError(
        "No MVBS datasets were computed. Check input Sv zarr files and resampling bins.")

# Concatenate along ping_time; datasets share frequency/depth coords
# Using data_vars='minimal' semantics is implicit in xr.concat for identical vars
ds_MVBS = xr.concat(mvbs_parts, dim="ping_time")

print("Plot 38 kHz channel...")
(
    ds_MVBS["Sv"]
    .isel(channel=0)  # Select a channel. Channel 0 is 38k
    .plot(x='ping_time', y='depth', yincrease=False, vmin=-80, vmax=-10)
)
plt.title("38 kHz")
plt.show()

print("Plot 200 kHz channel...")
(
    ds_MVBS["Sv"]
    .isel(channel=1)  # Select a channel. Channel 1 is 200k
    .plot(x='ping_time', y='depth', yincrease=False, vmin=-60, vmax=-10)
)
plt.title("200 kHz")
plt.show()
