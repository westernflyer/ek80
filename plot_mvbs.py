#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Plot MVBS echosounder data, using Sv data stored in Zarr format as the data source.

This script reads Sv data in Zarr format, subsamples the data into ping and
range bins, computes MVBS for each subsample, then concatenates the results. It
then produces plots for the 38 kHz and 200 kHz echograms.

The processing iteratively handles incomplete bins across files by leveraging
leftover data from prior iterations to ensure consistency in resampling.

Raises:
    RuntimeError: If no MVBS datasets are computed, indicating input files may be misconfigured.
"""
import argparse
import os.path
import sys
import warnings
from pathlib import Path

import echopype as ep
import matplotlib.pyplot as plt
import xarray as xr

from utilities import find_zarr_dirs

usagestr = """%(prog)s -h|--help
       %(prog)s [--ping-bin PING_BIN] [--range-bin RANGE_BIN] inputs ...
       %(prog)s [--ping-bin PING_BIN] [--range-bin RANGE_BIN] --root-dir ROOT_DIR
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


def calc_mvbs(zarr_input_dirs: list[Path] = None,
              ping_bin: str = "1s",
              range_bin: str = "0.5m") -> list[xr.Dataset]:
    """
    Calculate Mean Volume Backscattering Strength (MVBS) from Sv Zarr files.

    This function processes a list of Sv Zarr files, resampling them into user-specified
    ping and range bins, and then calculates the Mean Volume Backscattering Strength (MVBS).
    It handles partial bins by carrying over remaining data to the next iteration. The computed
    MVBS datasets are stored in memory and returned as a list.

    Parameters:
        zarr_input_dirs: List of Paths to the Sv Zarr directories to process.
        ping_bin: String specification of the ping bin size for time resampling (e.g., "1s", "5s").
        range_bin: String specification of the range bin size for depth resampling (e.g., "0.5m", "1m").

    Returns:
        List of xarray.Dataset objects, each containing the computed MVBS for a corresponding
        Sv Zarr file or subset.
    """

    print(f"Subsampling into ping bins of size {ping_bin}, "
          f"and range bins of size {range_bin}")

    # Initialize leftover ds_Sv zarr as None
    leftover_ds_Sv = None

    # Accumulate MVBS datasets in memory instead of writing to disk
    mvbs_parts = []

    # Iterate through all Sv Zarr Paths
    for zarr_input_dir in zarr_input_dirs:

        # Open ds_Sv from disk Zarr
        print(f"Calculating MVBS from Sv file {zarr_input_dir}")
        ds_Sv = xr.open_zarr(zarr_input_dir)

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
        leftover_ds_Sv = concat_ds_Sv.isel(ping_time=slice(cutoff_index, -1))

        # Compute MVBS on current subset
        ds_MVBS = ep.commongrid.compute_MVBS(
            complete_bins_Sv,
            range_var="depth",
            range_bin=range_bin,
            ping_time_bin=ping_bin,
        )

        # Accumulate MVBS in memory
        mvbs_parts.append(ds_MVBS)

    return mvbs_parts


def plot(mvbs: list[xr.Dataset]):
    """
    Plots Mean Volume Backscattering Strength (MVBS) across multiple frequency channels.

    This function visualizes computed MVBS datasets by concatenating them along the
    `ping_time` dimension and plotting Sv data for specific frequency channels. It generates
    separate plots for the 38 kHz and 200 kHz channels, displaying Sv against ping_time and
    depth with specified dB value ranges.

    Parameters:
    mvbs : list[xr.Dataset]
        List of xarray datasets representing computed MVBS data. Each dataset must share
        frequency and depth coordinates.

    Raises:
    RuntimeError
        If no MVBS datasets are provided as input (i.e., `mvbs` is None or empty).
    """
    if not mvbs:
        raise RuntimeError(
            "No MVBS datasets were computed. Check input Sv zarr files and resampling bins.")

    # Concatenate along ping_time; datasets share frequency/depth coords
    # Using data_vars='minimal' semantics is implicit in xr.concat for identical vars
    ds_MVBS = xr.concat(mvbs, dim="ping_time")

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


def parse_args():
    parser = argparse.ArgumentParser(
        description="Process and plot MVBS from Sv Zarr data.",
        usage=usagestr,
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input Sv data directories in Zarr format. Can use glob patterns. Use either positional"
             " arguments, or --root-dir, but not both.",
    )
    parser.add_argument(
        "--root-dir",
        default=None,
        help="Root directory of deployment files. Use either this option, "
             "or positional arguments, but not both.",
    )
    parser.add_argument(
        "--ping-bin",
        default="2s",
        help="Ping bin size for MVBS computation. Default: 2s",
    )
    parser.add_argument(
        "--range-bin",
        default="0.5m",
        help="Range bin size for MVBS computation. Default: 0.5m",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.inputs and args.root_dir:
        sys.exit("Error: Cannot specify both positional arguments and --root-dir.")
    elif args.inputs:
        sv_dirs = find_zarr_dirs(args.inputs)
    elif args.root_dir:
        sv_dirs = find_zarr_dirs([os.path.join(args.root_dir, "sv", "*.sv")])
    else:
        sys.exit("Error: Must specify either positional arguments or --root-dir.")

    if not sv_dirs:
        print("No input Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(sv_dirs)} Zarr directories")

    mvbs = calc_mvbs(zarr_input_dirs=sv_dirs, ping_bin=args.ping_bin, range_bin=args.range_bin)
    plot(mvbs=mvbs)
