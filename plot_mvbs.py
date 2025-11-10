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
import sys
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
import matplotlib.pyplot as plt
import xarray as xr

from utilities import find_zarr_dirs

usagestr = """%(prog)s -h|--help
       %(prog)s [--y-limit=Y-LIMIT] inputs ...
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


def plot(mvbs: Iterable[xr.Dataset], y_limit: float = 500):
    """
    Plots Mean Volume Backscattering Strength (MVBS) across multiple frequency channels.

    This function visualizes computed MVBS datasets by concatenating them along the
    `ping_time` dimension and plotting Sv data for specific frequency channels. It generates
    separate plots for the 38 kHz and 200 kHz channels, displaying Sv against ping_time and
    depth with specified dB value ranges.

    Parameters:
    mvbs : Iterable[xr.Dataset]
        Iterable of xarray datasets representing computed MVBS data. Each dataset must share
        frequency and depth coordinates.
    y_limit : float, optional
        Y-axis limit for the plots in meters. Default is 500.
    """

    # Concatenate along ping_time; datasets share frequency/depth coords
    # Using data_vars='minimal' semantics is implicit in xr.concat for identical vars
    ds_mvbs = xr.concat(mvbs, dim="ping_time")

    print("Plot 38 kHz channel...", flush=True)
    (
        ds_mvbs["Sv"]
        .isel(channel=0)  # Select a channel. Channel 0 is 38 kHz
        .plot(x='ping_time', y='depth', yincrease=False, vmin=-80, vmax=-10)
    )
    plt.title("38 kHz")
    plt.ylim(y_limit, 0)  # Set y-axis limits
    plt.show()

    print("Plot 200 kHz channel...", flush=True)
    (
        ds_mvbs["Sv"]
        .isel(channel=1)  # Select a channel. Channel 1 is 200 k
        .plot(x='ping_time', y='depth', yincrease=False, vmin=-60, vmax=-10)
    )
    plt.title("200 kHz")
    plt.ylim(y_limit, 0)  # Set y-axis limits
    plt.show()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot MVBS data.",
        usage=usagestr,
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input MVBS data directories in Zarr format. Can use glob patterns.",
    )
    parser.add_argument(
        "--y-limit",
        default=500,
        type=float,
        help="Y-axis limit in meters. Default is 500.",
    )
    return parser.parse_args()


def gen_mvbs(mvbs_paths: Iterable[Path | str]) -> Iterable[xr.Dataset]:
    for mvbs_path in mvbs_paths:
        yield xr.open_zarr(mvbs_path)

if __name__ == '__main__':
    args = parse_args()
    mvbs_dirs = find_zarr_dirs(args.inputs)

    if not mvbs_dirs:
        print("No input MVBS Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)
    print(f"Found {len(mvbs_dirs)} MVBS Zarr directories")

    mvbs_iterable = gen_mvbs(mvbs_dirs)
    plot(mvbs=mvbs_iterable, y_limit=args.y_limit)
