#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Given a deployment path, this script calculates MVBS for all Sv files.

This script reads Sv data in Zarr format, subsamples the data into ping and
range bins, computes MVBS for each subsample, then concatenates the results.

The processing iteratively handles incomplete bins across files by leveraging
leftover data from prior iterations to ensure consistency in resampling.

Raises:
    RuntimeError: If no MVBS datasets are computed, indicating input files may be misconfigured.
"""
import argparse
import sys
import warnings
from pathlib import Path
from typing import Iterable, Iterator

import echopype as ep
import xarray as xr
from xarray import Dataset

import utilities

usagestr = """%(prog)s -h|--help
       %(prog)s [--ping-bin PING-BIN-SIZE] [--range-bin RANGE-BIN-SIZE] DEPLOY-PREFIX
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


def gen_mvbs(zarr_inputs: Iterable[Path | str] = None,
             ping_bin: str = "1s",
             range_bin: str = "0.5m") -> Iterator[xr.Dataset]:
    """
    Generator that calculates Mean Volume Backscattering Strength (MVBS) from Zarr hierarchies
    of Sv data.

    This function processes a list of Sv Zarr hierarchies, resampling them into user-specified
    ping and range bins, and then calculates the Mean Volume Backscattering Strength (MVBS).
    It handles partial ping bins by carrying over remaining data to the next file.

    The computed MVBS datasets are stored in memory and returned as a list.

    Parameters:
        zarr_inputs: List of Sv Zarr hierarchies to process. It should be sorted by time.
        ping_bin: String specification of the ping bin size for time resampling (e.g., "1s", "5s").
        range_bin: String specification of the range bin size for depth resampling (e.g., "0.5m", "1m").

    Yields:
        xarray.Dataset objects, each containing the computed MVBS for a corresponding
        Sv Zarr hierarchy.
    """

    print(f"Subsampling into ping bins of size {ping_bin}, and range bins of size {range_bin}")

    # Initialize leftover ds_Sv zarr as None
    leftover_ds_sv = None

    # Iterate through all Sv Zarr Paths
    for zarr_input in zarr_inputs:

        # Open ds_Sv from disk Zarr
        print(f"Calculating MVBS for Sv hierarchy {zarr_input}", flush=True)
        ds_sv = xr.open_zarr(zarr_input)

        # Concat leftover Sv with current Sv
        if leftover_ds_sv:
            concat_ds_sv = xr.concat([leftover_ds_sv, ds_sv], dim="ping_time")
        else:
            concat_ds_sv = ds_sv

        # Create a Resample object for subsampling into user-specified ping bins
        resampled_data = concat_ds_sv.resample(ping_time=ping_bin, skipna=True)

        # Determine the start index of the last incomplete bin
        cutoff_index = max(group.start for group in resampled_data.groups.values())

        # Split data into complete and incomplete bins:

        # Take data up to the last complete bin
        complete_bins_sv = concat_ds_sv.isel(ping_time=slice(0, cutoff_index))

        # Keep remaining data for the next iteration
        leftover_ds_sv = concat_ds_sv.isel(ping_time=slice(cutoff_index, -1))

        print(f"Starting calculating MVBS for {zarr_input}", flush=True)
        # Compute MVBS on current subset
        ds_mvbs = ep.commongrid.compute_MVBS(
            complete_bins_sv,
            range_var="depth",
            range_bin=range_bin,
            ping_time_bin=ping_bin,
        )
        print(f"Finished calculating MVBS for {zarr_input}", flush=True)

        yield ds_mvbs


def parse_args():
    parser = argparse.ArgumentParser(
        description="Given a deployment path, process and save MVBS.",
        usage=usagestr,
    )
    parser.add_argument(
        "deploy_prefix",
        nargs=1,
        metavar="DEPLOY-PREFIX",
        help="Path to a deployment ID. "
             "This is something like '~/Data/Western_Flyer/baja2025/ek80/250416WF.'",
    )
    parser.add_argument(
        "--ping-bin",
        default="2s",
        help="Bin size for pings. Default: 2s",
    )
    parser.add_argument(
        "--range-bin",
        default="1.0m",
        help="Bin size for range. Default: 1.0m",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()

    print(f"Processing hierarchies with prefix {args.deploy_prefix[0]}")

    zarr_members = sorted(utilities.find_deploy_members(args.deploy_prefix[0]))
    print([str(z) for z in zarr_members])

    for mvbs in gen_mvbs(zarr_inputs=zarr_members, ping_bin=args.ping_bin, range_bin=args.range_bin):
        print(mvbs)
