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
import time
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
import xarray as xr

import utilities

usagestr = """%(prog)s -h|--help
       %(prog)s [--out-dir=OUT_DIR] [--ping-bin PING-BIN-SIZE] [--range-bin RANGE-BIN-SIZE] inputs ... 
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


def calc_and_save(sv_paths: Iterable[Path | str] = None,
                  out_dir: Path | str = "../MVBS_zarr/",
                  ping_bin: str = "5s",
                  range_bin: str = "1.0m"):
    """
    Calculate Mean Volume Backscattering Strength (MVBS) from Zarr hierarchies
    of Sv data, then save.

    This function processes a list of Sv Zarr hierarchies, resampling them into user-specified
    ping and range bins, and then calculates the Mean Volume Backscattering Strength (MVBS).
    It handles partial ping bins by carrying over remaining data to the next file. It then
    saves the results.

    Parameters:
        sv_paths: List of Sv Zarr hierarchies to process. It should be sorted by time.
        out_dir: Path to the output directory for saving MVBS.
        ping_bin: String specification of the ping bin size for time resampling (e.g., "1s", "5s").
        range_bin: String specification of the range bin size for depth resampling (e.g., "0.5m", "1m").
    """

    print(f"Subsampling into ping bins of size {ping_bin}, and range bins of size {range_bin}")

    # Initialize leftover ds_Sv zarr as None
    leftover_ds_sv = None

    # Iterate through all Sv Zarr Paths
    for sv_path in sv_paths:
        print(f"Calculating MVBS for Sv hierarchy {sv_path}", flush=True)

        # The directory where the MVBS files will be saved
        mvbs_out_dir = Path(Path(sv_path).parent / out_dir).expanduser().resolve()
        # If it doesn't exist, make it
        mvbs_out_dir.mkdir(parents=True, exist_ok=True)
        # The path where the MVBS file will be saved
        mvbs_path = Path(mvbs_out_dir / sv_path.name.replace('_Sv.zarr',
                                                             '_MVBS.zarr')).resolve()
        print(f"MVBS will be saved to {mvbs_path}", flush=True)

        # Open ds_Sv from disk Zarr
        ds_sv = xr.open_zarr(sv_path)

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

        print(f"Starting calculating MVBS for {sv_path}", flush=True)
        # Compute MVBS on current subset
        ds_mvbs = ep.commongrid.compute_MVBS(
            complete_bins_sv,
            range_var="depth",
            range_bin=range_bin,
            ping_time_bin=ping_bin,
        )
        print(f"Finished calculating MVBS for {sv_path}", flush=True)

        ds_mvbs.to_zarr(mvbs_path, mode="w")
        print(f"Saved MVBS to {mvbs_path}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate and save MVBS from Sv .zarr data.",
        usage=usagestr,
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input Sv .zarr directories. Can use glob patterns. "
             "Make sure they are time contiguous.",
    )
    parser.add_argument(
        "--out-dir",
        default="../MVBS_zarr/",
        help="Output directory. Default is '../MVBS_zarr/'.",
    )
    parser.add_argument(
        "--ping-bin",
        default="5s",
        help="Bin size for pings. Default: 5s",
    )
    parser.add_argument(
        "--range-bin",
        default="1.0m",
        help="Bin size for range. Default: 1.0m",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    zarr_dirs = utilities.find_zarr_dirs(args.inputs)
    if not zarr_dirs:
        print("No input Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(zarr_dirs)} Zarr directories")

    print([str(z) for z in zarr_dirs])

    start = time.time()
    calc_and_save(sv_paths=zarr_dirs, out_dir=args.out_dir, ping_bin=args.ping_bin,
                  range_bin=args.range_bin)
    stop = time.time()
    print(f"Calculation completed in {stop - start:.2f} seconds")
