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
import shutil
import sys
import time
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
import xarray as xr
from dask.distributed import Client

import utilities

usagestr = """%(prog)s -h|--help
       %(prog)s [--out-dir=OUT_DIR] [--ping-bin=PING-BIN-SIZE] [--range-bin=RANGE-BIN-SIZE] \\
       [--skip-existing] inputs ... 
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


def calc_all(sv_paths: Iterable[Path | str] = None,
             out_dir: Path | str = "../MVBS_zarr/",
             ping_bin: str = "5s",
             range_bin: str = "1.0m",
             skip_existing: bool = False,
             workers=4,
             threads=2, ):
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
        skip_existing: If True, skip existing MVBS files. Do not recalculate.
        workers: Number of workers for Dask Client.
        threads: Number of threads per worker.
    """

    print(f"Subsampling into ping bins of size {ping_bin}, and range bins of size {range_bin}")

    client = Client(n_workers=workers, threads_per_worker=threads)
    print("Dask Client Dashboard:", client.dashboard_link)

    open_and_save_futures = []
    for sv_path in sv_paths:
        # Normalize path
        sv_path = Path(sv_path).expanduser().resolve()
        # The directory where the MVBS files will be saved
        mvbs_out_dir = Path(Path(sv_path).parent / out_dir).expanduser().resolve()
        # The path where the MVBS file will be saved
        mvbs_path = Path(mvbs_out_dir / sv_path.name.replace('_Sv.zarr',
                                                             '_MVBS.zarr')).resolve()
        if skip_existing and mvbs_path.exists():
            print(f"Skipping {mvbs_path} - already exists")
            continue

        # If the output directory doesn't exist, make it
        mvbs_out_dir.mkdir(parents=True, exist_ok=True)

        open_and_save_future = client.submit(
            calculate_mvbs,
            pure=False,
            sv_path=sv_path,
            mvbs_path=mvbs_path,
            ping_bin=ping_bin,
            range_bin=range_bin,
        )
        open_and_save_futures.append(open_and_save_future)
    client.gather(open_and_save_futures)


def calculate_mvbs(sv_path, mvbs_path, ping_bin, range_bin):
    ds_sv = xr.open_zarr(sv_path, consolidated=False)
    try:
        print(f"Starting calculating MVBS for {sv_path}", flush=True)
        # Compute MVBS and save it
        ds_mvbs = ep.commongrid.compute_MVBS(
            ds_sv,
            range_var="depth",
            range_bin=range_bin,
            ping_time_bin=ping_bin,
        )
        print(f"Finished calculating MVBS for {sv_path}", flush=True)
    except ValueError as e:
        print(f"Error calculating MVBS for {sv_path}: {e}")
        raise
    # Remove the directory first:
    shutil.rmtree(mvbs_path, ignore_errors=True)
    # Now save the MVBS dataset
    ds_mvbs.to_zarr(mvbs_path, mode="w", consolidated=False)
    print(f"Saved MVBS to {mvbs_path}", flush=True)
    # clean up
    ds_mvbs.close()
    ds_sv.close()


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
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip existing MVBS files (default: False)",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=4,
        help="Number of workers for Dask Client (default: 4)",
    )
    parser.add_argument(
        "--threads",
        dest="threads",
        type=int,
        default=2,
        help="Number of threads per worker (default: 2)",
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    zarr_dirs = utilities.find_zarr_dirs(args.inputs)
    if not zarr_dirs:
        print("No input Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(zarr_dirs)} Sv Zarr directories")
    if args.skip_existing:
        print(f"Skipping existing MVBS files")
    print(f"Using {args.workers} workers and {args.threads} threads per worker")

    start = time.time()
    calc_all(sv_paths=zarr_dirs,
             out_dir=args.out_dir,
             ping_bin=args.ping_bin,
             range_bin=args.range_bin,
             skip_existing=args.skip_existing,
             workers=args.workers,
             threads=args.threads, )
    stop = time.time()
    print(f"Calculation completed in {stop - start:.2f} seconds")
