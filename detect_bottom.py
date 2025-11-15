#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Calculate 1-D bottom line from Sv data, using echopype and Dask.

Save the results to netCDF files.
"""
import argparse
import sys
import time
from pathlib import Path
from typing import Iterable

import echopype as ep
import xarray as xr
from dask.distributed import Client

import utilities


def calc_all(zarr_dirs: Iterable[Path | str],
             out_dir: Path | str = "../depth/",
             channel: str = "WBT Mini 278014-7 ES38-18|200-18C_ES",
             threshold: float = -40,
             offset_m: float = 0.5,
             bin_skip_from_surface: int = 300,
             skip_existing: bool = False,
             workers: int = 4,
             threads: int = 1, ):
    client = Client(n_workers=workers, threads_per_worker=threads)
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse zarr directories, calculate seafloor depth, and save to disk
    open_and_save_futures = []
    for zarr_dir in zarr_dirs:
        # The directory where the depth data will be saved
        depth_out_dir = Path(Path(zarr_dir).parent / out_dir).expanduser().resolve()
        # Where to save the depth file
        depth_path = Path(depth_out_dir / zarr_dir.name.replace('_Sv.zarr',
                                                                '_depth.nc')).resolve()
        if skip_existing and depth_path.exists():
            print(f"Skipping {depth_path} - already exists")
            continue

        # If the output directory doesn't exist, make it
        depth_out_dir.mkdir(parents=True, exist_ok=True)

        open_and_save_future = client.submit(
            calc_and_save_bottom_depth,
            pure=False,
            sv_input=zarr_dir,
            save_path=depth_path,
            channel=channel,
            threshold=threshold,
            offset_m=offset_m,
            bin_skip_from_surface=bin_skip_from_surface,
        )
        open_and_save_futures.append(open_and_save_future)
    client.gather(open_and_save_futures)


def calc_and_save_bottom_depth(sv_input: Path | str,
                               save_path: Path | str,
                               channel: str = "WBT Mini 278014-7 ES38-18|200-18C_ES",
                               threshold: float = -40,
                               offset_m: float = 0.5,
                               bin_skip_from_surface: int = 300,
                               ):
    print(f"Calculating bottom depth from {sv_input}", flush=True)
    with xr.open_zarr(sv_input, consolidated=False) as sv_ds:
        sea_floor = ep.mask.detect_seafloor(sv_ds,
                                            'basic',
                                            {
                                                "var_name": "Sv", "channel": channel,
                                                "threshold": threshold,
                                                "offset_m": offset_m,
                                                "bin_skip_from_surface": bin_skip_from_surface
                                            })
        sea_floor.to_netcdf(save_path)
        print(f"Saved bottom depth to {save_path}", flush=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate sea floor depth",
        #    usage=usagestr,
    )
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input Sv .zarr directories. Can use glob patterns. "
             "Make sure they are time contiguous.",
    )
    parser.add_argument(
        "--out-dir",
        default="../depth/",
        help="Output directory. Default is '../depth/'.",
    )
    parser.add_argument(
        "--channel",
        default="WBT Mini 278014-7 ES38-18|200-18C_ES",
        help="Channel name for detection. Default is 'WBT Mini 278014-7 ES38-18|200-18C_ES'.",
    )
    parser.add_argument(
        "--threshold",
        default=-40,
        type=float,
        help="Threshold for detection. Default is -40.",
    )
    parser.add_argument(
        "--offset-m",
        default=0.5,
        type=float,
        help="Offset in meters for detection. Default is 0.5.",
    )
    parser.add_argument(
        "--bin-skip-from-surface",
        default=300,
        type=int,
        help="Number of bins to skip from the surface. Default is 300.",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip existing Sv files (default: False)",
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
        default=1,
        help="Number of threads per worker (default: 1)",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    zarr_dirs = utilities.find_zarr_dirs(args.inputs)
    if not zarr_dirs:
        print("No input Sv Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(zarr_dirs)} Zarr directories")
    print(f"Using {args.workers} workers and {args.threads} threads per worker")
    if args.skip_existing:
        print(f"Skipping existing Sv files")

    start = time.time()

    calc_all(
        zarr_dirs=zarr_dirs,
        out_dir=args.out_dir,
        channel=args.channel,
        threshold=args.threshold,
        offset_m=args.offset_m,
        bin_skip_from_surface=args.bin_skip_from_surface,
        skip_existing=args.skip_existing,
        workers=args.workers,
        threads=args.threads,
    )
    stop = time.time()
    print(f"Calculation completed in {stop - start:.2f} seconds")
