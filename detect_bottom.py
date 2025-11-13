#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Calculate 1-D bottom line
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

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=FutureWarning)


def calc_all(sv_inputs: Iterable[Path], ):
    sea_floors_list = []
    for sv_input in sv_inputs:
        print(f"Processing {sv_input}")
        sea_floor = calc_bottom_depth(sv_input)
        sea_floors_list.append(sea_floor)
    sea_floors_dataset = xr.concat(sea_floors_list, dim='ping_time')
    return sea_floors_dataset

def calc_bottom_depth(sv_input: Path | str) -> xr.DataArray:
    sv_ds = xr.open_zarr(sv_input)
    sea_floor = ep.mask.detect_seafloor(sv_ds, 'basic', {
        "var_name": "Sv", "channel": 'WBT Mini 278014-7 ES38-18|200-18C_ES',
        "threshold": -40, "offset_m": 0.5, "bin_skip_from_surface": 300
    })
    return sea_floor


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
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    zarr_dirs = utilities.find_zarr_dirs(args.inputs)
    if not zarr_dirs:
        print("No input Sv Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(zarr_dirs)} Zarr directories")

    print([str(z) for z in zarr_dirs])

    start = time.time()
    sea_floors = calc_all(zarr_dirs)
    stop = time.time()
    print(f"Calculation completed in {stop - start:.2f} seconds")
    print(sea_floors)

    # This will group by 10-second bins and interpolate
    da_resampled = sea_floors.resample(ping_time='10s').interpolate('linear')
    print(da_resampled)