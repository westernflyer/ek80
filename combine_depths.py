#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Combine and resample depth files using specified interval, then output results
in both NetCDF and CSV formats.

This script reads depth files, combines them using shared coordinates, and
resamples their data along the ping_time axis according to a specified time
interval. The results are stored in a NetCDF file and a CSV file, placed in the
common directory of the input files.
"""

from pathlib import Path
import argparse
import os

import xarray as xr


def parse_args():
    parser = argparse.ArgumentParser(description="Combine and resample depth files.")
    parser.add_argument(
        "files", nargs="+", help="Input depth files (one or more)"
    )
    parser.add_argument(
        "-r", "--resample", default="120s", help="Resampling interval (default is '120s')"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    depth_paths = sorted(Path(f).expanduser().resolve() for f in args.files)
    print(f"Found {len(depth_paths)} depth files")

    # Read them all
    depths = xr.open_mfdataset(
        [str(p) for p in depth_paths],
        data_vars="minimal",
        coords="minimal",
        combine="by_coords",
    )

    # Resample to requested interval along ping_time
    interval = args.resample
    depths_resampled = depths.resample(ping_time=interval).interpolate("linear")

    # Determine output directory as the common parent of all inputs
    common_dir = Path(os.path.commonpath([str(p.parent) for p in depth_paths]))
    safe_interval = (
        interval.strip().lower().replace(" ", "").replace("/", "-")
    )

    nc_out = common_dir / f"depth-{safe_interval}.nc"
    csv_out = common_dir / f"depth-{safe_interval}.csv"

    # Write NetCDF
    depths_resampled.to_netcdf(nc_out)
    print(f"Wrote {nc_out}")

    # Create a Pandas DataFrame and export as CSV
    df = depths_resampled.to_dataframe()
    df.to_csv(csv_out)
    print(f"Wrote {csv_out}")


if __name__ == "__main__":
    main()
