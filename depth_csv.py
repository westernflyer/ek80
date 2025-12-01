#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Process a set of depth files, saving them as a CSV file.

The files are grouped by segment identifier. For example, all depth files for segment 250501WF
would be grouped together. For each segment, all its member depth files are
loaded into memory together, then subsampled at a regular interval (by default, 60 seconds).
Then the values for that segment are written to the CSV file.

The CSV file will contain:
- ping_time: The timestamp of the end of the subsample interval.
- latitude: The latitude at the corresponding ping_time.
- longitude: The longitude at the corresponding ping_time.
- depth: The depth measurement at the corresponding ping_time.
- segment: The segment identifier.
"""

import argparse
import csv
import datetime
import glob

import numpy as np
import xarray as xr

import utilities


def parse_args():
    parser = argparse.ArgumentParser(
        description="Combine and resample depth files. Write as a CSV file.")
    parser.add_argument(
        "files", nargs="+", help="Input depth files (one or more)"
    )
    parser.add_argument(
        "-r", "--resample", default="60s", help="Resampling interval (default is '60s')"
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    depth_paths = utilities.find_files(args.files)

    # Find the unique segments as a set
    segment_set = {depth_path.stem.split("-")[0] for depth_path in depth_paths}
    # Convert to a list and sort
    segments = sorted(list(segment_set))
    print(f"Found {len(depth_paths)} depth files in {len(segments)} unique segments.")

    with (open('depth_output.csv', 'w', newline='') as csvfile):
        fieldnames = ['ping_time', 'latitude', 'longitude', 'depth', 'segment']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        # Process each segment one at a time
        for segment in segments:
            print(f"Processing segment {segment}")
            segment_paths = glob.glob(f"{segment}-*.nc")
            depths = xr.open_mfdataset(
                [str(p) for p in segment_paths],
                data_vars="minimal",
                coords="minimal",
                combine="by_coords",
            )

            # Resample to requested interval along ping_time. Select the last
            # sample in each interval.
            depths_resampled = depths.resample(ping_time=args.resample).last()

            # Iterate over all resampled points and write to CSV
            ping_times = depths_resampled["ping_time"].values
            lats = depths_resampled["latitude"].to_numpy()
            lons = depths_resampled["longitude"].to_numpy()
            deps = depths_resampled["bottom_depth"].to_numpy()

            for i in range(len(ping_times)):
                if lats is None or np.isnan(lats[i]) \
                        or lons is None or np.isnan(lons[i]) \
                        or deps is None or np.isnan(deps[i]):
                    continue

                # ping_times[i] is a numpy.datetime64. When converted to a string, it would show
                # nanosecond precision. We don't need this. Convert to a Python datetime object,
                # then use that. This will give second precision.
                timestamp = datetime.datetime.fromisoformat(str(ping_times[i]))
                writer.writerow({
                    "ping_time": timestamp.isoformat(),
                    "latitude": f"{lats[i]:.3f}",
                    "longitude": f"{lons[i]:.3f}",
                    "depth": f"{deps[i]:.1f}",
                    "segment": segment,
                })
            depths.close()


if __name__ == "__main__":
    main()
