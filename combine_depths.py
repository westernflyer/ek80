#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
Quick-and-dirty script to combine and subsample depth data.
"""
from pathlib import Path

import xarray as xr

# Path to the root data directory:
data_dir = Path("/home/tkeffer/Data/Western_Flyer/baja2025/ek80/")

# Path to the directory holding depth data:
depth_dir = data_dir / "depth"
depth_paths = list(depth_dir.glob('*.nc'))

print(f"Found {len(depth_paths)} depth files")

# Read them all
depths = xr.open_mfdataset(
    depth_paths,
    data_vars='minimal',
    coords='minimal',
    combine='by_coords',
)

# Resample to 10-second intervals
depths_resampled = (depths
                    .resample(ping_time='10s')
                    .interpolate('linear'))
# Write
depths_resampled.to_netcdf(data_dir / "depth-10s.nc")

# Create a Pandas DataFrame:
df = depths_resampled.to_dataframe()
# Now export as CSV
df.to_csv(data_dir / "depth-10s.csv")
