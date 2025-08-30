"""Script for combining a group of netCDF files into one big files.

Depending on what version of xarray is installed, the `combine` function may raise a FutureWarning.

NOTE: This script causes the CPU to run at 100% for a long time and never finishes.
"""
import glob
import os.path
import warnings

import echopype as ep

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)

EK80_ROOT = "/home/tkeffer/WesternFlyerData/Echosounder_EK80Portable/"
# Which leg to work on:
LEG = "./Leg 3/"
LEG_PATH = os.path.join(EK80_ROOT, LEG)
# Where to find the converted data:
ECHODATA_NETCDF_DIRECTORY = os.path.join(LEG_PATH, './echodata_nc/')
# Where to save the combined data:
ECHODATA_NETCDF_COMBINED_PATH = os.path.join(LEG_PATH, './echodata_combined_nc')


def combine():
    nc_paths = sorted(glob.glob(os.path.join(ECHODATA_NETCDF_DIRECTORY, "*.nc")))
    nc_files = []
    for nc_path in nc_paths:
        print(f"Loading {nc_path}")
        nc_files.append(ep.open_converted(nc_path))
    ed_combined = ep.combine_echodata(nc_files)

    # Save the combined EchoData object to a new netCDF store
    result = ed_combined.to_netcdf(
        ECHODATA_NETCDF_COMBINED_PATH,
        overwrite=True,
    )
    print("result= ", result)


if __name__ == '__main__':
    combine()
