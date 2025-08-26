"""Script for processing and combining raw sonar files into netCDF format.

This script uses the Echopype library along with Dask to handle the conversion of `.raw` sonar
files into netCDF format. The script is designed to work with EK80 sonar data files.

The Dask package is used to parallelize the conversion and combining processes.

Functions:
    open_and_save(raw_file, sonar_model, use_swap, save_path): Opens an individual raw sonar file
        and saves it as a netCDF object.
    convert(client): Converts all raw sonar files in the specified directory to netCDF format.
"""
import glob
import os.path
import warnings

import echopype as ep
from dask.distributed import Client

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)

EK80_ROOT = "/home/tkeffer/WesternFlyerData/Echosounder_EK80Portable/"
# Which leg to work on:
LEG = "./Leg 3/"
LEG_PATH = os.path.join(EK80_ROOT, LEG)
# Where to save the converted data:
ECHODATA_NETCDF_DIRECTORY = os.path.join(LEG_PATH, './echodata_nc/')


def convert():
    """Convert all the raw sonar files to netCDF."""

    # Use maximum number of CPUs for Dask Client.
    # Set n_workers so that total_RAM / n_workers >= 4 or leave empty and let Dask decide
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    # Gather up a list of all the raw files in the specified directory
    raw_files = glob.glob(os.path.join(LEG_PATH, '*.raw'))
    # Parse EK80 `.raw` file and save to netCDF format
    open_and_save_futures = []
    for raw_file in raw_files:
        open_and_save_future = client.submit(
            open_and_save,
            raw_file=raw_file,
            sonar_model='ek80',
            use_swap=True,
            save_path=ECHODATA_NETCDF_DIRECTORY
        )
        open_and_save_futures.append(open_and_save_future)
    client.gather(open_and_save_futures)


def open_and_save(raw_file, sonar_model, use_swap, save_path):
    """Open and save an EchoData object to netCDF."""
    print(f"Converting {raw_file}")
    try:
        ed = ep.open_raw(
            raw_file=raw_file,
            sonar_model=sonar_model,
            use_swap=use_swap,
        )
        ed.to_netcdf(save_path, overwrite=True, compute=True)
    except Exception as e:
        print("Error with Exception: ", e)
        raise


if __name__ == '__main__':
    convert()
