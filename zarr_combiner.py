"""Script for processing and combining raw sonar files into Zarr format.

This script uses the Echopype library along with Dask to handle the conversion of `.raw` sonar
files into Zarr format and subsequently combines the processed Zarr files. The script is designed
to work with EK80 sonar data files.

The Dask package is used to parallelize the conversion and combining processes.

Functions:
    open_and_save(raw_file, sonar_model, use_swap, save_path): Opens an individual raw sonar file
        and saves it as a Zarr object.
    convert(client): Converts all raw sonar files in the specified directory to Zarr format.
    combine(client): Combines individual Zarr files into a single combined Zarr dataset.

Depending on what version of xarray is installed, the `combine` function may raise a FutureWarning.
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
ECHODATA_ZARR_DIRECTORY = os.path.join(LEG_PATH, './echodata_zarr/')
# Where to save the combined data:
ECHODATA_ZARR_COMBINED_PATH = os.path.join(LEG_PATH, './echodata_combined.zarr')


def convert(client):
    """Convert all the raw sonar files to Zarr."""
    raw_files = glob.glob(os.path.join(LEG_PATH, '*.raw'))
    # Parse EK80 `.raw` file and save to Zarr Store
    open_and_save_futures = []
    for raw_file in raw_files:
        open_and_save_future = client.submit(
            open_and_save,
            raw_file=raw_file,
            sonar_model='ek80',
            use_swap=True,
            save_path=ECHODATA_ZARR_DIRECTORY
        )
        open_and_save_futures.append(open_and_save_future)
    open_and_save_futures = client.gather(open_and_save_futures)


def open_and_save(raw_file, sonar_model, use_swap, save_path):
    """Open and save an EchoData object to Zarr."""
    print(f"Converting {raw_file}")
    try:
        ed = ep.open_raw(
            raw_file=raw_file,
            sonar_model=sonar_model,
            use_swap=use_swap,
        )
        ed.to_zarr(save_path, overwrite=True, compute=True)
    except Exception as e:
        print("Error with Exception: ", e)
        raise


def combine(client):
    # Open (lazy-load) Zarr stores containing EchoData Objects, and lazily combine them
    ed_future_list = []
    for converted_file in sorted(glob.glob(os.path.join(ECHODATA_ZARR_DIRECTORY, "*.zarr"))):
        print(f"Loading {converted_file}")
        ed_future = client.submit(
            ep.open_converted,
            converted_raw_path=converted_file,
            chunks={}
        )
        ed_future_list.append(ed_future)
    ed_list = client.gather(ed_future_list)
    ed_combined = ep.combine_echodata(ed_list)

    # Save the combined EchoData object to a new Zarr store
    # The appending operation only happens when relevant data needs to be save to disk
    ed_combined.to_zarr(
        ECHODATA_ZARR_COMBINED_PATH,
        overwrite=True,
        compute=True,
    )


if __name__ == '__main__':
    # Use maximum number of CPUs for Dask Client
    # Set n_workers so that total_RAM / n_workers >= 4
    # or leave empty and let Dask decide
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    convert(client=client)
    combine(client=client)
