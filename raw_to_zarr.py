"""
This module provides functionality to convert raw sonar files into zarr format using Echopype
and Dask for distributed processing. It includes commands to find valid raw files, perform
conversion, and manage arguments for command-line execution.

The main features include:
- Finding and validating input file paths or patterns.
- Converting raw files into zarr format, with support for distributed computation.
- Command-line parsing for easy execution and configuration.

Module requires Echopype and Dask for its operations.
"""
import argparse
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
from dask.distributed import Client

from utilities import find_raw_files

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)


def convert(inputs: Iterable[Path],
            save_dir: str = "../converted/",
            sonar_model: str = "ek80",
            use_swap: bool = True):
    """
    Converts a list of `.raw` files into zarr format and saves them in a given or default directory.

    This function identifies `.raw` files from the given inputs and processes them asynchronously.
    If no `.raw` files are found, it exits with a notification. The function uses Dask to manage
    parallel processing. It creates the output directory if it does not exist. The files are then
    converted and saved to the output directory

    Parameters:
    inputs: Iterable[str]
        A collection of file paths or directories to search for `.raw` files.
    save_dir: Path, optional
        The directory where converted files will be saved, relative to the original file. Default
        is "../converted/".
    sonar_model: str, optional
        The sonar model type to use for the conversion, defaulting to "ek80".
    use_swap: bool, optional
        Indicates whether swapping is enabled during conversion, defaulting to True.

    Raises:
        None directly by this function.

    Returns:
        None
    """
    raw_files = find_raw_files(inputs)
    if not raw_files:
        print("No .raw files found for inputs:", list(inputs))
        return

    print(f"Found {len(raw_files)} raw files")

    # Use maximum number of CPUs for Dask Client (defaults)
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse `.raw` file and save to zarr format
    open_and_save_futures = []
    for raw_file in raw_files:
        # Where to save the converted file
        intermediate_path = raw_file.parent / Path(save_dir) / raw_file.stem
        save_path = intermediate_path.with_suffix(".zarr").resolve()
        open_and_save_future = client.submit(
            open_and_save,
            raw_file=raw_file,
            sonar_model=sonar_model,
            use_swap=use_swap,
            save_path=save_path,
        )
        open_and_save_futures.append(open_and_save_future)
    client.gather(open_and_save_futures)


def open_and_save(raw_file, sonar_model, use_swap, save_path):
    """Open and save an EchoData object to zarr."""
    print(f"Converting {raw_file}; saving to {save_path}")
    ed = ep.open_raw(
        raw_file=raw_file,
        sonar_model=sonar_model,
        use_swap=use_swap,
    )
    ed.to_zarr(save_path, overwrite=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert EK80 .raw files to zarr using Echopype and Dask.")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Input .raw files or glob patterns",
    )
    parser.add_argument(
        "-o",
        "--out",
        dest="save_dir",
        default="../converted/",
        help="Output directory, relative to original file (default ../converted/)",
    )
    parser.add_argument(
        "--sonar-model",
        default="ek80",
        help="Sonar model for echopype.open_raw (default: ek80)",
    )
    parser.add_argument(
        "--no-swap",
        action="store_true",
        help="Disable use_swap flag when opening raw files (default: enabled)",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    convert(
        inputs=args.inputs,
        save_dir=args.save_dir,
        sonar_model=args.sonar_model,
        use_swap=not args.no_swap,
    )
