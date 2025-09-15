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
import glob
import os
import os.path
import warnings
from typing import Iterable, List

import echopype as ep
from dask.distributed import Client

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)


def find_raw_files(inputs: Iterable[str]) -> List[str]:
    """
    Find and return a list of raw files from the given input paths. This function processes the
    provided paths, expanding user home directories and environment variables, resolving glob
    patterns, and checking for valid file paths. It logs a warning for non-file or
    non-existent paths.

    Parameters:
        inputs (Iterable[str]): An iterable of input paths or glob patterns to search for raw
        files.

    Returns:
        List[str]: A sorted list of validated file paths.
    """
    seen = set()
    for inp in inputs:
        # Expand user home and env vars
        inp = os.path.expanduser(os.path.expandvars(inp))
        # Expand glob patterns, then scan for valid files
        for c in glob.glob(inp):
            if os.path.isfile(c):
                seen.add(c)
            else:
                print(f"Warning: {c} is not a file or does not exist. Ignored")
    return sorted(list(seen))


def convert(inputs: Iterable[str],
            save_dir: str | None = None,
            sonar_model: str = "ek80",
            use_swap: bool = True):
    """
    Converts a list of `.raw` files into zarr format and saves them in a given or default directory.

    This function identifies `.raw` files from the given inputs and processes them asynchronously. If no
    `.raw` files are found, it exits with a notification. The function employs Dask to manage parallel
    processing. It creates the output directory if it does not exist. The files are then converted and saved
    to the specified or automatically determined directory.

    Parameters:
    inputs: Iterable[str]
        A collection of file paths or directories to search for `.raw` files.
    save_dir: str | None, optional
        The directory where converted files will be saved. If not provided, defaults to a folder named
        `echodata_zarr` in the current working directory.
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

    # Determine default save directory if not provided
    if save_dir is None:
        # With directories ignored, default to CWD
        base_dir = os.getcwd()
        save_dir = os.path.abspath(os.path.join(base_dir, "echodata_zarr"))
    else:
        save_dir = os.path.abspath(os.path.expanduser(os.path.expandvars(save_dir)))

    os.makedirs(save_dir, exist_ok=True)

    # Use maximum number of CPUs for Dask Client (defaults)
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse `.raw` file and save to zarr format
    open_and_save_futures = []
    for raw_file in raw_files:
        open_and_save_future = client.submit(
            open_and_save,
            raw_file=raw_file,
            sonar_model=sonar_model,
            use_swap=use_swap,
            save_path=save_dir
        )
        open_and_save_futures.append(open_and_save_future)
    client.gather(open_and_save_futures)


def open_and_save(raw_file, sonar_model, use_swap, save_path):
    """Open and save an EchoData object to zarr."""
    print(f"Converting {raw_file}")
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
        help="Input .raw files or glob patterns (directories are ignored)",
    )
    parser.add_argument(
        "-o",
        "--out",
        dest="save_dir",
        default=None,
        help="Output directory to store netCDF files (default: ./echodata_zarr under input dir)",
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
