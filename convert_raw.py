#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
A module to convert raw echosounder files into Zarr format using echopype and Dask.
"""
import argparse
import os.path
import sys
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
from dask.distributed import Client

from utilities import find_raw_files

usagestr = """%(prog)s -h|--help
       %(prog)s [-o SAVE_DIR] [--sonar-model SONAR_MODEL] [--no-swap] inputs ... --out-dir OUTPUT_DIR
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)


def convert(raw_files: Iterable[Path],
            out_dir: Path | str,
            sonar_model: str = "ek80",
            use_swap: bool = True):
    """
    Converts a list of `.raw` files into zarr format and saves them in a given or default directory.

    The function uses Dask to manage parallel processing. It creates the output
    directory if it does not exist. The files are then converted and saved to
    the output directory

    Parameters:
    raw_files: Iterable[str]
        A collection of `.raw` files.
    out_dir: Path|str
        The directory where converted files will be saved. Required.
    sonar_model: str, optional
        The sonar model type to use for the conversion, defaulting to "ek80".
    use_swap: bool, optional
        Indicates whether swapping is enabled during conversion, defaulting to True.

    Raises:
        None directly by this function.

    Returns:
        None
    """
    # Use maximum number of CPUs for Dask Client (defaults)
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse `.raw` file and save to zarr format
    open_and_save_futures = []
    for raw_file in raw_files:
        # The directory where the converted file will be saved
        abs_out_dir = (raw_file.parent / Path(out_dir)).expanduser()
        # Make it if it doesn't exist
        abs_out_dir.mkdir(parents=True, exist_ok=True)
        # Final path to save the converted file
        save_path = (abs_out_dir / raw_file.stem).with_suffix(".zarr").resolve()
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
        description="Convert EK80 .raw files to Zarr using echopype and Dask.",
        usage=usagestr, )
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input .raw files or glob patterns. Use either positional arguments, "
             "or --root-dir, but not both.",
    )
    parser.add_argument(
        "-o",
        "--out-dir",
        dest="out_dir",
        required=True,
        help="Output directory. Required.",
    )
    parser.add_argument(
        "--root-dir",
        default=None,
        help="Root directory for a deployment. It should contain a directory named 'raw' containing the raw data files. "
             "Use either this option, or positional arguments, but not both.",
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
    if args.inputs and args.root_dir:
        sys.exit("Error: Cannot specify both positional arguments and --root-dir.")
    elif args.inputs:
        raw_files = find_raw_files(args.inputs)
    elif args.root_dir:
        raw_files = find_raw_files([os.path.join(args.root_dir, "raw", "*.raw")])
    else:
        sys.exit("Error: Must specify either positional arguments or --root-dir.")

    if not raw_files:
        print("No input .raw files found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(raw_files)} raw files")

    convert(
        raw_files=raw_files,
        out_dir=args.out_dir,
        sonar_model=args.sonar_model,
        use_swap=not args.no_swap,
    )
