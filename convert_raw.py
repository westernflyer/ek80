#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
A module to convert raw echosounder files into Zarr format using echopype and Dask.

Through experimentation, I have found a configuration with 4 workers, 2 threads per worker,
works well on my 4-core, 8-thread NUC.
"""
import argparse
import sys
import time
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
import echopype.qc
from dask.distributed import Client

from utilities import find_files

usagestr = """%(prog)s -h|--help
       %(prog)s [--out-dir=OUT_DIR] [--sonar-model={ek60|ek80}] [--no-swap] \\
                  [--workers=WORKERS] [--threads=THREADS]\\
                  inputs ...
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)
# Ignore UnstableSpecificationWarning. If the class is not directly importable, use a message filter
try:
    from zarr.errors import UnstableSpecificationWarning

    warnings.simplefilter("ignore", category=UnstableSpecificationWarning)
except ImportError:
    warnings.filterwarnings("ignore", message=".*UnstableSpecificationWarning.*")


def convert(raw_files: Iterable[Path],
            out_dir: Path | str = "./echodata_zarr/",
            sonar_model: str = "ek80",
            use_swap: bool = True,
            workers=4,
            threads=2):
    """
    Converts a list of `.raw` files into zarr format and saves them in a given or default directory.

    The function uses Dask to manage parallel processing. It creates the output
    directory if it does not exist. The files are then converted and saved to
    the output directory

    Parameters:
    raw_files: Iterable[str]
        A collection of `.raw` files.
    out_dir: Path|str, optional
        The directory where converted files will be saved. Default is './echodata_zarr/'.
    sonar_model: str, optional
        The sonar model type to use for the conversion. Default is "ek80".
    use_swap: bool, optional
        Indicates whether swapping is enabled during conversion. Default is True.

    Raises:
        None directly by this function.

    Returns:
        None
    """

    client = Client(n_workers=workers, threads_per_worker=threads)
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse `.raw` file and save to zarr format
    open_and_save_futures = []
    for raw_file in raw_files:
        # The directory where the converted data will be saved
        ed_dir = Path(Path(raw_file).parent / out_dir).expanduser().resolve()
        # If it doesn't exist, make it
        ed_dir.mkdir(parents=True, exist_ok=True)
        # Where to save the converted data
        ed_path = (ed_dir / f"{raw_file.stem}.zarr").resolve()
        open_and_save_future = client.submit(
            open_and_save,
            pure=False,
            raw_file=raw_file,
            sonar_model=sonar_model,
            use_swap=use_swap,
            save_path=ed_path,
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
    # Fix any time glitches in the NMEA data
    if echopype.qc.exist_reversed_time(ed['Platform'], "time1"):
        # Coerce increasing time
        echopype.qc.coerce_increasing_time(ed['Platform'])

    ed.to_zarr(save_path, overwrite=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert Simrad echosounder .raw files to Zarr format using echopype and Dask.",
        usage=usagestr, )

    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input .raw files. Can use glob patterns.",
    )
    parser.add_argument(
        "--out-dir",
        default="./echodata_zarr/",
        help="Output directory. Default is './echodata_zarr/'",
    )
    parser.add_argument(
        "--sonar-model",
        default="ek80",
        type=str.lower,
        choices=["ek60", "ek80"],
        help="Sonar model for echopype.open_raw. (default: ek80)",
    )
    parser.add_argument(
        "--no-swap",
        action="store_true",
        help="Disable use_swap flag when opening raw files (default: enabled)",
    )
    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=4,
        help="Number of workers for Dask Client (default: 4)",
    )
    parser.add_argument(
        "--threads",
        dest="threads",
        type=int,
        default=2,
        help="Number of threads per worker (default: 2)",
    )

    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    raw_files = find_files(args.inputs)

    if not raw_files:
        print("No input .raw files found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(raw_files)} raw files")
    print(f"Using {args.workers} workers and {args.threads} threads per worker")

    start = time.time()
    convert(
        raw_files=raw_files,
        out_dir=args.out_dir,
        sonar_model=args.sonar_model,
        use_swap=not args.no_swap,
    )
    stop = time.time()
    print(f"Conversion completed in {stop - start:.2f} seconds")
