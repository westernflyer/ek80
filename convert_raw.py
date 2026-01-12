#
#    Copyright (c) 2025-present Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
A module to convert raw echosounder files into Zarr or netCDF format using echopype.
"""
from __future__ import annotations

import argparse
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import Iterable

import echopype as ep
import echopype.qc

from utilities import find_files

usagestr = """%(prog)s -h|--help
       %(prog)s [--format={zarr|nc}] [--out-dir=OUT_DIR] [--sonar-model={ek60|ek80}] \\
                  [--skip-existing] [--no-swap] [--max-workers=MAX_WORKERS] \\
                  inputs ...
"""


def convert(raw_files: Iterable[Path],
            out_format: str = "zarr",
            out_dir: Path | str | None = None,
            sonar_model: str = "ek80",
            skip_existing: bool = False,
            use_swap: bool = True,
            max_workers: int | None = None):
    """
    Converts a list of `.raw` files into zarr or netCDF format, then save them in a
    given or default directory.

    The files are then converted and saved to the output directory

    Parameters:
    raw_files: Iterable[str]
        A collection of `.raw` files.
    out_format: str
        The format to save the converted files as. Can be 'zarr' or 'nc'. Default is 'zarr'.
    out_dir: Path|str, optional
        The directory where converted files will be saved. For zarr, the default
        is '../processed/echodata_zarr/'. For netCDF, the default is '../processed/echodata_nc/'.
    sonar_model: str, optional
        The sonar model type to use for the conversion. Default is "ek80".
    skip_existing: bool, optional
        If a converted file already exists, skip it. Default is False.
    use_swap: bool, optional
        Indicates whether swapping is enabled during conversion. Default is True.
    max_workers: int, optional
        The maximum number of processes to be used. If None, it will default to the number
        of processors on the machine.
    """
    if out_format == "zarr":
        default_dir = "../processed/echodata_zarr/"
    elif out_format == "nc":
        default_dir = "../processed/echodata_nc/"
    else:
        raise ValueError("Invalid format. Must be 'zarr' or 'netCDF'")
    out_dir = out_dir or default_dir

    # Parse `.raw` file and save to zarr or netCDF format
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for raw_file in raw_files:
            # The directory where the converted data will be saved
            ed_dir = Path(Path(raw_file).parent / out_dir).expanduser().resolve()
            # Path to save the converted data
            ed_path = (ed_dir / f"{raw_file.stem}.{out_format}").resolve()
            if skip_existing and ed_path.exists():
                print(f"Skipping {ed_path} - already exists")
                continue

            # If the output directory doesn't exist, make it
            ed_dir.mkdir(parents=True, exist_ok=True)

            # Submit the conversion job to the process pool
            executor.submit(
                open_and_save,
                raw_file=raw_file,
                out_format=out_format,
                sonar_model=sonar_model,
                use_swap=use_swap,
                save_path=ed_path,
            )


def open_and_save(raw_file, out_format, sonar_model, use_swap, save_path):
    """Open, convert, then save an EchoData object to zarr or netCDF format."""
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

    if out_format == 'zarr':
        ed.to_zarr(save_path, overwrite=True, consolidated=False)
    else:
        ed.to_netcdf(save_path, overwrite=True)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Convert Simrad echosounder .raw files to Zarr or netCDF format using"
                    " echopype.",
        usage=usagestr, )

    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input .raw files. Can use glob patterns.",
    )
    parser.add_argument(
        "--format",
        default="zarr",
        choices=["zarr", "nc"],
        help="Output format. Default is 'zarr'.",
    )
    parser.add_argument(
        "--out-dir",
        help="Output directory. Default is '../processed/echodata_[FORMAT]/' where [FORMAT] is "
             "the format specified with --format.",
    )
    parser.add_argument(
        "--sonar-model",
        default="ek80",
        type=str.lower,
        choices=["ek60", "ek80"],
        help="Sonar model. (default: ek80)",
    )
    parser.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip existing converted files (default: False)",
    )
    parser.add_argument(
        "--no-swap",
        action="store_true",
        help="Disable use_swap flag when opening raw files (default: enabled)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        help="The maximum number of processes that can be used. Default is the number of "
             "processors on the machine.",
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

    start = time.time()
    convert(
        raw_files=raw_files,
        out_format=args.format,
        out_dir=args.out_dir,
        sonar_model=args.sonar_model,
        skip_existing=args.skip_existing,
        use_swap=not args.no_swap,
        max_workers=args.max_workers,
    )
    stop = time.time()
    print(f"Conversion completed in {stop - start:.2f} seconds")
