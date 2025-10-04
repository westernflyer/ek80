"""
A module to calculate Sv from converted echosounder data in Zarr format, using echopype and Dask.

This module calculates calibrated volume backscatter (Sv), and save the
resultant datasets back to disk. The calculation is distributed using Dask to
improve efficiency. The module supports configurable calibration parameters such
as encoding mode, waveform mode, and depth offset.
"""
import argparse
import os.path
import sys
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
from dask.distributed import Client

from utilities import find_zarr_dirs

usagestr = """%(prog)s -h|--help
       %(prog)s [-o SAVE_DIR] [--encode-mode=(complex|power) [--depth-offset=OFFSET] [--waveform-mode=MODE] inputs ...
       %(prog)s [-o SAVE_DIR] [--encode-mode=(complex|power) [--depth-offset=OFFSET] [--waveform-mode=MODE] --deploy-dir DEPLOY_DIR
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)


def calc_all(zarr_dirs: Iterable[Path],
             save_dir: str = "../sv/",
             encode_mode: str = "complex",
             depth_offset: float | int = 1,
             waveform_mode: str = "CW"):

    # Use maximum number of CPUs for Dask Client (defaults)
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse zarr directories, calculate Sv, and save to disk
    open_and_save_futures = []
    for zarr_dir in zarr_dirs:
        # Where to save the converted file
        intermediate_path = zarr_dir.parent / Path(save_dir) / zarr_dir.stem
        save_path = intermediate_path.with_suffix(".sv").resolve()
        open_and_save_future = client.submit(
            calculate_sv,
            zarr_dir=zarr_dir,
            save_path=save_path,
            waveform_mode=waveform_mode,
            encode_mode=encode_mode,
            depth_offset=depth_offset
        )
        open_and_save_futures.append(open_and_save_future)
    client.gather(open_and_save_futures)


def calculate_sv(zarr_dir: Path,
                 save_path: Path,
                 waveform_mode: str = "CW",
                 encode_mode: str = "complex",
                 depth_offset: float | None = 1) -> None:
    print(f"Calculating Sv from {zarr_dir}; saving to {save_path}")
    ed_zarr = ep.open_converted(zarr_dir)

    # Calibrate backscatter measurement to Sv
    ds_Sv = ep.calibrate.compute_Sv(ed_zarr, waveform_mode=waveform_mode, encode_mode=encode_mode)

    # Add depth to the Sv dataset
    ds_Sv = ep.consolidate.add_depth(ds_Sv, depth_offset=depth_offset)

    # Save Sv dataset to Zarr on disk
    ds_Sv.to_zarr(save_path / f"{zarr_dir.stem}_Sv.zarr", mode="w")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate Sv from converted data in Zarr format using echopype.",
        usage=usagestr,)
    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input data directories in Zarr format. Can use glob patterns. Use either positional"
             " arguments, or --deploy-dir, but not both.",
    )
    parser.add_argument(
        "--deploy-dir",
        default=None,
        help="Root directory of deployment files. Use either this option, "
             "or positional arguments, but not both.",
    )
    parser.add_argument(
        "-o",
        "--out",
        dest="save_dir",
        default="../sv/",
        help="Output directory to store Sv zarr files (default: ./sv_zarr under CWD)",
    )
    parser.add_argument(
        "--encode-mode",
        dest="encode_mode",
        choices=["complex", "power"],
        default="complex",
        help="Encoding mode of EK80 data for calibration (default: complex)",
    )
    parser.add_argument(
        "--depth-offset",
        dest="depth_offset",
        type=float,
        default=1.0,
        help="Depth offset (meters) added when computing depth (default: 1.0)",
    )
    parser.add_argument(
        "--waveform-mode",
        dest="waveform_mode",
        choices=["CW", "BB"],
        default="CW",
        help="Waveform mode for calibration (default: CW)",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    if args.inputs and args.deploy_dir:
        sys.exit("Error: Cannot specify both positional arguments and --deploy-dir.")
    elif args.inputs:
        zarr_dirs = find_zarr_dirs(args.inputs)
    elif args.deploy_dir:
        zarr_dirs = find_zarr_dirs([os.path.join(args.deploy_dir, "converted", "*.zarr")])
    else:
        sys.exit("Error: Must specify either positional arguments or --deploy-dir.")

    if not zarr_dirs:
        print("No input Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(zarr_dirs)} Zarr directories")

    calc_all(
        zarr_dirs=zarr_dirs,
        save_dir=args.save_dir,
        encode_mode=args.encode_mode,
        depth_offset=args.depth_offset,
        waveform_mode=args.waveform_mode,
    )
