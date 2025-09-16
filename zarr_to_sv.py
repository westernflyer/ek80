"""
Calculate Sv from EK80 processed zarr files.
"""
import argparse
import os
import os.path
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
from dask.distributed import Client

from utilities import find_zarr_files

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)


def calc_all(inputs: Iterable[str],
             save_dir: Path | None = None,
             encode_mode: str = "complex",
             depth_offset: float | int = 1,
             waveform_mode: str = "CW"):
    zarr_dirs = find_zarr_files(inputs)
    if not zarr_dirs:
        print("No .zarr files found for inputs:", list(inputs))
        return

    print(f"Found {len(zarr_dirs)} zarr directories")

    # Determine default save directory if not provided
    if save_dir is None:
        save_dir = (Path.cwd() / "sv_zarr").absolute()
    else:
        save_dir = save_dir.expanduser().absolute()

    os.makedirs(save_dir, exist_ok=True)

    # Use maximum number of CPUs for Dask Client (defaults)
    client = Client()
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse zarr directories, calculate Sv, and save to disk
    open_and_save_futures = []
    for zarr_dir in zarr_dirs:
        open_and_save_future = client.submit(
            calculate_sv,
            zarr_dir=zarr_dir,
            save_path=save_dir,
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
    ed_zarr = ep.open_converted(zarr_dir)

    # Calibrate backscatter measurement to Sv
    ds_Sv = ep.calibrate.compute_Sv(ed_zarr, waveform_mode=waveform_mode, encode_mode=encode_mode)

    # Add depth to the Sv dataset
    ds_Sv = ep.consolidate.add_depth(ds_Sv, depth_offset=depth_offset)

    # Save Sv dataset to Zarr on disk
    ds_Sv.to_zarr(save_path / f"{zarr_dir.stem}_Sv.zarr", mode="w")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate Sv from EK80 zarr datasets using Echopype and Dask.")
    parser.add_argument(
        "inputs",
        nargs="+",
        help="Input zarr directories or glob patterns",
    )
    parser.add_argument(
        "-o",
        "--out",
        dest="save_dir",
        default=None,
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
    calc_all(
        inputs=args.inputs,
        save_dir=args.save_dir,
        encode_mode=args.encode_mode,
        depth_offset=args.depth_offset,
        waveform_mode=args.waveform_mode,
    )
