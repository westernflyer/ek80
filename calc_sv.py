#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
A module to calculate Sv from converted echosounder data in Zarr format, using echopype and Dask.

This module calculates calibrated volume backscatter (Sv) and saves the results to disk. The calculation is
distributed using Dask to improve efficiency. The module supports configurable calibration parameters such as
encoding mode, waveform mode, and depth offset.

"""
import argparse
import sys
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
from dask.distributed import Client

from utilities import find_zarr_dirs

usagestr = """%(prog)s -h|--help
       %(prog)s [--encode-mode={complex|power}] [--depth-offset=OFFSET] \\
                  [--waveform-mode={CW,BB}] [--workers=WORKERS] \\
                  --out-dir=OUT_DIR inputs ...
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
# Ignore large graph dask UserWarnings
warnings.simplefilter("ignore", category=UserWarning)


def calc_all(zarr_dirs: Iterable[Path],
             out_dir: Path | str,
             encode_mode: str = "complex",
             depth_offset: float | int = 1,
             waveform_mode: str = "CW",
             workers=1):

    client = Client(n_workers=workers, threads_per_worker=1)
    print("Dask Client Dashboard:", client.dashboard_link)

    # The directory where the Sv files will be saved
    abs_out_dir = Path(out_dir).expanduser()
    # Make it if it doesn't exist
    abs_out_dir.mkdir(parents=True, exist_ok=True)

    # Parse zarr directories, calculate Sv, and save to disk
    open_and_save_futures = []
    for zarr_dir in zarr_dirs:
        # Final path to save the Sv file
        save_path = (abs_out_dir / zarr_dir.stem).with_suffix(".sv").resolve()
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
                 waveform_mode: str = "cw",
                 encode_mode: str = "complex",
                 depth_offset: float | None = 1) -> None:
    print(f"Calculating Sv from {zarr_dir}; saving to {save_path}", flush=True)
    ed_zarr = ep.open_converted(zarr_dir)

    # Some of the Zarr files can raise an AttributeError
    # with a message: 'NoneType' object has no attribute 'sel'
    # This intercepts the exception and skips the file
    try:
        # Calibrate backscatter measurement to Sv
        ds_Sv = ep.calibrate.compute_Sv(ed_zarr, waveform_mode=waveform_mode, encode_mode=encode_mode)
    except AttributeError as e:
        print(f"Error: {e}")
        print(f"Skipping {zarr_dir}")
        return

    # Add depth to the Sv dataset
    ds_Sv = ep.consolidate.add_depth(ds_Sv, depth_offset=depth_offset)

    # Save Sv dataset in Zarr format
    ds_Sv.to_zarr(save_path, mode="w")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate Sv from converted data in Zarr format using echopype.",
        usage=usagestr,)

    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input .zarr data directories in Zarr format. Can use glob patterns.",
    )
    parser.add_argument(
        "--out-dir",
        required=True,
        help="Output directory. Required.",
    )
    parser.add_argument(
        "--encode-mode",
        dest="encode_mode",
        type=str.lower,
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
        type=str.upper,
        choices=["CW", "BB"],
        default="CW",
        help="Waveform mode for calibration (default: CW)",
    )

    parser.add_argument(
        "--workers",
        dest="workers",
        type=int,
        default=2,
        help="Number of workers for Dask Client (default: 2)",
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = parse_args()
    zarr_dirs = find_zarr_dirs(args.inputs)

    if not zarr_dirs:
        print("No input Zarr directories found.")
        print("Nothing done.")
        sys.exit(0)

    print(f"Found {len(zarr_dirs)} Zarr directories")

    calc_all(
        zarr_dirs=zarr_dirs,
        out_dir=args.out_dir,
        encode_mode=args.encode_mode,
        depth_offset=args.depth_offset,
        waveform_mode=args.waveform_mode,
        workers=args.workers
    )
