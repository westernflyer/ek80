#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
A module to calculate Sv from converted echosounder data in Zarr format, using echopype and Dask.

This module calculates calibrated volume backscatter (Sv) and saves the results to disk. The
calculation is distributed using Dask to improve efficiency. The module supports configurable
calibration parameters such as encoding mode, waveform mode, and depth offset.

Through experimentation, I have found a configuration with 4 workers, 1 thread per worker,
works best on my 4-core, 8-thread NUC. Even so, expect to get fatal
"asyncio.exceptions.CancelledError" exceptions after processing 60-80 files. Re-run, but use the
option --skip-existing to avoid re-processing existing files.
"""
import argparse
import sys
import time
import warnings
from pathlib import Path
from typing import Iterable

import echopype as ep
from dask.distributed import Client

from utilities import find_zarr_dirs

usagestr = """%(prog)s -h|--help
       %(prog)s [--out-dir=OUT_DIR] [--encode-mode={complex|power}] [--depth-offset=OFFSET] \\
                  [--waveform-mode={CW,BB}] [--skip-existing] [--workers=WORKERS] [--threads=THREADS]\\
                  inputs ...
"""

warnings.simplefilter("ignore", category=DeprecationWarning)
warnings.simplefilter("ignore", category=UserWarning)
# Suppress Zarr warnings about opening data with unknown metadata consolidation
warnings.simplefilter("ignore", category=RuntimeWarning)

# Suppress UnstableSpecificationWarning. If the class is not directly importable, use a message filter
try:
    from zarr.errors import UnstableSpecificationWarning

    warnings.simplefilter("ignore", category=UnstableSpecificationWarning)
except ImportError:
    warnings.filterwarnings("ignore", message=".*UnstableSpecificationWarning.*")


def calc_all(zarr_dirs: Iterable[Path],
             out_dir: Path | str = "../Sv_zarr/",
             encode_mode: str = "complex",
             depth_offset: float | int = 1,
             skip_existing: bool = True,
             waveform_mode: str = "CW",
             workers: int = 4,
             threads: int = 1):
    client = Client(n_workers=workers, threads_per_worker=threads)
    print("Dask Client Dashboard:", client.dashboard_link)

    # Parse zarr directories, calculate Sv, and save to disk
    open_and_save_futures = []
    for zarr_dir in zarr_dirs:
        # The directory where the Sv files will be saved
        sv_out_dir = Path(Path(zarr_dir).parent / out_dir).expanduser().resolve()
        # Where to save the Sv file
        sv_path = (sv_out_dir / f"{zarr_dir.stem}_Sv.zarr").resolve()
        if skip_existing and sv_path.exists():
            print(f"Skipping {sv_path} - already exists")
            continue

        # If the output directory doesn't exist, make it
        sv_out_dir.mkdir(parents=True, exist_ok=True)

        open_and_save_future = client.submit(
            calculate_sv,
            pure=False,
            zarr_dir=zarr_dir,
            save_path=sv_path,
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
    print(f"Calculating Sv from {zarr_dir}", flush=True)
    ed_zarr = ep.open_converted(zarr_dir)

    # Use only GGA sentences. This avoids errors involving duplicate timestamps.
    ed_zarr["Platform"] = ed_zarr["Platform"].where(
        (ed_zarr["Platform"]["sentence_type"] == "GGA"), drop=True)

    # Some of the Zarr files can raise an AttributeError with a message: 'NoneType' object has no
    # attribute 'sel'. Don't know why this is happening. Intercept the exception and skip
    # the file
    try:
        # Compute Sv
        ds_Sv = ep.calibrate.compute_Sv(ed_zarr, waveform_mode=waveform_mode,
                                        encode_mode=encode_mode)
    except AttributeError as e:
        print(f"Error: {e}")
        print(f"Skipping {zarr_dir}")
        return

    # Add depth and location to the Sv dataset without errors
    ds_Sv = ep.consolidate.add_depth(ds_Sv, depth_offset=depth_offset)
    ds_Sv = ep.consolidate.add_location(ds_Sv, ed_zarr, nmea_sentence="GGA")

    # Save Sv dataset in Zarr format
    ds_Sv.to_zarr(save_path, mode="w", consolidated=False)
    print(f"Saved Sv to {save_path}", flush=True)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Calculate and save Sv from converted .zarr data",
        usage=usagestr, )

    parser.add_argument(
        "inputs",
        nargs="*",
        help="Input converted ,zarr directories. Can use glob patterns.",
    )
    parser.add_argument(
        "--out-dir",
        default="../Sv_zarr/",
        help="Output directory. Default is '../Sv_zarr/'.",
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
        "--skip-existing",
        action="store_true",
        help="Skip existing Sv files (default: False)",
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
        default=1,
        help="Number of threads per worker (default: 1)",
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
    print(f"Using {args.workers} workers and {args.threads} threads per worker")
    if args.skip_existing:
        print(f"Skipping existing Sv files")

    start = time.time()
    calc_all(
        zarr_dirs=zarr_dirs,
        out_dir=args.out_dir,
        encode_mode=args.encode_mode,
        depth_offset=args.depth_offset,
        skip_existing=args.skip_existing,
        waveform_mode=args.waveform_mode,
        workers=args.workers,
        threads=args.threads,
    )
    stop = time.time()
    print(f"Calculation completed in {stop - start:.2f} seconds")
