#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
This script checks for missing Sv directories, then calculates them.
"""
import os
from pathlib import Path

from calc_sv import calc_all

converted_dir = Path("~/Data/processed/Western_Flyer/baja2025/ek80/").expanduser()
sv_dir = Path("~/Data/processed/Western_Flyer/baja2025/ek80/sv/").expanduser()

if __name__ == "__main__":
    all_converted = converted_dir.glob("*.zarr")
    # Form a set of segment names from the Zarr directories. These will be names such as "250501WF-D20250501-T181250"
    all_segment_names = {Path(f.stem) for f in all_converted}
    sv_dirs = sv_dir.glob("*.sv")
    # Form a set of segment names from the sv directories
    sv_stem_names = {Path(f.stem) for f in sv_dirs}
    # The difference will be the segment names of the missing sv directories. Sort them.
    missing_sv = sorted(all_segment_names - sv_stem_names)

    print(f"Found {len(all_segment_names)} converted Zarr directories", flush=True)
    print(f"Found {len(sv_stem_names)} sv directories", flush=True)
    print(f"Identified {len(missing_sv)} missing Sv segments", flush=True)

    if missing_sv:
        print(f"Missing Sv segments: {[str(d) for d in missing_sv]}", flush=True)

        print("Converting missing segments", flush=True)

        starting_directories = [Path(f).with_suffix(".zarr") for f in missing_sv]
        os.chdir(converted_dir)
        calc_all(zarr_dirs=starting_directories, out_dir=sv_dir)
    else:
        print("All Sv have already been calculated.")
