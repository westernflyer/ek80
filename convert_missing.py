#
#    Copyright (c) 2025 Tom Keffer <tkeffer@gmail.com>
#
#    See the file LICENSE.txt for your rights.
#
"""
For various reasons, not all files get converted. This script hunts down the missing files,
then converts them.
"""
import os
from pathlib import Path

from convert_raw import convert

raw_dir = Path("~/Data/Western_Flyer/baja2025/ek80/").expanduser()
converted_dir = raw_dir / "echodata_zarr"

all_files = raw_dir.glob("*.raw")
# Form a set of file names from the raw files
all_file_names = {Path(f.stem) for f in all_files}
converted_files = converted_dir.glob("*.zarr")
# Form a set of converted file names
converted_file_names = {Path(f.stem) for f in converted_files}
# The difference will be the missing converted files
missing_converted = all_file_names - converted_file_names

print(f"Found {len(all_file_names)} raw files")
print(f"Found {len(converted_file_names)} converted files")
print(f"Found {len(missing_converted)} missing converted files")

if __name__ == "__main__":
    if missing_converted:
        print(f"Missing converted files: {missing_converted}")

        print("Converting missing files")

        raw_to_be_converted = [Path(f).with_suffix(".raw") for f in missing_converted]
        os.chdir(raw_dir)
        convert(raw_files=raw_to_be_converted, out_dir=converted_dir, sonar_model="EK80", use_swap=True)
    else:
        print("All files already converted.")
