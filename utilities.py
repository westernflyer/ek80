import glob
import os
from pathlib import Path
from typing import Iterable, List


def find_raw_files(inputs: Iterable[str]) -> List[Path]:
    """
    Find and return a list of raw files from the given input paths. This function processes the
    provided paths, expanding user home directories and environment variables, resolving glob
    patterns, and checking for valid file paths. It logs a warning for non-file or
    non-existent paths.

    Parameters:
        inputs (Iterable[str]): An iterable of input paths or glob patterns to search for raw
        files.

    Returns:
        List[Path]: A sorted list of validated file paths.
    """
    seen = set()
    for inp in inputs:
        # Expand user home and env vars
        inp = os.path.expanduser(os.path.expandvars(inp))
        # Expand glob patterns, then scan for valid files
        for c in glob.glob(inp):
            if os.path.isfile(c):
                seen.add(Path(c))
            else:
                print(f"Warning: {c} is not a file or does not exist. Ignored")
    return sorted(list(seen))

def find_zarr_dirs(inputs: Iterable[str]) -> List[Path]:
    """
    Find Zarr directories within given input paths.

    Scans the provided input paths, expanding any user home, and glob patterns,
    to collect directories representing Zarr files. Any non-directory
    inputs are ignored with a warning.

    Arguments:
        inputs (Iterable[str]): An iterable of path strings to scan.

    Returns:
        List[Path]: A sorted list of unique directories found.

    Raises:
        None
    """
    seen = set()
    for inp in inputs:
        # Expand user home and env vars
        expanded = os.path.expanduser(os.path.expandvars(inp))
        # Expand glob patterns, then scan for valid directories
        for c in glob.glob(expanded):
            p = Path(c)
            if p.is_dir():
                seen.add(p)
            else:
                print(f"Warning: {p} is not a directory or does not exist. Ignored")
    return sorted(list(seen))

def find_deploy_members(deploy_id : Path|str) -> Iterable[Path]:
    """
    Find Zarr hierarchies with the given deployment ID prefix.
    """
    # Convert any possible string to a Path. Resolve '~'.
    deploy_id = Path(deploy_id).expanduser()
    return deploy_id.parent.glob(f"{deploy_id.stem}*.zarr")
