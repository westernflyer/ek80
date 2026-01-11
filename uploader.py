from __future__ import annotations

import argparse
import os
import sys
import threading
from pathlib import Path
from typing import Set, Iterable

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from utilities import find_files

DEFAULT_S3_PREFIX = "data/Western_Flyer/baja2025/Echosounder_EK80Portable/"


class S3Uploader:
    """
    A class to upload files to S3 bucket with duplicate checking.
    """

    def __init__(self, bucket_name: str,
                 aws_access_key_id: str = None,
                 aws_secret_access_key: str = None,
                 region_name: str = None):
        """
        Initialize S3 uploader.

        Args:
            bucket_name: Name of the S3 bucket
            aws_access_key_id: AWS access key (optional, can use environment variables)
            aws_secret_access_key: AWS secret key (optional, can use environment variables)
            region_name: AWS region (optional)
        """
        self.bucket_name = bucket_name

        # Initialize S3 client
        if aws_access_key_id and aws_secret_access_key:
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
        else:
            # Use default credentials (from environment or AWS config)
            self.s3_client = boto3.client('s3', region_name=region_name)

    def get_existing_files(self, prefix: str = '') -> Set[str]:
        """
        Get set of files already in the S3 bucket.

        Args:
            prefix: Optional prefix to filter files (e.g., 'folder/')

        Returns:
            Set of file keys (paths) in the bucket
        """
        existing_files = set()

        try:
            # Use paginator to handle buckets with many files
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)

            for page in pages:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        existing_files.add(obj['Key'])

            print(f"Found {len(existing_files)} existing files in bucket '{self.bucket_name}'")
            return existing_files

        except ClientError as e:
            print(f"Error accessing bucket: {e}")
            return existing_files
        except NoCredentialsError:
            print("AWS credentials not found!")
            return existing_files

    def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """
        Upload a single file to S3.

        Args:
            local_path: Path to the local file
            s3_key: The key (path) to use in S3

        Returns:
            True if upload successful, False otherwise
        """
        try:
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key,
                                       Callback=ProgressPercentage(local_path))
            print(f"✓ Uploaded: {local_path.name} -> s3://{self.bucket_name}/{s3_key}")
            return True
        except FileNotFoundError:
            print(f"✗ File not found: {local_path}")
            return False
        except ClientError as e:
            print(f"✗ Error uploading {local_path.name}: {e}")
            return False

    def upload_files(self, files: Iterable[Path | str], s3_path: str = '',
                     force_upload: bool = False) -> dict:
        """
        Upload a provided list of files to S3.

        Args:
            files: List of local file paths to upload
            s3_path: Prefix to add to all S3 keys
            force_upload: If True, always upload files, even if they already exist in S3

        Returns:
            Dictionary with upload statistics
        """
        # Normalize to Path objects
        file_paths = [Path(f) for f in files]

        # Filter out non-files with a message
        valid_files = []
        for f in file_paths:
            if f.exists() and f.is_file():
                valid_files.append(f)
            else:
                print(f"✗ Not a file or not found: {f}")

        if not valid_files:
            print("No valid files to upload.")
            return {'uploaded': 0, 'skipped': 0, 'failed': 0}

        existing_files = set()
        if not force_upload:
            existing_files = self.get_existing_files(prefix=s3_path)

        stats = {'uploaded': 0, 'skipped': 0, 'failed': 0}

        print(f"\nProcessing {len(valid_files)} files")
        print("-" * 60)

        for file_path in valid_files:
            # Use the file name under the provided prefix
            s3_key = os.path.join(s3_path, file_path.name)

            if not force_upload and s3_key in existing_files:
                print(f"○ Skipped (exists): {file_path.name}")
                stats['skipped'] += 1
                continue

            if self.upload_file(file_path, s3_key):
                stats['uploaded'] += 1
            else:
                stats['failed'] += 1

        print("-" * 60)
        print("Summary:")
        print(f"  Uploaded: {stats['uploaded']}")
        print(f"  Skipped:  {stats['skipped']}")
        print(f"  Failed:   {stats['failed']}")

        return stats


class ProgressPercentage(object):
    """Print a percentage completion to stdout"""

    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify, assume this is hooked up to a single filename
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


# Example usage
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload specified files to an S3 bucket. "
                                                 "By default, skips files that already exist in the bucket.")
    parser.add_argument('files', nargs='+',
                        help='List of file paths to upload. Can use glob patterns.')
    parser.add_argument('--bucket', default='wff-archive',
                        help='S3 bucket name (default: wff-archive)')
    parser.add_argument('--prefix', default=DEFAULT_S3_PREFIX,
                        help='S3 key prefix to prepend to uploaded files '
                             f'(default: {DEFAULT_S3_PREFIX}')
    parser.add_argument('--suffix', default='raw',
                        help='S3 directory under prefix (default: "raw")')
    parser.add_argument('--region', default='us-west-2',
                        help='AWS region for the S3 client (default: us-west-2)')
    parser.add_argument('--force-upload', action='store_true',
                        help='Force an upload, even if the file already exists in S3', )

    args = parser.parse_args()

    uploader = S3Uploader(bucket_name=args.bucket, region_name=args.region)

    files = find_files(args.files)

    results = uploader.upload_files(
        files=files,
        s3_path=args.prefix + args.suffix,
        force_upload=args.force_upload,
    )

    print(f"\nDone! Total uploaded: {results['uploaded']}")
