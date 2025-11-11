import argparse
import os
import sys
import threading
from pathlib import Path
from typing import Set, Iterable

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

from utilities import find_files


class S3Uploader:
    """
    A class to upload files to S3 bucket with duplicate checking.
    """

    def __init__(self, bucket_name: str, aws_access_key_id: str = None,
                 aws_secret_access_key: str = None, region_name: str = None):
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

    def upload_directory(self, local_dir: Path, s3_prefix: str = '',
                         skip_existing: bool = True) -> dict:
        """
        Upload all files from a directory to S3, optionally skipping existing files.

        Args:
            local_dir: Path to local directory
            s3_prefix: Prefix to add to all S3 keys (e.g., 'data/')
            skip_existing: If True, skip files that already exist in S3

        Returns:
            Dictionary with upload statistics
        """
        if not local_dir.exists() or not local_dir.is_dir():
            print(f"Directory not found: {local_dir}")
            return {'uploaded': 0, 'skipped': 0, 'failed': 0}

        # Get existing files if we want to skip them
        existing_files = set()
        if skip_existing:
            existing_files = self.get_existing_files(prefix=s3_prefix)

        # Get all files in directory (recursively)
        files_to_upload = [f for f in local_dir.rglob('*') if f.is_file()]

        stats = {'uploaded': 0, 'skipped': 0, 'failed': 0}

        print(f"\nProcessing {len(files_to_upload)} files from {local_dir}")
        print("-" * 60)

        for file_path in files_to_upload:
            # Create S3 key preserving directory structure
            relative_path = file_path.relative_to(local_dir)
            s3_key = f"{s3_prefix}{relative_path}".replace('\\', '/')

            # Skip if file already exists
            if skip_existing and s3_key in existing_files:
                print(f"○ Skipped (exists): {relative_path}")
                stats['skipped'] += 1
                continue

            # Upload the file
            if self.upload_file(file_path, s3_key):
                stats['uploaded'] += 1
            else:
                stats['failed'] += 1

        # Print summary
        print("-" * 60)
        print(f"Summary:")
        print(f"  Uploaded: {stats['uploaded']}")
        print(f"  Skipped:  {stats['skipped']}")
        print(f"  Failed:   {stats['failed']}")

        return stats

    def upload_files(self, files: Iterable[Path | str], s3_prefix: str = '',
                     skip_existing: bool = True) -> dict:
        """
        Upload a provided list of files to S3.

        Args:
            files: List of local file paths to upload
            s3_prefix: Prefix to add to all S3 keys (e.g., 'data/')
            skip_existing: If True, skip files that already exist in S3

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
        if skip_existing:
            existing_files = self.get_existing_files(prefix=s3_prefix)

        stats = {'uploaded': 0, 'skipped': 0, 'failed': 0}

        print(f"\nProcessing {len(valid_files)} files")
        print("-" * 60)

        for file_path in valid_files:
            # Use the file name under the provided prefix
            s3_key = f"{s3_prefix}{file_path.name}".replace('\\', '/')

            if skip_existing and s3_key in existing_files:
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
    parser = argparse.ArgumentParser(description="Upload specified files to an S3 bucket.")
    parser.add_argument('files', nargs='+', help='List of file paths to upload')
    parser.add_argument('--bucket', default='wff-archive',
                        help='S3 bucket name (default: wff-archive)')
    parser.add_argument('--prefix', default='data/raw/Western_Flyer/baja2025/ek80/',
                        help='S3 key prefix to prepend to uploaded files (default: data/raw/Western_Flyer/baja2025/ek80/)')
    parser.add_argument('--region', default='us-west-2',
                        help='AWS region for the S3 client (default: us-west-2)')
    parser.add_argument('--no-skip-existing', dest='skip_existing', action='store_false',
                        help='Do not skip files that already exist in S3. Forces all files to be uploaded')
    parser.set_defaults(skip_existing=True)

    args = parser.parse_args()

    uploader = S3Uploader(bucket_name=args.bucket, region_name=args.region)

    files = find_files(args.files)

    results = uploader.upload_files(
        files=files,
        s3_prefix=args.prefix,
        skip_existing=args.skip_existing
    )

    print(f"\nDone! Total uploaded: {results['uploaded']}")
