import boto3
from pathlib import Path
from botocore.exceptions import ClientError, NoCredentialsError
from typing import List, Set


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
        Get list of files already in the S3 bucket.

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
            self.s3_client.upload_file(str(local_path), self.bucket_name, s3_key)
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


# Example usage
if __name__ == "__main__":
    # Configuration
    BUCKET_NAME = "wff-archive"
    LOCAL_DIRECTORY = Path("~/Data/Western_Flyer/baja2025/ek80/").expanduser()
    S3_PREFIX = "data/raw/Western_Flyer/baja2025/ek80/"  # Optional: prefix in S3 (like a folder)
    S3_REGION = "us-west-2"

    # Option 1: Use default AWS credentials (from ~/.aws/credentials or environment)
    uploader = S3Uploader(bucket_name=BUCKET_NAME, region_name=S3_REGION)

    # Option 2: Provide credentials explicitly
    # uploader = S3Uploader(
    #     bucket_name=BUCKET_NAME,
    #     aws_access_key_id="YOUR_ACCESS_KEY",
    #     aws_secret_access_key="YOUR_SECRET_KEY",
    #     region_name="us-east-1"
    # )

    # Upload files, skipping those that already exist
    results = uploader.upload_directory(
        local_dir=LOCAL_DIRECTORY,
        s3_prefix=S3_PREFIX,
        skip_existing=True
    )

    print(f"\nDone! Total uploaded: {results['uploaded']}")