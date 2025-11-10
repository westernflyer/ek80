#!/usr/bin/env bash
# Upload one or more files to an S3 prefix, skipping files that already exist there.
#
# Usage:
#   ./upload_to_s3.sh s3://bucket/path/ file1 [file2 ...]
#
# Notes:
# - The first argument must be an S3 URI prefix (bucket + optional path).
# - Each subsequent argument is a local file to upload.
# - If a file with the same basename already exists at the destination, it will be skipped.
# - Requires AWS CLI v2 to be installed and configured (credentials/profile/SSO).

set -uo pipefail

usage() {
  echo "Usage: $0 s3://bucket/path/ file1 [file2 ...]" >&2
}

# Validate arguments
if [[ $# -lt 2 ]]; then
  usage
  exit 1
fi

S3_PREFIX="$1"; shift

# Basic validation of S3 prefix
if [[ ! "$S3_PREFIX" =~ ^s3:// ]]; then
  echo "Error: First argument must be an S3 URI prefix like s3://my-bucket/path/" >&2
  exit 1
fi

# Ensure prefix ends with a single '/'
S3_PREFIX="${S3_PREFIX%/}/"

# Check that aws CLI is available
if ! command -v aws >/dev/null 2>&1; then
  echo "Error: AWS CLI not found. Please install AWS CLI v2 and configure credentials." >&2
  exit 1
fi

# Optionally, uncomment to force an SSO login (customize session name if needed)
aws sso login --sso-session my-sso || true

# Upload loop
RC=0
for file in "$@"; do
  if [[ ! -f "$file" ]]; then
    echo "[SKIP] Not a regular file: $file" >&2
    RC=1
    continue
  fi

  base_name="$(basename -- "$file")"
  dest_uri="${S3_PREFIX}${base_name}"

  # Check existence in S3 (exact key). If exists, skip.
  if aws s3 ls "$dest_uri" >/dev/null 2>&1; then
    echo "[EXISTS] $dest_uri — skipping"
    continue
  fi

  echo "[UPLOAD] $file -> $dest_uri"
  if ! aws s3 cp "$file" "$dest_uri" \
      --cli-read-timeout 0 \
      --cli-connect-timeout 0; then
    echo "[ERROR] Upload failed for: $file" >&2
    RC=1
    continue
  fi

  # Verify upload by re-checking existence
  if aws s3 ls "$dest_uri" >/dev/null 2>&1; then
    echo "[OK] Uploaded: $dest_uri"
  else
    echo "[WARN] Could not verify upload for: $dest_uri" >&2
    RC=1
  fi

done

exit "$RC"
