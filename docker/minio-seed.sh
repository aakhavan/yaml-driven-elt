#!/bin/sh
set -eu

# Defaults
MINIO_ALIAS="${MINIO_ALIAS:-minio}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_BUCKET="${MINIO_BUCKET:-olist}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minio}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minio123}"
MAX_RETRIES="${MINIO_READY_RETRIES:-60}"
SLEEP_SECS="${MINIO_READY_SLEEP:-2}"

echo "minio-seed: alias=$MINIO_ALIAS endpoint=$MINIO_ENDPOINT bucket=$MINIO_BUCKET"

# Wait for MinIO to accept connections and for alias to be configured
TRY=0
until mc alias set "$MINIO_ALIAS" "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
  TRY=$((TRY+1))
  if [ "$TRY" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: Could not set mc alias after $TRY attempts. Exiting." >&2
    exit 1
  fi
  echo "MinIO not ready yet (alias set), retry $TRY/$MAX_RETRIES..."
  sleep "$SLEEP_SECS"
done

echo "Alias configured. Waiting for MinIO to respond to 'mc ls'..."
TRY=0
until mc ls "$MINIO_ALIAS" >/dev/null 2>&1; do
  TRY=$((TRY+1))
  if [ "$TRY" -ge "$MAX_RETRIES" ]; then
    echo "ERROR: MinIO did not become responsive after $TRY attempts. Exiting." >&2
    exit 1
  fi
  sleep "$SLEEP_SECS"
done

echo "Creating bucket $MINIO_BUCKET if it does not exist..."
mc mb -p "$MINIO_ALIAS/$MINIO_BUCKET" >/dev/null 2>&1 || true

# Upload CSVs if present
if ls /seed/data/*.csv >/dev/null 2>&1; then
  echo "Uploading CSV files from /seed/data to $MINIO_ALIAS/$MINIO_BUCKET/ ..."
  mc cp --recursive /seed/data/*.csv "$MINIO_ALIAS/$MINIO_BUCKET/"
else
  echo "No CSV files found in /seed/data to upload."
fi

echo "Listing contents of bucket:"
mc ls "$MINIO_ALIAS/$MINIO_BUCKET" || true

echo "Seeding completed."

