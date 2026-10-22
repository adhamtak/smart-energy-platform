#!/bin/sh
# Creates the lakehouse bucket and its top-level prefixes. Safe to run many times.
# Env (set by docker-compose): S3_ROOT_USER, S3_ROOT_PASSWORD, S3_BUCKET
set -eu

ENDPOINT="http://minio:9000"

echo "Waiting for MinIO at ${ENDPOINT} ..."
i=0
until mc alias set local "$ENDPOINT" "$S3_ROOT_USER" "$S3_ROOT_PASSWORD" >/dev/null 2>&1; do
  i=$((i + 1))
  if [ "$i" -ge 30 ]; then
    echo "MinIO is not reachable after 60 seconds" >&2
    exit 1
  fi
  sleep 2
done

mc mb --ignore-existing "local/${S3_BUCKET}"

# S3 has no real folders; an empty .keep object makes each prefix visible in the console.
for prefix in reference bronze silver gold quarantine checkpoints answer_key; do
  : | mc pipe "local/${S3_BUCKET}/${prefix}/.keep"
done

echo "Bucket layout:"
mc ls "local/${S3_BUCKET}"
