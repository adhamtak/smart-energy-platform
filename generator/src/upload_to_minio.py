
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import uuid

import boto3
from botocore.config import Config
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "generated-100k-1day"


def get_s3_client():
    endpoint_url = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    access_key = os.getenv("S3_ROOT_USER")
    secret_key = os.getenv("S3_ROOT_PASSWORD")

    if not access_key or not secret_key:
        raise ValueError(
            "Missing S3_ROOT_USER or S3_ROOT_PASSWORD in project .env."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
        ),
    )


def upload_file(client, bucket, key, path):
    client.upload_file(str(path), bucket, key)
    print(f"Uploaded s3://{bucket}/{key}")


def main():
    parser = argparse.ArgumentParser(
        description="Upload chunked synthetic AMI data to MinIO."
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=DEFAULT_DATA_DIR,
        help="Generated dataset directory.",
    )
    args = parser.parse_args()

    data_dir = args.input_dir.resolve()

    if not data_dir.is_dir():
        raise FileNotFoundError(f"Dataset directory not found: {data_dir}")

    manifest_path = data_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Missing manifest: {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))

    if manifest.get("data_type") != "synthetic":
        raise ValueError("Refusing to upload an unlabelled/non-synthetic dataset.")

    if manifest.get("num_days") != 1:
        raise ValueError(
            "This uploader version expects a one-day dataset. "
            "Do not use it for a multi-day dataset without adding date partitioning."
        )

    start_date = datetime.strptime(
        manifest["start_date"], "%Y-%m-%d"
    ).date()

    reading_files = sorted((data_dir / "readings").glob("part-*.parquet"))
    if not reading_files:
        raise FileNotFoundError(
            f"No readings/part-*.parquet files found in {data_dir}"
        )

    expected_files = (manifest["num_meters"] + manifest["batch_meters"] - 1) // manifest["batch_meters"]
    if len(reading_files) != expected_files:
        raise ValueError(
            f"Expected {expected_files} reading files from the manifest, "
            f"found {len(reading_files)}."
        )

    dimension_names = [
        "dim_region",
        "dim_substation",
        "dim_transformer",
        "dim_meter",
    ]

    for name in dimension_names:
        if not (data_dir / f"{name}.parquet").is_file():
            raise FileNotFoundError(f"Missing dimension table: {name}.parquet")

    bucket = os.getenv("S3_BUCKET", "lakehouse")
    base_prefix = os.getenv(
        "GEN_S3_PREFIX", "dev/historical-generator"
    ).strip("/")

    if not base_prefix:
        raise ValueError("GEN_S3_PREFIX must be a non-empty development prefix.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"run_{timestamp}_{uuid.uuid4().hex[:6]}"
    prefix = f"{base_prefix}/runs/{run_id}"

    print(f"Dataset: {data_dir}")
    print(f"Dataset type: synthetic")
    print(f"Meters: {manifest['num_meters']:,}")
    print(f"Expected readings: {manifest['records']['readings']:,}")
    print(f"Reading files: {len(reading_files):,}")
    print(f"Bucket: {bucket}")
    print(f"New upload prefix: {prefix}")

    client = get_s3_client()
    client.head_bucket(Bucket=bucket)

    # Upload reference dimensions.
    for name in dimension_names:
        path = data_dir / f"{name}.parquet"
        key = f"{prefix}/reference/{name}/part-00000.parquet"
        upload_file(client, bucket, key, path)

    # This dataset contains one simulated day. Each meter batch file
    # covers that day, so all files belong in the same date partition.
    year = start_date.strftime("%Y")
    month = start_date.strftime("%m")
    day = start_date.strftime("%d")

    reading_prefix = (
        f"{prefix}/bronze/smart_meter_readings/"
        f"year={year}/month={month}/day={day}"
    )

    for index, path in enumerate(reading_files):
        key = f"{reading_prefix}/{path.name}"
        upload_file(client, bucket, key, path)

    # Upload manifest last so it signals that all dataset objects uploaded.
    upload_file(
        client,
        bucket,
        f"{prefix}/metadata/manifest.json",
        manifest_path,
    )

    print("\nUpload complete.")
    print(f"Dataset prefix: s3://{bucket}/{prefix}/")
    print("Keep this prefix for your teammates and project documentation.")


if __name__ == "__main__":
    main()
