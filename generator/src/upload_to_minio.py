"""Upload the historical dataset (any number of days) to MinIO as Bronze.

Layout (identical date partitions to the streaming consumer):

    <prefix>/run_id=<run>/reference/<dim>/part-00000.parquet
    <prefix>/run_id=<run>/readings/year=YYYY/month=MM/day=DD/part-*.parquet
    <prefix>/run_id=<run>/readings/year=YYYY/month=MM/day=DD/_DAY_CLOSED
    <prefix>/run_id=<run>/metadata/manifest.json            (uploaded last)

It uploads the `readings_by_date/dt=YYYY-MM-DD/` folders written by
historical_generator.py.
"""

import argparse
import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv


def find_project_root(start: Path) -> Path:
    start = Path(start).resolve()
    for candidate in (start, *start.parents):
        if any((candidate / marker).exists()
               for marker in (".env", "docker-compose.yml", ".git")):
            return candidate
    return start


PROJECT_ROOT = find_project_root(Path(__file__).parent)
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_DATA_DIR = PROJECT_ROOT / "data" / "generated-100k-6days"


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
            max_pool_connections=32,
        ),
    )


def date_path(day: str) -> str:
    """'2026-07-07' -> 'year=2026/month=07/day=07'."""
    year, month, dd = day.split("-")
    return f"year={year}/month={month}/day={dd}"


def collect_day_files(data_dir: Path, manifest: dict) -> dict:
    """Validate readings_by_date/ against the manifest; return {day: [files]}."""
    by_date_dir = data_dir / "readings_by_date"
    if not by_date_dir.is_dir():
        raise FileNotFoundError(
            f"Missing {by_date_dir}. Re-run historical_generator.py (without "
            "--skip-partition) to create the date-partitioned readings."
        )

    start_date = datetime.strptime(manifest["start_date"], "%Y-%m-%d").date()
    expected_days = [
        (start_date + timedelta(days=i)).isoformat()
        for i in range(manifest["num_days"])
    ]

    found = {
        path.name.removeprefix("dt="): sorted(path.glob("part-*.parquet"))
        for path in sorted(by_date_dir.glob("dt=*"))
        if path.is_dir()
    }
    if sorted(found) != expected_days:
        raise ValueError(
            f"Date partitions {sorted(found)} do not match the manifest "
            f"({expected_days})."
        )

    expected_files = -(-manifest["num_meters"] // manifest["batch_meters"])
    expected_rows = manifest["num_meters"] * manifest["readings_per_meter_per_day"]

    for day, files in found.items():
        if len(files) != expected_files:
            raise ValueError(
                f"{day}: expected {expected_files} files, found {len(files)}."
            )
        rows = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
        if rows != expected_rows:
            raise ValueError(
                f"{day}: expected {expected_rows:,} rows, found {rows:,}."
            )

    return found


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
    parser.add_argument(
        "--workers",
        type=int,
        default=8,
        help="Parallel upload threads.",
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

    day_files = collect_day_files(data_dir, manifest)

    dimension_names = [
        "dim_region",
        "dim_substation",
        "dim_transformer",
        "dim_meter",
    ]

    for name in dimension_names:
        if not (data_dir / f"{name}.parquet").is_file():
            raise FileNotFoundError(f"Missing dimension table: {name}.parquet")

    bucket = os.getenv("S3_BUCKET", "smart-energy")
    base_prefix = os.getenv("GEN_S3_PREFIX", "bronze/historical").strip("/")
    if not base_prefix:
        raise ValueError("GEN_S3_PREFIX must be a non-empty development prefix.")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"run_{timestamp}_{uuid.uuid4().hex[:6]}"
    prefix = f"{base_prefix}/run_id={run_id}"

    total_files = sum(len(files) for files in day_files.values())

    print(f"Dataset: {data_dir}")
    print("Dataset type: synthetic")
    print(f"Meters: {manifest['num_meters']:,}")
    print(f"Days: {manifest['num_days']} ({min(day_files)} .. {max(day_files)})")
    print(f"Expected readings: {manifest['records']['readings']:,}")
    print(f"Reading files: {total_files:,}")
    print(f"Bucket: {bucket}")
    print(f"New upload prefix: {prefix}")

    client = get_s3_client()
    client.head_bucket(Bucket=bucket)

    # Reference dimensions.
    for name in dimension_names:
        key = f"{prefix}/reference/{name}/part-00000.parquet"
        client.upload_file(str(data_dir / f"{name}.parquet"), bucket, key)
        print(f"Uploaded s3://{bucket}/{key}")

    # Readings: one date partition per day, uploaded in parallel.
    jobs = []
    for day, files in day_files.items():
        target = f"{prefix}/readings/{date_path(day)}"
        for path in files:
            jobs.append((path, f"{target}/{path.name}"))

    def upload(job):
        path, key = job
        client.upload_file(str(path), bucket, key)
        return key

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        for count, _ in enumerate(pool.map(upload, jobs), start=1):
            if count % 50 == 0 or count == len(jobs):
                print(f"Uploaded {count:,}/{len(jobs):,} reading files")

    # Per-day completion marker, same name the streaming consumer writes.
    expected_rows = manifest["num_meters"] * manifest["readings_per_meter_per_day"]
    for day in day_files:
        client.put_object(
            Bucket=bucket,
            Key=f"{prefix}/readings/{date_path(day)}/_DAY_CLOSED",
            Body=json.dumps({"dt": day, "expected_rows": expected_rows}).encode(),
        )

    # Upload manifest last so it signals that all dataset objects uploaded.
    client.upload_file(
        str(manifest_path), bucket, f"{prefix}/metadata/manifest.json"
    )

    print("\nUpload complete.")
    print(f"Dataset prefix: s3://{bucket}/{prefix}/")
    print("Keep this prefix for your teammates and project documentation.")


if __name__ == "__main__":
    main()