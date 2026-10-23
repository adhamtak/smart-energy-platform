import os
from collections import Counter
from io import BytesIO
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")

RUN_ID = "20261023T124246Z"
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = f"bronze/streaming/run_id={RUN_ID}/"

REQUIRED_COLUMNS = {
    "reading_id",
    "event_timestamp",
    "meter_id",
    "transformer_id",
    "voltage_v",
    "active_power_kw",
    "reactive_power_kvar",
    "energy_kwh",
    "power_factor",
    "schema_version",
}

endpoint = f"http://localhost:{os.getenv('S3_API_PORT', '9000')}"

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=os.getenv("S3_ROOT_USER"),
    aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
    region_name="us-east-1",
)

print(f"Validating s3://{BUCKET}/{PREFIX}")

objects = []
paginator = s3.get_paginator("list_objects_v2")

for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
    objects.extend(
        obj for obj in page.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    )

if not objects:
    raise SystemExit("FAIL: No Parquet files found for this run ID.")

print(f"Parquet files found: {len(objects):,}")

total_rows = 0
missing_columns = set()
null_counts = Counter()
duplicate_ids = 0
seen_ids = set()
invalid_timestamps = 0
invalid_numeric_counts = Counter()
schemas = {}
min_timestamp = None
max_timestamp = None
all_timestamp_values = set()
file_errors = []

numeric_columns = [
    "voltage_v",
    "active_power_kw",
    "reactive_power_kvar",
    "energy_kwh",
    "power_factor",
]

for index, obj in enumerate(
    sorted(objects, key=lambda item: item["Key"]), start=1
):
    key = obj["Key"]

    try:
        response = s3.get_object(Bucket=BUCKET, Key=key)
        df = pd.read_parquet(BytesIO(response["Body"].read()))

        total_rows += len(df)
        schemas[str(df.dtypes.to_dict())] = schemas.get(
            str(df.dtypes.to_dict()), 0
        ) + 1

        missing_columns.update(REQUIRED_COLUMNS - set(df.columns))

        for col in REQUIRED_COLUMNS.intersection(df.columns):
            null_counts[col] += int(df[col].isna().sum())

        if "reading_id" in df.columns:
            ids = df["reading_id"].dropna().astype(str)
            duplicate_ids += int(ids.duplicated().sum())

            for reading_id in ids:
                if reading_id in seen_ids:
                    duplicate_ids += 1
                else:
                    seen_ids.add(reading_id)

        if "event_timestamp" in df.columns:
            ts = pd.to_datetime(
                df["event_timestamp"], utc=True, errors="coerce"
            )
            invalid_timestamps += int(ts.isna().sum())
            valid_ts = ts.dropna()

            if not valid_ts.empty:
                file_min = valid_ts.min()
                file_max = valid_ts.max()

                min_timestamp = (
                    file_min if min_timestamp is None
                    else min(min_timestamp, file_min)
                )
                max_timestamp = (
                    file_max if max_timestamp is None
                    else max(max_timestamp, file_max)
                )

                all_timestamp_values.update(
                    valid_ts.astype(str).unique()
                )

        for col in numeric_columns:
            if col in df.columns:
                values = pd.to_numeric(df[col], errors="coerce")
                invalid_numeric_counts[col] += int(
                    (values.isna() & df[col].notna()).sum()
                )
                if col == "power_factor":
                    invalid_numeric_counts["power_factor_outside_-1_to_1"] += int(
                        ((values < -1) | (values > 1)).sum()
                    )
                elif col in {"voltage_v", "energy_kwh"}:
                    invalid_numeric_counts[f"{col}_negative"] += int(
                        (values < 0).sum()
                    )

    except Exception as exc:
        file_errors.append((key, str(exc)))

    if index % 10 == 0 or index == len(objects):
        print(f"Processed {index:,}/{len(objects):,} files")

print("\n========== VALIDATION REPORT ==========")
print(f"Run ID: {RUN_ID}")
print(f"Parquet files: {len(objects):,}")
print(f"Total rows: {total_rows:,}")
print(f"Unique non-null reading IDs: {len(seen_ids):,}")
print(f"Duplicate reading IDs: {duplicate_ids:,}")
print(f"Invalid timestamps: {invalid_timestamps:,}")
print(f"Timestamp range: {min_timestamp} -> {max_timestamp}")
print(f"Distinct timestamps: {len(all_timestamp_values):,}")

print("\nMissing required columns:")
print(sorted(missing_columns) if missing_columns else "None")

print("\nNull counts:")
for col in sorted(REQUIRED_COLUMNS):
    print(f"  {col}: {null_counts[col]:,}")

print("\nNumeric checks:")
for col, count in sorted(invalid_numeric_counts.items()):
    print(f"  {col}: {count:,}")

print("\nSchema variants:")
for schema, count in schemas.items():
    print(f"  {count} file(s): {schema}")

print("\nFile errors:")
if file_errors:
    for key, error in file_errors:
        print(f"  {key}: {error}")
else:
    print("None")

checks = {
    "Parquet files readable": not file_errors,
    "Required columns present": not missing_columns,
    "No duplicate reading IDs": duplicate_ids == 0,
    "No invalid timestamps": invalid_timestamps == 0,
    "No invalid numeric conversions": sum(invalid_numeric_counts.values()) == 0,
}

print("\n========== CHECK RESULTS ==========")
for name, passed in checks.items():
    print(f"{'PASS' if passed else 'FAIL'}: {name}")

print("\nNote: Negative values and power-factor bounds are diagnostic checks.")
print("Review them against the simulator's intended measurement rules.")
