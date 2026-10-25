"""
Shows the test readings (reading_id starting with 'T') that reached Bronze.

Run from the project root, after the streaming job has processed the test messages:
    python check_test_rows.py
    python check_test_rows.py --day 2026-12-01     # another test day
"""
import argparse
import io
import os

import boto3
import pandas as pd
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--day", default="2026-12-01", help="event day, YYYY-MM-DD")
parser.add_argument("--transformer", default="TX_NCAI_0001")
parser.add_argument("--rated", type=float, default=500.0, help="rated capacity in kVA")
args = parser.parse_args()
year, month, day = args.day.split("-")

bucket = os.getenv("S3_BUCKET", "lakehouse")
bronze = os.getenv("BRONZE_PATH", f"s3a://{bucket}/bronze/smart_meter_readings/")
base = bronze.replace(f"s3a://{bucket}/", "").rstrip("/")
prefix = f"{base}/year={year}/month={month}/day={day}/"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
    aws_access_key_id=os.getenv("S3_ROOT_USER"),
    aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

frames = []
paginator = s3.get_paginator("list_objects_v2")
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        if obj["Key"].endswith(".parquet"):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            frames.append(pd.read_parquet(io.BytesIO(body)))

if not frames:
    print(f"No parquet files under s3://{bucket}/{prefix}")
    raise SystemExit(0)

df = pd.concat(frames, ignore_index=True)
tests = df[df["reading_id"].astype(str).str.startswith("T")]
print(f"Rows in this day folder: {len(df)} | test rows: {len(tests)}\n")
cols = ["reading_id", "event_timestamp", "meter_id", "voltage_v",
        "active_power_kw", "energy_kwh"]
print(tests[cols].sort_values(["reading_id", "event_timestamp"]).to_string(index=False))
print("\nCount per reading_id (more than 1 means a duplicate got through):")
print(tests["reading_id"].value_counts().sort_index().to_string())

# Loading per 15-minute window for one transformer, using ALL rows (any reading_id).
# This is the same calculation the overload rule does.
tx = df[df["transformer_id"] == args.transformer].copy()
print(f"\nTransformer {args.transformer}: all rows per 15-min window "
      f"(rated {args.rated} kVA)")
if tx.empty:
    print("No rows for this transformer on this day.")
else:
    tx["window"] = pd.to_datetime(tx["event_timestamp"], utc=True).dt.floor("15min")
    g = tx.groupby("window").agg(
        rows=("reading_id", "count"),
        total_kw=("active_power_kw", "sum"),
        total_kvar=("reactive_power_kvar", "sum"),
    )
    g["est_kva"] = (g["total_kw"] ** 2 + g["total_kvar"] ** 2) ** 0.5
    g["loading_pct"] = g["est_kva"] / args.rated * 100
    print(g.round(2).to_string())