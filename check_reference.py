"""
Shows what is stored in MinIO under reference/dim_transformer/ (the table the
streaming job reads at startup) and compares it with the local file.

Run from the project root:
    python check_reference.py
    python check_reference.py --local data/proto/dim_transformer.parquet --transformer TX_NCAI_0001
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
parser.add_argument("--local", default="data/proto/dim_transformer.parquet")
parser.add_argument("--transformer", default="TX_NCAI_0001")
args = parser.parse_args()

bucket = os.getenv("S3_BUCKET", "lakehouse")
prefix = "reference/dim_transformer/"

s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
    aws_access_key_id=os.getenv("S3_ROOT_USER"),
    aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

frames = []
print(f"Files under s3://{bucket}/{prefix}")
for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        print(f"  {obj['Key']}  {obj['Size']} bytes  modified {obj['LastModified']}")
        if obj["Key"].endswith(".parquet"):
            body = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            frames.append(pd.read_parquet(io.BytesIO(body)))

if not frames:
    raise SystemExit("No parquet files found there.")

remote = pd.concat(frames, ignore_index=True)
print(f"\nMinIO copy: {len(remote)} rows, {remote['transformer_id'].nunique()} distinct transformers")
dups = remote["transformer_id"].duplicated().sum()
print(f"Duplicate transformer_id rows in MinIO copy: {dups}")
print(f"\nMinIO copy, {args.transformer}:")
print(remote[remote["transformer_id"] == args.transformer].to_string(index=False))

local = pd.read_parquet(args.local)
print(f"\nLocal file {args.local}: {len(local)} rows, {local['transformer_id'].nunique()} distinct transformers")
print(f"Local file, {args.transformer}:")
print(local[local["transformer_id"] == args.transformer].to_string(index=False))

merged = remote.drop_duplicates("transformer_id").merge(
    local, on="transformer_id", how="outer", suffixes=("_minio", "_local"), indicator=True
)
print("\nTransformers only in one of the two copies:")
print(merged["_merge"].value_counts().to_string())
if "rated_capacity_kva_minio" in merged and "rated_capacity_kva_local" in merged:
    both = merged[merged["_merge"] == "both"]
    diff = both[both["rated_capacity_kva_minio"] != both["rated_capacity_kva_local"]]
    print(f"Transformers with a different rated_capacity_kva: {len(diff)}")
