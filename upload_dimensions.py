"""
Uploads the 4 dimension tables to MinIO under reference/<table>/part-00000.parquet
(the location the data contract specifies).

Run from the project root (next to docker-compose.yml):
    python upload_dimensions.py                 # uses data/proto
    python upload_dimensions.py --dir data/xyz  # or another folder
"""
import argparse
import os
from pathlib import Path

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

parser = argparse.ArgumentParser()
parser.add_argument("--dir", type=Path, default=Path("data/proto"))
args = parser.parse_args()

bucket = os.getenv("S3_BUCKET", "lakehouse")
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
    aws_access_key_id=os.getenv("S3_ROOT_USER"),
    aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
    region_name="us-east-1",
    config=Config(s3={"addressing_style": "path"}),
)

for name in ["dim_region", "dim_substation", "dim_transformer", "dim_meter"]:
    path = args.dir / f"{name}.parquet"
    if not path.is_file():
        raise FileNotFoundError(f"Not found: {path}. Check the --dir folder.")
    key = f"reference/{name}/part-00000.parquet"
    s3.upload_file(str(path), bucket, key)
    print(f"uploaded {path} -> s3://{bucket}/{key}")