
import os
from pathlib import Path, PurePosixPath
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

import boto3
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

HOST = os.getenv("DATABRICKS_HOST", "").rstrip("/")
VOLUME = os.getenv(
    "DATABRICKS_VOLUME",
    "/Volumes/workspace/default/smart_energy_landing",
).rstrip("/")

BUCKET = os.getenv("S3_BUCKET", "lakehouse")
PREFIX = os.getenv("GEN_S3_PREFIX", "bronze/historical").strip("/")
TOKEN = os.getenv("DATABRICKS_TOKEN")

S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT_URL",
    f"http://localhost:{os.getenv('S3_API_PORT', '9000')}",
)


def request_api(method, path, body=None):
    request = Request(
        HOST + path,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "application/octet-stream",
        },
    )

    try:
        with urlopen(request, timeout=180) as response:
            return response.status
    except HTTPError as exc:
        if exc.code in (404,):
            return exc.code
        detail = exc.read(1000).decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Databricks API returned HTTP {exc.code}: {detail}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(
            f"Cannot connect to Databricks: {exc.reason}"
        ) from exc


def api_path(endpoint, volume_path):
    return endpoint + quote(volume_path, safe="/=@._-")


def ensure_directory(path):
    status = request_api(
        "PUT",
        api_path("/api/2.0/fs/directories", path.rstrip("/") + "/"),
    )
    if status not in (200, 201, 204):
        raise RuntimeError(
            f"Directory creation failed: {path} (HTTP {status})"
        )


def exists(path):
    status = request_api(
        "HEAD",
        api_path("/api/2.0/fs/files", path),
    )
    if status == 200:
        return True
    if status == 404:
        return False
    raise RuntimeError(
        f"Cannot check destination {path} (HTTP {status})"
    )


def main():
    if not TOKEN:
        raise RuntimeError(
            "DATABRICKS_TOKEN is missing from the environment."
        )
    if not HOST.startswith("https://"):
        raise RuntimeError(
            "Set DATABRICKS_HOST to your HTTPS Databricks workspace URL."
        )
    if not BUCKET or not PREFIX:
        raise RuntimeError("S3_BUCKET and GEN_S3_PREFIX are required.")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=os.getenv("S3_ROOT_USER"),
        aws_secret_access_key=os.getenv("S3_ROOT_PASSWORD"),
        region_name="us-east-1",
    )

    s3.head_bucket(Bucket=BUCKET)

    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=BUCKET,
        Prefix=PREFIX + "/",
    ):
        keys.extend(
            item["Key"]
            for item in page.get("Contents", [])
            if not item["Key"].endswith("/")
        )

    if not keys:
        print(f"No objects found at s3://{BUCKET}/{PREFIX}/")
        return

    # Data files first, then completion markers, then manifests.
    def order(key):
        if key.endswith("/metadata/manifest.json"):
            return 2
        if key.endswith("/_DAY_CLOSED"):
            return 1
        return 0

    keys.sort(key=lambda key: (order(key), key))
    uploaded = 0
    skipped = 0

    for index, key in enumerate(keys, start=1):
        relative = key[len(PREFIX):].lstrip("/")
        destination = f"{VOLUME}/{PREFIX}/{relative}"

        if exists(destination):
            skipped += 1
            print(f"[{index}/{len(keys)}] Already exists: {key}")
            continue

        ensure_directory(str(PurePosixPath(destination).parent))

        response = s3.get_object(Bucket=BUCKET, Key=key)
        try:
            data = response["Body"].read()
        finally:
            response["Body"].close()

        status = request_api(
            "PUT",
            api_path("/api/2.0/fs/files", destination),
            body=data,
        )
        if status not in (200, 201, 204):
            raise RuntimeError(
                f"Upload failed for {key} (HTTP {status})"
            )

        uploaded += 1
        print(f"[{index}/{len(keys)}] Uploaded: {key}")

    print("\nTransfer summary")
    print(f"Source: s3://{BUCKET}/{PREFIX}/")
    print(f"Destination: {VOLUME}/{PREFIX}/")
    print(f"Discovered: {len(keys)}")
    print(f"Uploaded: {uploaded}")
    print(f"Already present: {skipped}")


if __name__ == "__main__":
    main()
