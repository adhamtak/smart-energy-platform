"""Show what is inside your MinIO buckets, to find where the data really is.

    python backend/where_is_my_data.py
"""
from collections import Counter

from common import BUCKET, discover_layout, s3_client


def main():
    s3 = s3_client()
    buckets = [b["Name"] for b in s3.list_buckets()["Buckets"]]
    print("Buckets:", buckets or "none")
    if BUCKET not in buckets:
        print(f"\nBucket '{BUCKET}' does not exist. Set S3_BUCKET in .env to one of the above.")
        return

    folders, total = Counter(), 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
        for obj in page.get("Contents", []):
            total += 1
            folders["/".join(obj["Key"].split("/")[:-1][:5]) or "(bucket root)"] += 1
    print(f"\nBucket '{BUCKET}' has {total:,} objects. Folders (first 5 levels):")
    for folder, n in sorted(folders.items())[:80]:
        print(f"  {n:>6}  {folder}")

    print("\nThe portal will read:")
    try:
        layout = discover_layout()
    except RuntimeError as error:
        print("  NOTHING:", error)
        return
    for glob in layout["readings"]:
        print("  readings:", glob)
    print("  dim_meter:", layout["dim_meter"] or "not found")


if __name__ == "__main__":
    main()
