
# MinIO to Databricks Transfer

## Purpose

Transfer historical batch objects from the local MinIO bucket to a
Databricks Unity Catalog Volume over HTTPS.

The transfer script does not expose MinIO publicly and does not modify
the source objects.

## Requirements

- Windows with CMD
- Python environment and project dependencies installed
- Local MinIO running and accessible
- Access to the configured Databricks workspace
- A Databricks token with permission to write to the target Volume

## Configuration

1. Copy `.env.example` to `.env` if `.env` does not already exist.
2. Configure the MinIO settings in `.env`.
3. Set `DATABRICKS_HOST` and `DATABRICKS_VOLUME` in `.env`.
4. Set `DATABRICKS_TOKEN` as a local environment variable.
5. Never commit `.env` or publish access tokens.

Example token setup in Windows CMD:

```bat
set DATABRICKS_TOKEN=YOUR_TOKEN
```

This applies only to the current CMD window. Do not paste a real token
into source files, documentation, screenshots, or Git commits.

## Run the transfer

From the repository root:

```bat
.venv\Scripts\python.exe -m py_compile scripts\minio_to_databricks.py
.venv\Scripts\python.exe scripts\minio_to_databricks.py
```

The script discovers objects under `GEN_S3_PREFIX` in the configured
MinIO bucket and uploads them to the corresponding path under the
configured Databricks Volume.

Existing destination files are skipped.

## Verify the result

In a Databricks Python notebook, run:

```python
from pathlib import Path

root = Path(
    "/Volumes/workspace/default/smart_energy_landing/bronze/historical"
)
files = [path for path in root.rglob("*") if path.is_file()]

print(f"Files found: {len(files)}")
for path in sorted(files)[:10]:
    print(path.relative_to(root))
```

Adjust the path if `DATABRICKS_VOLUME` is different in your environment.

## Important notes

- Run the generator and MinIO uploader before the transfer if new data
  needs to be created.
- Do not regenerate historical data just to repeat a transfer.
- A successful file transfer does not mean Bronze-to-Silver or
  Silver-to-Gold processing has run.
- Keep credentials and generated data out of Git.
