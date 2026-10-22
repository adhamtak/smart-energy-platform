# Smart Energy Intelligence Platform

SIC 8 Big Data graduation project: simulated AMI smart-meter readings → Kafka → Spark → lakehouse → dashboard,
with detection of transformer overload, persistent voltage drop, and irregular consumption.

> All data is **synthetic**. Nothing here is real EEHC telemetry.

## Quick start

Requires Docker with Compose v2.

```bash
cp .env.example .env        # then edit S3_ROOT_PASSWORD
docker compose up -d
docker compose ps           # minio, kafka, kafka-ui should be running/healthy
docker compose logs kafka-init minio-init   # both should end with a listing and exit 0
```

| What | Where |
|---|---|
| MinIO web console | http://localhost:9001 (`S3_ROOT_USER` / `S3_ROOT_PASSWORD`) |
| S3 API | http://localhost:9000 |
| Kafka UI | http://localhost:8081 |
| Kafka from your laptop | `localhost:9092` |

Stop with `docker compose down`. Add `-v` to also wipe all stored data.

## Connecting from your own containers (Spark, Trino, Airflow ...)

All services share the Docker network `smart-energy-net`. In your own compose file:

```yaml
networks:
  default:
    name: smart-energy-net
    external: true
```

Then use these addresses **inside Docker**:

| | Inside Docker | From your laptop |
|---|---|---|
| Kafka | `kafka:29092` | `localhost:9092` |
| S3 / MinIO | `http://minio:9000` | `http://localhost:9000` |

Spark needs the S3A settings below, and the `hadoop-aws` jar that matches your Spark's Hadoop version.

```python
.config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
.config("spark.hadoop.fs.s3a.access.key", "<S3_ROOT_USER>")
.config("spark.hadoop.fs.s3a.secret.key", "<S3_ROOT_PASSWORD>")
.config("spark.hadoop.fs.s3a.path.style.access", "true")
.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
```

Never commit your `.env`.

## Repo layout

```
docker-compose.yml        shared infrastructure (MinIO, Kafka, Kafka UI)
.env.example              copy to .env; ports, credentials, topic names, generator scale
docker/init/              bucket and topic creation scripts
docs/schema.md            THE data contract: read this first
shared/schemas.py         the same schemas as PySpark StructTypes
generator/                synthetic data generator (next)
```

## Notes

- **MinIO image:** the upstream project stopped publishing free Docker images in Oct 2025 and archived
  its repo in April 2026, so we use Chainguard's free build. The image is set by `S3_IMAGE` in `.env`,
  so it can be swapped for another S3-compatible store (for example RustFS) without code changes.
- **Teammates on different machines** each get their own empty MinIO. The generator is seeded, so everyone
  can regenerate the same 1,000-meter dataset locally. For the 100,000-meter dataset, host one MinIO on one
  machine and share access over a VPN such as Tailscale.
- **Windows:** `.gitattributes` keeps shell scripts on LF line endings so they run inside Linux containers.
