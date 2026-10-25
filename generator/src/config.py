"""Shared generator configuration, read from the project's .env file."""

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv


def find_project_root(start: Path) -> Path:
    """Walk upwards to the folder holding .env / docker-compose.yml / .git."""
    start = Path(start).resolve()
    for candidate in (start, *start.parents):
        if any((candidate / marker).exists()
               for marker in (".env", "docker-compose.yml", ".git")):
            return candidate
    return start


PROJECT_ROOT = find_project_root(Path(__file__).parent)
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int
    num_meters: int
    num_days: int
    start_date: date
    meters_per_transformer: int

    # Upload target (must match what upload_to_minio.py uses).
    s3_bucket: str
    s3_prefix: str

    @classmethod
    def from_env(cls) -> "GeneratorConfig":
        return cls(
            seed=int(os.getenv("GEN_SEED", "42")),
            # Small defaults for smoke tests. For the project dataset set
            # GEN_NUM_METERS=100000 and GEN_NUM_DAYS=6 in .env.
            num_meters=int(os.getenv("GEN_NUM_METERS", "10")),
            num_days=int(os.getenv("GEN_NUM_DAYS", "6")),
            start_date=date.fromisoformat(
                os.getenv("GEN_START_DATE", "2026-07-01")
            ),
            meters_per_transformer=int(
                os.getenv("GEN_METERS_PER_TRANSFORMER", "10")
            ),
            s3_bucket=os.getenv("S3_BUCKET", "smart-energy"),
            s3_prefix=os.getenv(
                "GEN_S3_PREFIX", "bronze/historical"
            ).strip("/"),
        )

    def validate(self) -> None:
        if self.num_meters < 1:
            raise ValueError("GEN_NUM_METERS must be at least 1.")

        if self.num_days < 1:
            raise ValueError("GEN_NUM_DAYS must be at least 1.")

        if self.meters_per_transformer < 1:
            raise ValueError(
                "GEN_METERS_PER_TRANSFORMER must be at least 1."
            )

        if not self.s3_bucket:
            raise ValueError("S3_BUCKET cannot be empty.")

        if not self.s3_prefix:
            raise ValueError(
                "GEN_S3_PREFIX cannot be empty; refusing to write "
                "to the bucket root."
            )