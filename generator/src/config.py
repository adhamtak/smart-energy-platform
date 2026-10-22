
import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from dotenv import load_dotenv

# Project root: smart-energy-platform/
PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


@dataclass(frozen=True)
class GeneratorConfig:
    seed: int
    num_meters: int
    num_days: int
    start_date: date
    meters_per_transformer: int

    # Keep test output isolated from existing smoke-test data.
    s3_bucket: str
    s3_prefix: str

    @classmethod
    def from_env(cls) -> "GeneratorConfig":
        return cls(
            seed=int(os.getenv("GEN_SEED", "42")),
            num_meters=int(os.getenv("GEN_NUM_METERS", "10")),
            num_days=int(os.getenv("GEN_NUM_DAYS", "2")),
            start_date=date.fromisoformat(
                os.getenv("GEN_START_DATE", "2026-07-01")
            ),
            meters_per_transformer=int(
                os.getenv("GEN_METERS_PER_TRANSFORMER", "10")
            ),
            s3_bucket=os.getenv("S3_BUCKET", "lakehouse"),
            s3_prefix=os.getenv(
                "GEN_S3_PREFIX", "dev/historical-generator"
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
