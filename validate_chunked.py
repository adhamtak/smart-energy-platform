
import argparse
import json
from pathlib import Path

import pandas as pd


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    parser = argparse.ArgumentParser(
        description="Validate chunked synthetic AMI Parquet data."
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path("data/generated-chunk-test"),
    )
    args = parser.parse_args()
    data_dir = args.data_dir

    manifest_path = data_dir / "manifest.json"
    require(manifest_path.exists(), f"Missing {manifest_path}")

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    require(
        manifest.get("data_type") == "synthetic",
        "Dataset must be labelled synthetic.",
    )

    meters = pd.read_parquet(data_dir / "dim_meter.parquet")
    transformers = pd.read_parquet(data_dir / "dim_transformer.parquet")
    substations = pd.read_parquet(data_dir / "dim_substation.parquet")
    regions = pd.read_parquet(data_dir / "dim_region.parquet")

    require(meters["meter_id"].is_unique, "Duplicate meter IDs.")
    require(
        transformers["transformer_id"].is_unique,
        "Duplicate transformer IDs.",
    )
    require(
        substations["substation_id"].is_unique,
        "Duplicate substation IDs.",
    )
    require(regions["region_id"].is_unique, "Duplicate region IDs.")

    require(
        set(meters["transformer_id"]).issubset(
            set(transformers["transformer_id"])
        ),
        "A meter references a missing transformer.",
    )
    require(
        set(transformers["substation_id"]).issubset(
            set(substations["substation_id"])
        ),
        "A transformer references a missing substation.",
    )
    require(
        set(transformers["region_id"]).issubset(set(regions["region_id"])),
        "A transformer references a missing region.",
    )

    expected_meters = manifest["num_meters"]
    expected_days = manifest["num_days"]
    expected_per_meter = expected_days * 96
    expected_total = expected_meters * expected_per_meter

    require(len(meters) == expected_meters, "Meter count mismatch.")
    require(
        manifest["records"]["readings"] == expected_total,
        "Manifest reading count is inconsistent.",
    )

    files = sorted((data_dir / "readings").glob("part-*.parquet"))
    require(files, "No readings/part-*.parquet files found.")

    meter_to_transformer = dict(
        zip(meters["meter_id"], meters["transformer_id"])
    )
    seen_meters = set()
    total_rows = 0
    start = pd.Timestamp(manifest["start_date"], tz="UTC")
    end = start + pd.Timedelta(days=expected_days) - pd.Timedelta(minutes=15)

    required_columns = {
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

    for number, file_path in enumerate(files, start=1):
        df = pd.read_parquet(file_path)

        require(
            required_columns.issubset(df.columns),
            f"Missing columns in {file_path.name}.",
        )
        require(
            df["reading_id"].is_unique,
            f"Duplicate reading IDs in {file_path.name}.",
        )

        require(
            not df["meter_id"].isin(seen_meters).any(),
            f"A meter appears in more than one batch: {file_path.name}.",
        )
        seen_meters.update(df["meter_id"].unique())

        require(
            df["meter_id"].isin(meter_to_transformer).all(),
            f"Unknown meter ID in {file_path.name}.",
        )

        expected_transformers = df["meter_id"].map(meter_to_transformer)
        require(
            df["transformer_id"].eq(expected_transformers).all(),
            f"Meter/transformer mismatch in {file_path.name}.",
        )

        timestamps = pd.to_datetime(
            df["event_timestamp"], utc=True, errors="coerce"
        )
        require(timestamps.notna().all(), "Invalid event timestamp.")
        require(
            timestamps.dt.minute.mod(15).eq(0).all()
            and timestamps.dt.second.eq(0).all(),
            "Timestamps are not aligned to 15-minute intervals.",
        )

        require(
            timestamps.ge(start).all() and timestamps.le(end).all(),
            f"Timestamp outside expected period in {file_path.name}.",
        )

        for column in [
            "voltage_v",
            "active_power_kw",
            "reactive_power_kvar",
            "energy_kwh",
            "power_factor",
        ]:
            require(
                pd.to_numeric(df[column], errors="coerce").notna().all(),
                f"Invalid numeric values in {column}.",
            )

        require((df["voltage_v"] > 0).all(), "Non-positive voltage.")
        require((df["active_power_kw"] >= 0).all(), "Negative active power.")
        require(
            (df["reactive_power_kvar"] >= 0).all(),
            "Negative reactive power.",
        )
        require((df["energy_kwh"] >= 0).all(), "Negative energy.")
        require(
            (df["power_factor"] > 0).all()
            and (df["power_factor"] <= 1).all(),
            "Power factor outside (0, 1].",
        )
        require(
            df["energy_kwh"].sub(df["active_power_kw"] * 0.25).abs()
            .le(0.00011).all(),
            "Energy does not match active power × 0.25 hours.",
        )
        require(
            df["schema_version"].eq(1).all(),
            "Unexpected schema version.",
        )

        df["_event_time"] = timestamps
        per_meter = df.groupby("meter_id").agg(
            count=("reading_id", "size"),
            unique_times=("_event_time", "nunique"),
            first_time=("_event_time", "min"),
            last_time=("_event_time", "max"),
        )

        require(
            per_meter["count"].eq(expected_per_meter).all(),
            f"Incorrect readings per meter in {file_path.name}.",
        )
        require(
            per_meter["unique_times"].eq(expected_per_meter).all(),
            f"Duplicate/missing time intervals in {file_path.name}.",
        )
        require(
            per_meter["first_time"].eq(start).all()
            and per_meter["last_time"].eq(end).all(),
            f"Unexpected time coverage in {file_path.name}.",
        )

        total_rows += len(df)
        print(f"PASS {number}/{len(files)}: {file_path.name} ({len(df):,} rows)")

    require(
        len(seen_meters) == expected_meters,
        f"Expected {expected_meters:,} meters across reading files, "
        f"found {len(seen_meters):,}.",
    )
    require(
        total_rows == expected_total,
        f"Expected {expected_total:,} readings, found {total_rows:,}.",
    )

    print("\nVALIDATION PASSED")
    print(f"Meters: {len(seen_meters):,}")
    print(f"Reading files: {len(files):,}")
    print(f"Total readings: {total_rows:,}")
    print(f"Period: {start} -> {end}")
    print("Dataset type: synthetic")


if __name__ == "__main__":
    main()
