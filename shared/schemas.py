"""Shared PySpark schemas for the Smart Energy Intelligence Platform.

Mirrors docs/schema.md (the contract). If the two disagree, fix this file to match the doc,
and bump SCHEMA_VERSION on any breaking change.

Usage:
    from shared.schemas import READING_RAW
    df = spark.read.schema(READING_RAW).json(path)
"""

from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

SCHEMA_VERSION = 1

# ---- Allowed values (see docs/schema.md) ------------------------------------------------
ANOMALY_TYPES = ["TRANSFORMER_OVERLOAD", "VOLTAGE_DROP", "IRREGULAR_CONSUMPTION"]
CUSTOMER_TYPES = [
    "RESIDENTIAL", "COMMERCIAL", "INDUSTRIAL", "AGRICULTURAL",
    "GOVERNMENT", "PUBLIC_LIGHTING", "OTHER",
]
PHASE_TYPES = ["SINGLE_PHASE", "THREE_PHASE"]
ASSET_STATUS = ["ACTIVE", "INACTIVE"]
SEVERITIES = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
INCIDENT_STATUS = ["OPEN", "INVESTIGATING", "RESOLVED"]

NOMINAL_PHASE_VOLTAGE_V = 220.0  # assumption: phase-to-neutral nominal at the meter


def _s(name, nullable=False):
    return StructField(name, StringType(), nullable)


def _d(name, nullable=False):
    return StructField(name, DoubleType(), nullable)


def _t(name, nullable=False):
    return StructField(name, TimestampType(), nullable)


# ---- Dimensions (bucket prefix: reference/) ---------------------------------------------
DIM_REGION = StructType([_s("region_id"), _s("region_name")])

DIM_SUBSTATION = StructType([
    _s("substation_id"), _s("name"), _s("region_id"), _s("city"),
    _d("primary_kv"), _d("secondary_kv"), _d("capacity_mva"), _s("status"),
])

DIM_TRANSFORMER = StructType([
    _s("transformer_id"), _s("substation_id"), _s("region_id"),
    _d("rated_capacity_kva"), _d("nominal_voltage_v"),
    StructField("install_date", DateType(), False), _s("status"),
])

DIM_METER = StructType([
    _s("meter_id"), _s("transformer_id"), _s("customer_type"),
    _s("tariff_category", nullable=True), _s("phase_type"),
    StructField("install_date", DateType(), False), _s("status"),
])

# ---- Readings ---------------------------------------------------------------------------
# Raw event as it arrives on Kafka / lands in Bronze. event_timestamp stays a string here
# (ISO-8601 UTC with Z) so a malformed value can be caught instead of silently nulled.
READING_RAW = StructType([
    _s("reading_id"),
    _s("event_timestamp"),
    _s("meter_id"),
    _s("transformer_id"),
    _d("voltage_v"),
    _d("active_power_kw"),
    _d("reactive_power_kvar"),
    _d("energy_kwh"),
    _d("power_factor", nullable=True),
    StructField("schema_version", IntegerType(), False),
])

# Silver: typed timestamp plus quality columns.
READING_SILVER = StructType([
    _s("reading_id"),
    _t("event_timestamp"),
    _s("meter_id"),
    _s("transformer_id"),
    _d("voltage_v"),
    _d("active_power_kw"),
    _d("reactive_power_kvar"),
    _d("energy_kwh"),
    _d("power_factor", nullable=True),
    StructField("schema_version", IntegerType(), False),
    _s("quality_flag", nullable=True),
    _t("ingested_at"),
])

# Dead-letter message on ami-readings-dlq.
DLQ_MESSAGE = StructType([
    _s("raw_payload"), _s("error"), _s("source_topic"), _t("failed_at"),
])

# ---- Ground truth (bucket prefix: answer_key/) ------------------------------------------
INJECTED_ANOMALIES = StructType([
    _s("event_id"), _s("anomaly_type"), _s("asset_type"), _s("asset_id"),
    _t("start_time"), _t("end_time"), _s("severity"),
    StructField("affected_meters", ArrayType(StringType()), False),
    _s("scenario_params", nullable=True),
])

# ---- Pipeline output / Gold -------------------------------------------------------------
FACT_INCIDENTS = StructType([
    _s("incident_id"), _s("anomaly_type"), _s("asset_type"), _s("asset_id"), _s("region_id"),
    _t("detected_at"), _t("start_time"), _t("end_time", nullable=True),
    _s("status"), _s("severity"), _s("rule_id"), _d("threshold"), _d("observed_value"),
])

FACT_INCIDENT_EVIDENCE = StructType([_s("incident_id"), _s("reading_id")])

FACT_TRANSFORMER_15MIN = StructType([
    _s("transformer_id"), _t("interval_start"),
    StructField("meters_reporting", IntegerType(), False),
    StructField("meters_expected", IntegerType(), False),
    _d("total_kw"), _d("total_kvar"), _d("est_kva"), _d("loading_pct"),
    _d("avg_voltage_v"), _d("min_voltage_v"),
])

FACT_DAILY_REGION = StructType([
    _s("region_id"), StructField("date", DateType(), False),
    _d("demand_kwh"), _d("reporting_rate"),
    StructField("incident_count", IntegerType(), False),
])

METER_BASELINE = StructType([
    _s("meter_id"), StructField("hour_of_week", IntegerType(), False),
    _d("expected_kwh"), _d("std_kwh"),
])
