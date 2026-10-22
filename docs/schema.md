# Data Contract — Smart Energy Intelligence Platform

**Status:** DRAFT v1 for team review · **Owner:** data generation · **Schema version:** `1`

All readings, IDs, asset mappings and injected incidents are **synthetic**. EEHC publications
(Annual Report 2023/24) ground the structure and scale; they do not make simulated data real.
Anything marked **(assumption)** is a configurable engineering assumption, not an official
Egyptian utility standard.

Section 11 lists the decisions the team still has to confirm. Please read it first.

---

## 1. Conventions

| Topic | Rule |
|---|---|
| Timestamps | ISO-8601 **UTC** with a `Z` suffix, e.g. `2026-07-01T18:15:00Z`. Dashboards convert to `Africa/Cairo`. |
| Reading timestamp | The **start** of the 15-minute interval, aligned to :00, :15, :30, :45. |
| Interval | 15 minutes, so 96 expected readings per meter per full day. |
| Energy | `energy_kwh` is energy used **during that interval** (not a cumulative counter). Expect `energy_kwh ≈ active_power_kw × 0.25`. |
| Units | Volts (V), kilowatts (kW), kilovolt-amperes reactive (kvar), kilowatt-hours (kWh), kilovolt-amperes (kVA). |
| IDs | Strings. Formats in section 2. |
| Column names | `snake_case`. |
| Schema version | `schema_version` is an integer in every event. Bump it on any breaking change. |

## 2. Grid hierarchy and dimension tables

```
region (9 distribution companies)
  └── substation
        └── distribution transformer
              └── smart meter
                    └── 15-minute readings
```

Every meter references a valid transformer, and every transformer a valid substation.
The dimensions are small (one Parquet file each) and everyone joins against them.

### dim_region (9 rows)

| Column | Type | Notes |
|---|---|---|
| `region_id` | string | `NCAI`, `SCAI`, `ALX`, `CNL`, `NDLT`, `SDLT`, `BEH`, `MEG`, `UEG` |
| `region_name` | string | The 9 EEHC distribution companies: North Cairo, South Cairo, Alexandria, Canal, North Delta, South Delta, Beheira, Middle Egypt, Upper Egypt |

### dim_substation

| Column | Type | Notes |
|---|---|---|
| `substation_id` | string | `SUB_<REGION>_<nn>`, e.g. `SUB_NCAI_03` |
| `name` | string | |
| `region_id` | string | FK → dim_region |
| `city` | string | |
| `primary_kv` | double | e.g. 66 or 132 |
| `secondary_kv` | double | e.g. 11 or 22 |
| `capacity_mva` | double | |
| `status` | string | `ACTIVE` / `INACTIVE` |

### dim_transformer

| Column | Type | Notes |
|---|---|---|
| `transformer_id` | string | `TX_<REGION>_<nnnn>`, e.g. `TX_NCAI_0007` |
| `substation_id` | string | FK → dim_substation |
| `region_id` | string | FK → dim_region (denormalized) |
| `rated_capacity_kva` | double | Used for loading % |
| `nominal_voltage_v` | double | Low-voltage side, line-to-line: `380` |
| `install_date` | date | |
| `status` | string | `ACTIVE` / `INACTIVE` |

### dim_meter

| Column | Type | Notes |
|---|---|---|
| `meter_id` | string | `MTR<nnnnnn>`, e.g. `MTR000001` (6 digits covers 100,000 meters) |
| `transformer_id` | string | FK → dim_transformer |
| `customer_type` | string | `RESIDENTIAL`, `COMMERCIAL`, `INDUSTRIAL`, `AGRICULTURAL`, `GOVERNMENT`, `PUBLIC_LIGHTING`, `OTHER` (the 7 EEHC subscriber categories) |
| `tariff_category` | string, nullable | Optional label. Not used by any detection rule in v1. |
| `phase_type` | string | `SINGLE_PHASE` / `THREE_PHASE` |
| `install_date` | date | |
| `status` | string | `ACTIVE` / `INACTIVE` |

## 3. Raw reading event

One reading per message. This is the payload on Kafka `ami-readings` and the row in Bronze.

| Field | Type | Required | Notes |
|---|---|---|---|
| `reading_id` | string | yes | Unique and **stable**: `READ_<meter_id>_<YYYYMMDDHHMM>`, e.g. `READ_MTR000001_202607011815`. A replay or retry produces the same ID, so downstream deduplication works. |
| `event_timestamp` | string | yes | Interval start, UTC (section 1) |
| `meter_id` | string | yes | FK → dim_meter |
| `transformer_id` | string | yes | Must equal `dim_meter.transformer_id`. Denormalized so streaming needs no join to key or aggregate. |
| `voltage_v` | double | yes | Phase-to-neutral voltage at the meter connection point (average of phases for three-phase meters). Nominal **220 V** (assumption). It is not a transformer-terminal measurement. |
| `active_power_kw` | double | yes | Average over the interval |
| `reactive_power_kvar` | double | yes | Average over the interval |
| `energy_kwh` | double | yes | Interval energy |
| `power_factor` | double | no | Nullable. Can be derived as kW / √(kW² + kvar²). |
| `schema_version` | int | yes | `1` |

Example:

```json
{
  "reading_id": "READ_MTR000001_202607011815",
  "event_timestamp": "2026-07-01T18:15:00Z",
  "meter_id": "MTR000001",
  "transformer_id": "TX_NCAI_0001",
  "voltage_v": 230.8,
  "active_power_kw": 1.35,
  "reactive_power_kvar": 0.40,
  "energy_kwh": 0.3375,
  "power_factor": 0.959,
  "schema_version": 1
}
```

**Plausibility.** Do not drop out-of-range readings: a low voltage or a high load may be the anomaly
we are looking for. Only physically impossible values (`voltage_v ≤ 0`, negative `active_power_kw` or
`energy_kwh`, since export is not modelled) are invalid. Everything else is flagged in Silver
with a `quality_flag`, never deleted.

**Deliberately bad data (generator option).** Malformed JSON, missing required fields, duplicate
`reading_id`s, late or out-of-order events, and missing readings. Rates are config values.

## 4. Kafka topics

| Topic | Key | Value | Producer → Consumer |
|---|---|---|---|
| `ami-readings` | `transformer_id` (UTF-8) | Reading JSON (section 3) | Generator → streaming job |
| `meter-status` | `meter_id` | Offline / recovering events, if modelled | Generator → streaming job |
| `anomaly-alerts` | `incident_id` | Alert JSON (section 8) | Streaming job → serving / dashboard |
| `ami-readings-dlq` | none | Dead-letter JSON (below) | Streaming job → review |

Partitions: `ami-readings` = 6 (`READINGS_PARTITIONS`), `meter-status` = 3, `anomaly-alerts` = 3, DLQ = 1.
Keying by `transformer_id` keeps one transformer's meters in one partition, so ordering holds per
transformer only, not globally.

**Broker addresses:** inside Docker `kafka:29092`; from your laptop `localhost:9092`.

**Dead-letter message:**

```json
{ "raw_payload": "<original bytes as a string>", "error": "missing field: voltage_v",
  "source_topic": "ami-readings", "failed_at": "2026-07-01T18:16:02Z" }
```

## 5. Lakehouse layout

Bucket `lakehouse` (`S3_BUCKET`). Inside Docker the endpoint is `http://minio:9000`; from your laptop
`http://localhost:9000`. Paths use `s3a://` from Spark.

| Prefix | Contents | Written by |
|---|---|---|
| `reference/dim_region/`, `dim_substation/`, `dim_transformer/`, `dim_meter/` | Dimensions, Parquet | Data generation |
| `bronze/smart_meter_readings/year=YYYY/month=MM/day=DD/` | Raw readings, Parquet, partitioned by date | Data generation (history), streaming job (landing) |
| `silver/` | Typed, deduplicated readings. Same columns as section 3 with `event_timestamp` as a real timestamp, plus `quality_flag` (string) and `ingested_at` (timestamp). | Streaming and batch |
| `quarantine/` | Invalid records kept for review | Streaming and batch |
| `gold/` | Dashboard tables (section 9) | Batch |
| `checkpoints/<job_name>/` | Spark streaming checkpoints | Streaming |
| `answer_key/injected_anomalies/` | Ground truth (section 7). **Detection jobs must not read it.** | Data generation |

Partition by date only. Never partition by `meter_id` (too many small files).

## 6. Scale parameters

Set in `.env` (`GEN_*`). The same code produces both sizes.

| | Prototype | Target |
|---|---|---|
| Meters | 1,000 | 100,000 |
| Transformers | ~10 (100 meters each: set `GEN_METERS_PER_TRANSFORMER=100`) | ~550 (100,000 ÷ 183 meters per transformer, from the EEHC report) |
| Substations | ~1–2 | ~20–25 (assumption) |
| Days | 7 | 7 |
| Readings | 672,000 | ~67.2 million (~9.6 million per day) |

Fixed seed (`GEN_SEED`) gives the same data on every run.

## 7. Answer key: `injected_anomalies`

Ground truth for testing detection and measuring mean time to detect. Kept separate from the stream.

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | `INJ_0001` |
| `anomaly_type` | string | `TRANSFORMER_OVERLOAD`, `VOLTAGE_DROP`, `IRREGULAR_CONSUMPTION` |
| `asset_type` | string | `TRANSFORMER` or `METER` |
| `asset_id` | string | |
| `start_time`, `end_time` | timestamp (UTC) | |
| `severity` | string | `LOW`, `MEDIUM`, `HIGH`, `CRITICAL` |
| `affected_meters` | array&lt;string&gt; | |
| `scenario_params` | string (JSON) | e.g. load multiplier, voltage factor |

## 8. Pipeline output: alerts and incidents

Separate from section 7. These are what the pipeline *detected*.

**`fact_incidents`** (also the shape of an `anomaly-alerts` message)

| Column | Type | Notes |
|---|---|---|
| `incident_id` | string | **Deterministic** (hash of `anomaly_type` + `asset_id` + `start_time`), so replays never create duplicates |
| `anomaly_type` | string | Same enum as section 7 |
| `asset_type`, `asset_id`, `region_id` | string | |
| `detected_at` | timestamp | |
| `start_time` | timestamp | |
| `end_time` | timestamp, nullable | Null while open |
| `status` | string | `OPEN`, `INVESTIGATING`, `RESOLVED` |
| `severity` | string | As section 7 |
| `rule_id` | string | |
| `threshold` | double | |
| `observed_value` | double | |

**`fact_incident_evidence`**: `incident_id`, `reading_id`. Links every alert back to its readings.

An irregular-consumption flag is a reason to investigate, not proof of theft.

## 9. Gold tables

| Table | Grain | Columns |
|---|---|---|
| `fact_transformer_15min` | transformer × interval | `transformer_id`, `interval_start`, `meters_reporting`, `meters_expected`, `total_kw`, `total_kvar`, `est_kva`, `loading_pct`, `avg_voltage_v`, `min_voltage_v` |
| `fact_daily_region` | region × day | `region_id`, `date`, `demand_kwh`, `reporting_rate`, `incident_count` |
| `meter_baseline` | meter × hour of week | `meter_id`, `hour_of_week`, `expected_kwh`, `std_kwh` |
| `fact_meter_readings`, `fact_incidents`, `dim_*`, `dim_time` | see plan, section 6.1 | |

**Loading:** `est_kva = √((Σ active_power_kw)² + (Σ reactive_power_kvar)²)` over the transformer's
reporting meters, and `loading_pct = est_kva ÷ rated_capacity_kva × 100`. Do not sum per-meter kVA,
which overstates the load. When `meters_reporting < meters_expected` the estimate is low, so show both.

## 10. Data-quality checks

Schema and types · unique `reading_id` · every meter → transformer → substation · about 96
readings per meter per day when no gaps are injected · `energy_kwh ≈ kW × 0.25` · loading uses the
right rated capacity · every alert links to evidence · freshness (latest event time vs now).

## 11. Decisions to confirm with the team

| # | Proposal | Why it matters |
|---|---|---|
| 1 | UTC interval-start timestamps with `Z` | Windowing and watermarks in streaming depend on it |
| 2 | `reading_id = READ_<meter_id>_<YYYYMMDDHHMM>` (deterministic, not a global counter) | Deduplication and idempotent replay |
| 3 | Kafka key = `transformer_id` | Lets streaming aggregate per transformer without a shuffle-heavy join |
| 4 | Nominal voltage 220 V phase-to-neutral (EEHC LV is 220/380 V); the plan's example value of 230.8 V fits this | Voltage-drop thresholds |
| 5 | `power_factor` optional | Smaller messages |
| 6 | Bronze path is shared: history covers days 1–6 and the live replay covers day 7, so they never overlap | Avoids duplicate rows between batch and streaming |
| 7 | Table format (Delta or Iceberg) is the batch owner's call; Bronze history stays plain Parquet | Only affects Silver and Gold |
| 8 | Rated capacity is chosen from standard sizes so normal peak loading is roughly 40–80% (assumption). EEHC's average (about 458 kVA per 183 subscribers) would give only about 25% by my rough estimate, which leaves no room for an overload scenario. | Overload injection needs headroom to cross a threshold |
