# Earthquake Data Engineering Pipeline

An end-to-end Google Cloud data engineering project that ingests
earthquake data from the USGS Earthquake Hazards Program API, stores raw
data in Google Cloud Storage, transforms and validates data in BigQuery,
maintains current and historical curated datasets, and produces
analytics-ready tables.

## Architecture

``` 
USGS Earthquake API
        |
        v
Python / Cloud Composer / Airflow
        |
        v
GCS L0 Raw
        |
        v
BigQuery L1
        |
        v
Data Quality Validation
   +----+----+
   |         |
   v         v
validation  failed
results     records
        |
        v
BigQuery L2
  +-------------+-------------+
  |                           |
  v                           v
Current                    History
  +-------------+-------------+
                |
                v
BigQuery L3 Analytics

```

## Objective

The pipeline:

1.  Extracts earthquake events from the USGS API.
2.  Stores the raw API response in GCS.
3.  Creates a normalized L1 JSONL snapshot.
4.  Loads the data into BigQuery.
5.  Performs data-quality validation.
6.  Stores validation results and failed records separately.
7.  Continues valid records into the curated layer.
8.  Maintains both current and historical curated data.
9.  Builds analytics-ready L3 tables.
10. Logs task execution in BigQuery.

## Source

USGS Earthquake Hazards Program API:

`https://earthquake.usgs.gov/fdsnws/event/1/query`

The API returns GeoJSON. Coordinates are represented as:

`[longitude, latitude, depth_km]`

The pipeline currently processes a 100-record snapshot per run.

## GCP Components

  Component       Technology                 Purpose
  --------------- -------------------------- ---------------------------------------
  Source          USGS API                   Earthquake event data
  Orchestration   Cloud Composer / Airflow   Pipeline orchestration
  Raw storage     Google Cloud Storage       Raw API responses
  Processing      Python                     API extraction and normalization
  L1/L2/L3        BigQuery                   Storage, transformation and analytics
  Validation      BigQuery SQL + Airflow     Data-quality checks
  Audit           BigQuery                   Task execution logging
  BI              Power BI                   Optional visualization

## Data Layers

### L0 --- Raw

Date-based GCS storage:

``` 
gs://project-548f052d-b3a2-43b8-987-earthquake-raw/
project2/raw/earthquakes/<run_date>/earthquakes_raw.json
```

The raw layer preserves the source response.

### L1 --- Structured

BigQuery dataset:

`earthquake_raw`

Main table:

`earthquakes`

The GeoJSON response is normalized into fields such as:

-   earthquake_id
-   magnitude
-   place
-   event_time
-   updated_time
-   status
-   event_type
-   magnitude_type
-   tsunami
-   significance
-   longitude
-   latitude
-   depth_km

### Validation

Validation checks include:

-   earthquake ID not null
-   magnitude between -2 and 10
-   event time not null
-   latitude between -90 and 90
-   longitude between -180 and 180
-   depth between -100 and 1000 km

Data-quality failures do not fail the Airflow DAG. They are written
to:

`earthquake_raw.validation_results`

and, where applicable:

`earthquake_raw.failed_records`

Technical failures such as API, GCS, BigQuery or permission errors
remain Airflow task failures and can be retried.

### L2 --- Curated

Dataset:

`earthquake_curated`

Tables:

-   `earthquakes_curated` --- latest validated batch
-   `earthquakes_history` --- accumulated validated history

`earthquake_id` is the business key used for idempotent historical
processing.

Derived fields include:

-   event_date
-   event_year
-   event_month
-   magnitude_category
-   tsunami_flag
-   location
-   depth_category

### L3 --- Analytics

Dataset:

`earthquake_analytics`

Tables:

-   `daily_earthquake_activity`
-   `magnitude_distribution`
-   `significant_events`

These tables are designed for reporting and BI consumption.

## Airflow DAG

File:

`earthquake_pipeline.py`

DAG:

`earthquake_pipeline`

Task flow:

``` 
extract_api_and_upload_raw
        ↓
load_l1_snapshot
        ↓
load_l1_to_bigquery
        ↓
validate_l1_data
        ↓
build_l2_curated
        ↓
build_l3_analytics
```

## How to run

The deployed DAG is designed to be triggered manually in Airflow/Cloud Composer.

In the Airflow **Trigger DAG** form, provide:

```json
{
  "bucket": "project-548f052d-b3a2-43b8-987-earthquake-raw",
  "source_folder": "project2/raw/earthquakes"
}
```

The DAG uses these values to locate the raw GCS snapshot for the run date. The pipeline then executes:

```text
USGS API
  → GCS L0
  → L1 JSONL
  → BigQuery L1
  → Validation
  → L2 Current + History
  → L3 Analytics
```

For the detailed trigger workflow, troubleshooting notes, and controlled validation-failure test, see [`docs/Troubleshooting.md`](docs/Troubleshooting.md).

> **Note:** The repository does not contain GCP credentials or service-account keys. The DAG expects the Google Cloud connection/identity provided by the Composer environment.

## Evidence

Execution evidence is stored under `Evidence/`. The evidence set covers the clean pipeline, a controlled invalid-record test, validation failure capture, failed-record quarantine, and downstream continuation.

| Evidence | Files | Result |
|---|---|---|
| API → GCS | `01_api_to_gcs.png` | Raw USGS response present in date-based GCS L0 path |
| L1 | `02.1_l1_bigquery.png`, `02.2_l1_bigquery.png` | 100 records / 100 unique IDs; normalized records visible |
| Clean validation | `03.1_validation_pass.png`, `03.2_validation_pass.png` | 6 checks PASS; 0 failed records |
| Failure injection | `04.1_failed_record_test.png`, `04.2_failed_record_test.png` | `TEST_INVALID_001` introduced; L1 becomes 101 records |
| Failure detection | `05_validation_results_fail.png` | Magnitude, latitude, longitude and depth checks fail for the test record |
| Quarantine | `06.1_failed_records_quarantine.png`, `06.2_failed_records_quarantine.png` | 1 failed record / 1 unique failed ID captured |
| L2 continuation | `07_l2_valid_records_continue.png` | 100 valid records continue; invalid test record excluded |

Detailed evidence descriptions are available in [`docs/Evidence_Details.md`](docs/Evidence_Details.md).

## Audit Logging

Table:

`earthquake_raw.pipeline_task_logs`

Fields:

``` 
run_id
task_name
status
start_time
end_time
records_processed
message
```

The Airflow `run_id` is carried through pipeline logging and validation
outputs to make individual executions traceable.

## Idempotency

Validation output for the same Airflow run is cleared/reprocessed during
retries to avoid duplicate validation results.

Historical processing uses `earthquake_id` as the business key so
repeated processing does not intentionally create duplicate earthquake
events.




## Supporting Documentation

- [`docs/Documentation.md`](docs/Documentation.md) — technical architecture and layer responsibilities.
- [`docs/Troubleshooting.md`](docs/Troubleshooting.md) — implementation issues encountered during development, their causes, and resolutions.
- [`sql/validation.sql`](sql/validation.sql) — reference copy of the validation SQL; the deployed DAG currently inlines the SQL.
- `sql/validation_queries.sql` — inspection, evidence, failed-record test, L2 verification, and audit queries.
- [`docs/Evidence_Details.md`](docs/Evidence_Details.md) — description of the captured validation and pipeline evidence.

## License

MIT License. See [LICENSE](LICENSE).
