# Earthquake Data Engineering Pipeline --- Technical Documentation

## Purpose

This document describes the technical design of the USGS Earthquake Data
Engineering Pipeline.

## Layer Responsibilities

### L0 --- Raw

Stores the original USGS API response in GCS.

Purpose: source preservation, traceability and reprocessing.

### L1 --- Structured

Normalizes the JSON response into a consistent BigQuery schema.

Purpose: create a clean staging/current snapshot for validation and
transformation.

### Validation

Runs explicit data-quality rules against L1.

Outputs:

-   `earthquake_raw.validation_results`
-   `earthquake_raw.failed_records`

A data-quality failure is treated as a data outcome, not as an Airflow
infrastructure failure.

### L2 --- Curated

Produces two datasets:

-   **Current:** latest validated batch.
-   **History:** accumulated validated earthquake events.

Business key:

`earthquake_id`

### L3 --- Analytics

Produces reporting-ready tables:

-   `daily_earthquake_activity`
-   `magnitude_distribution`
-   `significant_events`

## Data Quality Rules

  Rule            Condition
  --------------- ------------------
  Earthquake ID   Must not be NULL
  Magnitude       -2 to 10
  Event time      Must not be NULL
  Latitude        -90 to 90
  Longitude       -180 to 180
  Depth           -100 to 1000 km

## Failure Classification

### Data-quality failure

Examples: invalid magnitude, latitude, longitude, depth or NULL ID.

Handling:

1.  Record validation result.
2.  Store failed records.
3.  Continue valid records downstream.

### Technical failure

Examples: API failure, GCS failure, BigQuery failure or permission
failure.

Handling:

-   Airflow task fails.
-   Airflow retry behavior can be used.

## Current vs History

Example:

``` 
Run 1
Current  = Run 1 events
History  = Run 1 events

Run 2
Current  = Run 2 events
History  = Run 1 + new Run 2 events
```

The historical layer uses `earthquake_id` to make repeated processing
idempotent.

## Evidence

Execution evidence is maintained under `../Evidence/` and is described in `Evidence_Details.md`.

The evidence covers:

1. USGS API response stored in GCS L0.
2. L1 BigQuery record and uniqueness counts.
3. Clean validation with all six checks passing and zero failed records.
4. Controlled insertion of `TEST_INVALID_001` with invalid magnitude, latitude, longitude and depth values.
5. Validation detecting the invalid values without accepting them as valid data.
6. Failed-record quarantine in `earthquake_raw.failed_records`.
7. L2 continuation with 100 valid records while excluding the invalid test record.

The controlled failure test is intentionally performed against the L1 BigQuery table rather than modifying the source GCS object. This keeps the source layer clean while allowing the validation behavior to be tested deterministically.

## Auditability

Each task writes to:

`earthquake_raw.pipeline_task_logs`

The Airflow `run_id` is propagated across pipeline logging and
validation outputs.

## Supporting Files

- [`Troubleshooting.md`](Troubleshooting.md) contains the main implementation/runtime troubleshooting notes.
- [`../sql/validation.sql`](../sql/validation.sql) contains a reference copy of the validation SQL; the deployed DAG currently inlines the SQL.
- `sql/validation_queries.sql` contains read/verification queries and the controlled failed-record test queries.
- `Evidence_Details.md` documents the evidence captured for the pipeline and validation behavior.
