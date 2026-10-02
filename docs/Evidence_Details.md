# Evidence Details

This folder contains execution evidence for the Earthquake Data Engineering Pipeline. The screenshots document the normal pipeline path and the controlled data-quality failure test.

## Evidence Summary

| Evidence | File | Purpose | What it demonstrates |
|---|---|---|---|
| 01 | `01_api_to_gcs.png` | Raw ingestion | The USGS API response is stored in the date-based GCS L0 raw path. |
| 02.1 | `02.1_l1_bigquery.png` | L1 record-count verification | L1 contains 100 records with 100 unique earthquake IDs. |
| 02.2 | `02.2_l1_bigquery.png` | L1 data inspection | The normalized earthquake records are present in the BigQuery L1 table. |
| 03.1 | `03.1_validation_pass.png` | Clean validation | All six data-quality checks pass with `failed_count = 0`. |
| 03.2 | `03.2_validation_pass.png` | Clean failed-record check | The clean validation run has zero records in `failed_records`. |
| 04.1 | `04.1_failed_record_test.png` | Controlled failure injection | A deliberately invalid test record is introduced into the L1 table. |
| 04.2 | `04.2_failed_record_test.png` | Injection verification | L1 contains 101 records after adding the single test record. |
| 05 | `05_validation_results_fail.png` | Failure detection | Validation identifies the invalid record through failed magnitude, latitude, longitude and depth checks. |
| 06.1 | `06.1_failed_records_quarantine.png` | Failed-record quarantine | The invalid record is written to `earthquake_raw.failed_records` with its failure information. |
| 06.2 | `06.2_failed_records_quarantine.png` | Failed-record count | The test run contains exactly one failed record and one unique failed earthquake ID. |
| 07 | `07_l2_valid_records_continue.png` | Downstream continuation | L2 contains 100 valid records, confirming the invalid record did not enter the curated current layer. |

## Detailed Evidence

### 01 — API to GCS

**File:** `01_api_to_gcs.png`

Shows the date-based GCS L0 raw location containing the USGS API response file. This establishes source preservation before the data enters BigQuery.

**Pipeline stage:** USGS API → GCS L0 Raw

### 02 — L1 BigQuery

**Files:** `02.1_l1_bigquery.png`, `02.2_l1_bigquery.png`

The first screenshot verifies the L1 row and uniqueness counts. The second provides a direct inspection of the normalized records in `earthquake_raw.earthquakes`.

Expected clean state:
- Total records: 100
- Unique earthquake IDs: 100

**Pipeline stage:** GCS L0 → BigQuery L1

### 03 — Clean Validation

**Files:** `03.1_validation_pass.png`, `03.2_validation_pass.png`

The validation results show all six configured checks passing with zero failures. The second screenshot confirms that no records are present in the failed-record output for the clean run.

Validation rules:
- `earthquake_id` not null
- magnitude between -2 and 10
- `event_time` not null
- latitude between -90 and 90
- longitude between -180 and 180
- depth between -100 and 1000 km

**Expected result:** 6 PASS checks, 0 failed records.

### 04 — Controlled Invalid Record Test

**Files:** `04.1_failed_record_test.png`, `04.2_failed_record_test.png`

A synthetic record with ID `TEST_INVALID_001` was deliberately inserted into the L1 table for validation testing. The test record uses invalid values for magnitude, latitude, longitude and depth.

Test values:
- magnitude: `99`
- latitude: `200`
- longitude: `300`
- depth_km: `2000`

The L1 table increased from 100 to 101 records, confirming that the test record was present before validation was rerun.

**Purpose:** Verify that data-quality failures are detected and quarantined without treating them as Airflow infrastructure failures.

### 05 — Validation Failure Detection

**File:** `05_validation_results_fail.png`

The validation output records failures for the invalid test record while the unaffected rules continue to pass. The failed checks are:

- `magnitude_range` → FAIL, 1
- `latitude_range` → FAIL, 1
- `longitude_range` → FAIL, 1
- `depth_range` → FAIL, 1

The non-violated checks remain PASS.

This demonstrates that validation produces a data-quality result rather than silently accepting invalid data.

### 06 — Failed-Record Quarantine

**Files:** `06.1_failed_records_quarantine.png`, `06.2_failed_records_quarantine.png`

The invalid record is persisted in `earthquake_raw.failed_records`. The record retains the invalid values and the associated failure reason. The count check confirms exactly one failed record and one unique failed earthquake ID for the test run.

**Expected result:**
- failed records: 1
- unique failed IDs: 1
- test ID: `TEST_INVALID_001`

### 07 — Valid Records Continue to L2

**File:** `07_l2_valid_records_continue.png`

The L2 current curated table contains 100 valid records after validation, while the deliberately invalid `TEST_INVALID_001` record is excluded.

This demonstrates the intended behavior:

```text
L1: 101 records
       |
       +---- 1 invalid record → failed_records
       |
       +---- 100 valid records → L2 curated
```

## Evidence Interpretation

The evidence demonstrates two different classes of failure handling:

**Data-quality failure:**
Invalid values are recorded in validation outputs, the affected record is quarantined, and valid records continue downstream.

**Technical failure:**
API, GCS, BigQuery, permission, or orchestration errors remain task-level failures and can trigger Airflow retry behavior.

The controlled test therefore verifies the key design decision of Project 2: **data-quality failures are handled as data outcomes and do not stop processing of valid records.**

## Evidence Limitations

The current evidence set does not include separate screenshots for Airflow task status, L3 analytics, or `pipeline_task_logs`. The included evidence focuses on the data-path and validation behavior: source storage, L1, clean validation, controlled failure injection, failure capture, and successful continuation into L2.
