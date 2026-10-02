# Troubleshooting Guide — Earthquake Data Engineering Pipeline

This document records the main implementation and runtime issues encountered while building and testing the Earthquake Data Engineering Pipeline, together with the resolution used in the final implementation.

## 1. Airflow runtime parameters were not appearing in the trigger UI

### Symptom
The DAG could read `dag_run.conf`, but the Airflow trigger form did not provide fields for the GCS bucket and source folder.

### Cause
Reading values from `dag_run.conf` does not by itself define fields in the Airflow trigger UI.

### Resolution
The DAG defines runtime parameters with Airflow `Param` objects:

- `bucket`
- `source_folder`

The task code reads the supplied values from the DAG run configuration.

### Final runtime configuration

```json
{
  "bucket": "project-548f052d-b3a2-43b8-987-earthquake-raw",
  "source_folder": "project2/raw/earthquakes"
}
```

---

## 2. Validation initially treated negative earthquake depth as invalid

### Symptom
The first inspection of the L1 dataset reported six records with negative depth values.

### Cause
The initial validation assumed `depth_km >= 0`.

### Resolution
The validation rule was aligned with the USGS API domain used by the project:

``` 
-100 km <= depth_km <= 1000 km
```

Negative depths therefore remain valid when they fall within the accepted range.

### Final rule

```sql
depth_km IS NULL
OR depth_km < -100
OR depth_km > 1000
```

---

## 3. Data-quality failures must not fail the Airflow DAG

### Requirement
An invalid earthquake record should be captured and quarantined without stopping valid records from continuing downstream.

### Final behavior

``` 
L1
 ↓
Validation
 ├── validation_results
 └── failed_records
        ↓
Valid records continue
        ↓
L2
        ↓
L3
```

Data-quality failures are therefore recorded as validation outcomes. Technical failures such as API, GCS, BigQuery, SQL, or permission failures remain task failures and can use Airflow retry behavior.

---

## 4. Testing the failed-record path initially appeared to pass

### Symptom
An intentionally invalid record was inserted into the L1 BigQuery table, followed by a full DAG run. Validation subsequently showed all checks as PASS.

### Cause
The full DAG starts by rebuilding/reloading L1 from the clean GCS source. The manually inserted test record was therefore overwritten before validation executed.

### Correct test method

1. Run a clean DAG run.
2. Confirm L1 contains the expected 100 records.
3. Insert one controlled invalid record directly into L1.
4. Do **not** rerun the entire DAG.
5. Rerun only:
   - `validate_l1_data`
   - `build_l2_curated`
   - `build_l3_analytics`
6. Inspect `validation_results` and `failed_records`.
7. Verify the invalid record is absent from L2.
8. Remove the test record.
9. Run a fresh clean pipeline to restore the final project state.

This keeps the source GCS data untouched while testing the validation and downstream behavior.

---

## 5. Validation retries could create duplicate audit rows

### Symptom
Airflow retries could potentially insert duplicate validation results or failed-record rows for the same run.

### Resolution
The validation logic clears existing validation output for the current Airflow `run_id` before reprocessing:

```sql
DELETE FROM earthquake_raw.validation_results
WHERE run_id = @run_id;

DELETE FROM earthquake_raw.failed_records
WHERE run_id = @run_id;
```

The same Airflow run ID is propagated through the validation and audit outputs.

---

## 6. Validation run ID and Airflow run ID needed to be consistent

### Symptom
Using a separately generated UUID for validation made it harder to correlate validation output with a specific Airflow execution.

### Resolution
The final implementation uses the Airflow DAG run ID as the pipeline run identifier.

This allows:

``` 
Airflow DAG run
      ↓
pipeline_task_logs
      ↓
validation_results
      ↓
failed_records
```

to be traced using the same `run_id`.

---

## 7. Datetime values caused JSON serialization errors in task logging

### Symptom
Task logging initially raised a Python error similar to:

``` 
TypeError: Object of type datetime is not JSON serializable
```

### Cause
A Python `datetime` object was being passed into a JSON-serialized logging payload without conversion.

### Resolution
Task timestamps are converted to ISO-formatted strings before being written to the logging table/payload.

The final task log contains:

- `start_time`
- `end_time`
- `status`
- `records_processed`
- `message`

---

## 8. L3 task was defined but not initially instantiated in the DAG

### Symptom
The L3 function existed in the DAG code, but the final task dependency chain did not initially include an instantiated L3 task.

### Resolution
The DAG explicitly creates the task and places it after L2:

``` 
validate_l1_data
        ↓
build_l2_curated
        ↓
build_l3_analytics
```

This ensures L3 analytics are rebuilt after successful L2 processing.

---

## 9. Current and historical L2 data need different responsibilities

### Design decision
The curated layer contains:

- `earthquakes_curated` — latest validated/current batch
- `earthquakes_history` — accumulated validated events

The historical table uses `earthquake_id` as the business key for idempotent processing.

This prevents repeated processing of the same earthquake event from intentionally creating duplicate historical records.

---

## 10. Duplicate DAG files

### Symptom
Development included more than one DAG file with different DAG IDs.

### Resolution
The final project should contain only the production DAG used by the project:

``` 
dags/earthquake_pipeline.py
```

Keeping a single production DAG avoids duplicate deployment and ambiguity when the repository is reviewed or deployed.

---

## Final troubleshooting principle

The project deliberately separates **data-quality failures** from **technical pipeline failures**:

| Failure type | Example | Airflow task | Downstream valid data |
|---|---|---|---|
| Data quality | Invalid magnitude | SUCCESS | Continues |
| Data quality | Invalid coordinates | SUCCESS | Continues |
| Data quality | Invalid depth | SUCCESS | Continues |
| Technical | API failure | FAIL / retry | Stops |
| Technical | BigQuery failure | FAIL / retry | Stops |
| Technical | Permission failure | FAIL / retry | Stops |

