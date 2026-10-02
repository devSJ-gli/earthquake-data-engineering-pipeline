-- ============================================================
-- Validation & Evidence Queries
-- ============================================================
-- These queries are for inspection, testing and evidence capture.
-- The executable validation logic used by the pipeline is in:
--     sql/validation.sql
--
-- Replace RUN_ID with the Airflow DAG run_id when required.
-- ============================================================

-- ------------------------------------------------------------
-- 1. Confirm L1 row count and uniqueness
-- ------------------------------------------------------------
SELECT
  COUNT(*) AS total_records,
  COUNT(DISTINCT earthquake_id) AS unique_ids
FROM
  `project.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 2. Confirm the controlled test record exists
-- ------------------------------------------------------------
SELECT
  earthquake_id,
  magnitude,
  latitude,
  longitude,
  depth_km
FROM
  `project.earthquake_raw.earthquakes`
WHERE
  earthquake_id = 'TEST_INVALID_001';


-- ------------------------------------------------------------
-- 3. Clean validation result for the latest run
-- ------------------------------------------------------------
SELECT
  run_id,
  check_name,
  failed_count,
  status,
  details
FROM
  `project.earthquake_raw.validation_results`
ORDER BY
  validation_time DESC,
  check_name;


-- ------------------------------------------------------------
-- 4. Validation results for a specific Airflow run
-- ------------------------------------------------------------
SELECT
  run_id,
  check_name,
  failed_count,
  status,
  details
FROM
  `project.earthquake_raw.validation_results`
WHERE
  run_id = 'RUN_ID'
ORDER BY
  check_name;


-- ------------------------------------------------------------
-- 5. Failed records for a specific Airflow run
-- ------------------------------------------------------------
SELECT
  run_id,
  earthquake_id,
  magnitude,
  latitude,
  longitude,
  depth_km,
  failure_reason
FROM
  `project.earthquake_raw.failed_records`
WHERE
  run_id = 'RUN_ID'
ORDER BY
  earthquake_id;


-- ------------------------------------------------------------
-- 6. Count unique failed records for a run
-- ------------------------------------------------------------
SELECT
  COUNT(*) AS failed_record_count,
  COUNT(DISTINCT earthquake_id) AS unique_failed_records
FROM
  `project.earthquake_raw.failed_records`
WHERE
  run_id = 'RUN_ID';


-- ------------------------------------------------------------
-- 7. Verify the invalid record is excluded from L2 Current
-- ------------------------------------------------------------
SELECT
  COUNT(*) AS current_count,
  COUNTIF(earthquake_id = 'TEST_INVALID_001') AS invalid_record_count
FROM
  `project.earthquake_curated.earthquakes_curated`;


-- ------------------------------------------------------------
-- 8. Explicitly search L2 for the invalid test record
-- ------------------------------------------------------------
SELECT *
FROM
  `project.earthquake_curated.earthquakes_curated`
WHERE
  earthquake_id = 'TEST_INVALID_001';


-- ------------------------------------------------------------
-- 9. Verify L3 analytics contains output
-- ------------------------------------------------------------
SELECT
  event_date,
  earthquake_count,
  average_magnitude,
  maximum_magnitude,
  minimum_magnitude,
  significant_earthquake_count,
  tsunami_event_count
FROM
  `project.earthquake_analytics.daily_earthquake_activity`
ORDER BY
  event_date DESC;


-- ------------------------------------------------------------
-- 10. Verify pipeline task logs
-- ------------------------------------------------------------
SELECT
  run_id,
  task_name,
  status,
  start_time,
  end_time,
  records_processed,
  message
FROM
  `project.earthquake_raw.pipeline_task_logs`
WHERE
  run_id = 'RUN_ID'
ORDER BY
  start_time;


-- ------------------------------------------------------------
-- 11. Remove the controlled test record after testing
-- ------------------------------------------------------------
-- Run only after all failed-record evidence has been captured.
DELETE FROM
  `project.earthquake_raw.earthquakes`
WHERE
  earthquake_id = 'TEST_INVALID_001';


-- ------------------------------------------------------------
-- 12. Verify the test record has been removed
-- ------------------------------------------------------------
SELECT
  COUNT(*) AS remaining_test_records
FROM
  `project.earthquake_raw.earthquakes`
WHERE
  earthquake_id = 'TEST_INVALID_001';
