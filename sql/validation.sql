
DECLARE validation_run_id STRING DEFAULT @run_id;
DECLARE validation_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

-- ------------------------------------------------------------
-- 1. Make validation retry-safe for the current Airflow run
-- ------------------------------------------------------------

DELETE FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`
WHERE run_id = validation_run_id;

DELETE FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.failed_records`
WHERE run_id = validation_run_id;


-- ------------------------------------------------------------
-- 2. Earthquake ID validation
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`

SELECT
  validation_run_id AS run_id,
  validation_time,
  'earthquake_id_not_null' AS check_name,
  COUNTIF(earthquake_id IS NULL) AS failed_count,
  IF(
    COUNTIF(earthquake_id IS NULL) = 0,
    'PASS',
    'FAIL'
  ) AS status,
  'earthquake_id must not be NULL' AS details
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 3. Magnitude validation
-- USGS magnitude range used by the pipeline: -2 to 10
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`

SELECT
  validation_run_id,
  validation_time,
  'magnitude_range',
  COUNTIF(
    magnitude IS NULL
    OR magnitude < -2
    OR magnitude > 10
  ),
  IF(
    COUNTIF(
      magnitude IS NULL
      OR magnitude < -2
      OR magnitude > 10
    ) = 0,
    'PASS',
    'FAIL'
  ),
  'magnitude must be between -2 and 10'
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 4. Event timestamp validation
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`

SELECT
  validation_run_id,
  validation_time,
  'event_time_not_null',
  COUNTIF(event_time IS NULL),
  IF(
    COUNTIF(event_time IS NULL) = 0,
    'PASS',
    'FAIL'
  ),
  'event_time must not be NULL'
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 5. Latitude validation
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`

SELECT
  validation_run_id,
  validation_time,
  'latitude_range',
  COUNTIF(
    latitude IS NULL
    OR latitude < -90
    OR latitude > 90
  ),
  IF(
    COUNTIF(
      latitude IS NULL
      OR latitude < -90
      OR latitude > 90
    ) = 0,
    'PASS',
    'FAIL'
  ),
  'latitude must be between -90 and 90'
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 6. Longitude validation
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`

SELECT
  validation_run_id,
  validation_time,
  'longitude_range',
  COUNTIF(
    longitude IS NULL
    OR longitude < -180
    OR longitude > 180
  ),
  IF(
    COUNTIF(
      longitude IS NULL
      OR longitude < -180
      OR longitude > 180
    ) = 0,
    'PASS',
    'FAIL'
  ),
  'longitude must be between -180 and 180'
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 7. Depth validation
--
-- Negative depth values are valid in the USGS data.
-- Accepted range: -100 km to 1000 km.
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.validation_results`

SELECT
  validation_run_id,
  validation_time,
  'depth_range',
  COUNTIF(
    depth_km IS NULL
    OR depth_km < -100
    OR depth_km > 1000
  ),
  IF(
    COUNTIF(
      depth_km IS NULL
      OR depth_km < -100
      OR depth_km > 1000
    ) = 0,
    'PASS',
    'FAIL'
  ),
  'depth_km must be between -100 and 1000 km'
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`;


-- ------------------------------------------------------------
-- 8. Capture failed records
--
-- A record is considered invalid if it violates any validation
-- rule. Valid records are allowed to continue downstream.
-- ------------------------------------------------------------

INSERT INTO
  `project-548f052d-b3a2-43b8-987.earthquake_raw.failed_records`

SELECT
  validation_run_id AS run_id,
  validation_time,
  earthquake_id,
  magnitude,
  place,
  event_time,
  updated_time,
  status,
  event_type,
  magnitude_type,
  tsunami,
  significance,
  longitude,
  latitude,
  depth_km,

  ARRAY_TO_STRING(
    ARRAY(
      SELECT reason
      FROM UNNEST([
        IF(
          earthquake_id IS NULL,
          'earthquake_id is NULL',
          NULL
        ),
        IF(
          magnitude IS NULL
          OR magnitude < -2
          OR magnitude > 10,
          'magnitude outside valid range',
          NULL
        ),
        IF(
          event_time IS NULL,
          'event_time is NULL',
          NULL
        ),
        IF(
          latitude IS NULL
          OR latitude < -90
          OR latitude > 90,
          'latitude outside valid range',
          NULL
        ),
        IF(
          longitude IS NULL
          OR longitude < -180
          OR longitude > 180,
          'longitude outside valid range',
          NULL
        ),
        IF(
          depth_km IS NULL
          OR depth_km < -100
          OR depth_km > 1000,
          'depth outside valid range',
          NULL
        )
      ]) AS reason
      WHERE reason IS NOT NULL
    ),
    '; '
  ) AS failure_reason

FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes`

WHERE
  earthquake_id IS NULL
  OR magnitude IS NULL
  OR magnitude < -2
  OR magnitude > 10
  OR event_time IS NULL
  OR latitude IS NULL
  OR latitude < -90
  OR latitude > 90
  OR longitude IS NULL
  OR longitude < -180
  OR longitude > 180
  OR depth_km IS NULL
  OR depth_km < -100
  OR depth_km > 1000;