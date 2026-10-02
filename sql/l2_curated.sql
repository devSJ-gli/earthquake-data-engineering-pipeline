
DECLARE validation_run_id STRING DEFAULT @run_id;


-- ============================================================
-- 1. Build CURRENT curated dataset
--
-- Only records that passed validation are included.
-- ============================================================

CREATE OR REPLACE TABLE
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_curated`
AS

SELECT
  e.earthquake_id,
  e.magnitude,
  e.place,
  e.event_time,
  e.updated_time,
  e.status,
  e.event_type,
  e.magnitude_type,
  e.tsunami,
  e.significance,
  e.longitude,
  e.latitude,
  e.depth_km,

  DATE(e.event_time) AS event_date,

  EXTRACT(
    YEAR FROM e.event_time
  ) AS event_year,

  EXTRACT(
    MONTH FROM e.event_time
  ) AS event_month,

  CASE
    WHEN e.magnitude < 2.0 THEN 'Micro'
    WHEN e.magnitude < 4.0 THEN 'Minor'
    WHEN e.magnitude < 5.0 THEN 'Light'
    WHEN e.magnitude < 6.0 THEN 'Moderate'
    WHEN e.magnitude < 7.0 THEN 'Strong'
    ELSE 'Major'
  END AS magnitude_category,

  e.tsunami = 1 AS tsunami_flag,

  ST_GEOGPOINT(
    e.longitude,
    e.latitude
  ) AS location,

  CASE
    WHEN e.depth_km < 70 THEN 'Shallow'
    WHEN e.depth_km < 300 THEN 'Intermediate'
    ELSE 'Deep'
  END AS depth_category

FROM
  `project-548f052d-b3a2-43b8-987.earthquake_raw.earthquakes` e

WHERE NOT EXISTS (

  SELECT 1

  FROM
    `project-548f052d-b3a2-43b8-987.earthquake_raw.failed_records` f

  WHERE
    f.run_id = validation_run_id
    AND f.earthquake_id = e.earthquake_id
);


-- ============================================================
-- 2. Create history table if it does not already exist
-- ============================================================

CREATE TABLE IF NOT EXISTS
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_history`
AS

SELECT *
FROM
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_curated`
WHERE FALSE;


-- ============================================================
-- 3. Add new validated records to HISTORY
--
-- MERGE makes the operation idempotent.
-- ============================================================

MERGE INTO
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_history`
AS target

USING
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_curated`
AS source

ON
  target.earthquake_id = source.earthquake_id

WHEN NOT MATCHED THEN

INSERT (
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
  event_date,
  event_year,
  event_month,
  magnitude_category,
  tsunami_flag,
  location,
  depth_category
)

VALUES (
  source.earthquake_id,
  source.magnitude,
  source.place,
  source.event_time,
  source.updated_time,
  source.status,
  source.event_type,
  source.magnitude_type,
  source.tsunami,
  source.significance,
  source.longitude,
  source.latitude,
  source.depth_km,
  source.event_date,
  source.event_year,
  source.event_month,
  source.magnitude_category,
  source.tsunami_flag,
  source.location,
  source.depth_category
);