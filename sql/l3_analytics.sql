
-- ============================================================
-- 1. Daily Earthquake Activity
-- ============================================================

CREATE OR REPLACE TABLE
  `project-548f052d-b3a2-43b8-987.earthquake_analytics.daily_earthquake_activity`
AS

SELECT
  event_date,

  COUNT(*) AS earthquake_count,

  ROUND(
    AVG(magnitude),
    2
  ) AS average_magnitude,

  ROUND(
    MAX(magnitude),
    2
  ) AS maximum_magnitude,

  ROUND(
    MIN(magnitude),
    2
  ) AS minimum_magnitude,

  COUNTIF(
    significance >= 600
  ) AS significant_earthquake_count,

  COUNTIF(
    tsunami_flag = TRUE
  ) AS tsunami_event_count

FROM
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_history`

GROUP BY
  event_date

ORDER BY
  event_date;


-- ============================================================
-- 2. Magnitude Distribution
-- ============================================================

CREATE OR REPLACE TABLE
  `project-548f052d-b3a2-43b8-987.earthquake_analytics.magnitude_distribution`
AS

SELECT
  magnitude_category,

  COUNT(*) AS earthquake_count,

  ROUND(
    AVG(magnitude),
    2
  ) AS average_magnitude,

  ROUND(
    MAX(magnitude),
    2
  ) AS maximum_magnitude,

  ROUND(
    MIN(magnitude),
    2
  ) AS minimum_magnitude

FROM
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_history`

GROUP BY
  magnitude_category

ORDER BY

  CASE magnitude_category

    WHEN 'Micro' THEN 1
    WHEN 'Minor' THEN 2
    WHEN 'Light' THEN 3
    WHEN 'Moderate' THEN 4
    WHEN 'Strong' THEN 5
    WHEN 'Major' THEN 6

  END;


-- ============================================================
-- 3. Significant Earthquake Events
--
-- An event is included when:
--   magnitude >= 4.0
--   OR significance >= 600
--   OR tsunami_flag = TRUE
-- ============================================================

CREATE OR REPLACE TABLE
  `project-548f052d-b3a2-43b8-987.earthquake_analytics.significant_events`
AS

SELECT
  earthquake_id,
  event_time,
  place,
  magnitude,
  magnitude_category,
  significance,
  tsunami_flag,
  depth_km,
  depth_category,
  latitude,
  longitude,
  location

FROM
  `project-548f052d-b3a2-43b8-987.earthquake_curated.earthquakes_history`

WHERE
  magnitude >= 4.0
  OR significance >= 600
  OR tsunami_flag = TRUE

ORDER BY
  event_time DESC;