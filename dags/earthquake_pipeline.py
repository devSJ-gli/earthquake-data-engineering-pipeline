from datetime import datetime, timezone

import json
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models import Param
from airflow.operators.python import get_current_context
from airflow.providers.google.cloud.hooks.gcs import GCSHook


PROJECT_ID = "project-548f052d-b3a2-43b8-987"

API_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"


with DAG(
    dag_id="earthquake_pipeline",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["project2", "earthquake"],

    params={
        "bucket": Param(
            "",
            type="string",
            title="GCS Bucket",
            description="GCS bucket used for the earthquake pipeline",
        ),
        "source_folder": Param(
            "",
            type="string",
            title="Source Folder",
            description="GCS source folder for raw earthquake data",
        ),
    },
) as dag:

    # ============================================================
    # HELPER
    # WRITE TASK LOG
    # ============================================================

    def write_task_log(
        client,
        run_id,
        task_name,
        status,
        start_time,
        end_time,
        records_processed,
        message,
    ):

        table_id = (
            f"{PROJECT_ID}."
            f"earthquake_raw."
            f"pipeline_task_logs"
        )

        # Convert Python datetime objects to ISO strings
        # because insert_rows_json uses JSON serialization.
        if isinstance(start_time, datetime):
            start_time = start_time.isoformat()

        if isinstance(end_time, datetime):
            end_time = end_time.isoformat()

        rows = [
            {
                "run_id": run_id,
                "task_name": task_name,
                "status": status,
                "start_time": start_time,
                "end_time": end_time,
                "records_processed": records_processed,
                "message": message,
            }
        ]

        errors = client.insert_rows_json(
            table_id,
            rows,
        )

        if errors:
            raise RuntimeError(
                f"Failed to write task log: {errors}"
            )

    # ============================================================
    # TASK 1
    # USGS API -> GCS L0 Raw
    # ============================================================

    @task
    def extract_api_and_upload_raw():

        from google.cloud import bigquery

        context = get_current_context()
        dag_run = context["dag_run"]

        run_id = dag_run.run_id
        start_time = datetime.now(timezone.utc)

        task_name = "extract_api_and_upload_raw"

        try:

            conf = dag_run.conf or {}

            bucket_name = conf.get("bucket")
            source_folder = conf.get("source_folder")

            if not bucket_name:
                raise ValueError(
                    "Missing required DAG input: bucket"
                )

            if not source_folder:
                raise ValueError(
                    "Missing required DAG input: source_folder"
                )

            params = {
                "format": "geojson",
                "limit": 100,
                "orderby": "time",
            }

            response = requests.get(
                API_URL,
                params=params,
                timeout=30,
            )

            response.raise_for_status()

            data = response.json()

            features = data.get(
                "features",
                []
            )

            if not features:
                raise ValueError(
                    "USGS API returned no earthquake records"
                )

            raw_json = json.dumps(
                data,
                indent=2,
            )

            run_date = context["ds"]

            source_folder = source_folder.rstrip("/")

            raw_path = (
                f"{source_folder}/"
                f"{run_date}/"
                f"earthquakes_raw.json"
            )

            gcs_hook = GCSHook(
                gcp_conn_id="google_cloud_default"
            )

            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=raw_path,
                data=raw_json,
                mime_type="application/json",
            )

            end_time = datetime.now(timezone.utc)

            client = bigquery.Client(
                project=PROJECT_ID
            )

            write_task_log(
                client=client,
                run_id=run_id,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=len(features),
                message=(
                    f"Raw data uploaded to "
                    f"gs://{bucket_name}/{raw_path}"
                ),
            )

            print(
                f"Records received: {len(features)}"
            )

            print(
                f"Raw data uploaded to: "
                f"gs://{bucket_name}/{raw_path}"
            )

        except Exception as e:

            end_time = datetime.now(timezone.utc)

            try:

                client = bigquery.Client(
                    project=PROJECT_ID
                )

                write_task_log(
                    client=client,
                    run_id=run_id,
                    task_name=task_name,
                    status="FAILED",
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=0,
                    message=str(e),
                )

            except Exception as log_error:

                print(
                    f"Unable to write failure log: "
                    f"{log_error}"
                )

            raise

    # ============================================================
    # TASK 2
    # GCS L0 Raw -> L1 JSONL
    # ============================================================

    @task
    def load_l1_snapshot():

        from google.cloud import bigquery

        context = get_current_context()
        dag_run = context["dag_run"]

        run_id = dag_run.run_id
        start_time = datetime.now(timezone.utc)

        task_name = "load_l1_snapshot"

        try:

            conf = dag_run.conf or {}

            bucket_name = conf.get("bucket")
            source_folder = conf.get("source_folder")

            if not bucket_name:
                raise ValueError(
                    "Missing required DAG input: bucket"
                )

            if not source_folder:
                raise ValueError(
                    "Missing required DAG input: source_folder"
                )

            run_date = context["ds"]

            source_folder = source_folder.rstrip("/")

            raw_path = (
                f"{source_folder}/"
                f"{run_date}/"
                f"earthquakes_raw.json"
            )

            gcs_hook = GCSHook(
                gcp_conn_id="google_cloud_default"
            )

            raw_data = gcs_hook.download(
                bucket_name=bucket_name,
                object_name=raw_path,
            )

            data = json.loads(
                raw_data.decode("utf-8")
            )

            features = data.get(
                "features",
                []
            )

            if not features:
                raise ValueError(
                    "No earthquake features found "
                    "in raw GCS data"
                )

            l1_records = []

            for feature in features:

                properties = feature.get(
                    "properties",
                    {}
                )

                geometry = feature.get(
                    "geometry",
                    {}
                )

                coordinates = geometry.get(
                    "coordinates",
                    []
                )

                record = {
                    "earthquake_id": feature.get("id"),
                    "magnitude": properties.get("mag"),
                    "place": properties.get("place"),
                    "event_time_ms": properties.get("time"),
                    "updated_time_ms": properties.get("updated"),
                    "status": properties.get("status"),
                    "event_type": properties.get("type"),
                    "magnitude_type": properties.get("magType"),
                    "tsunami": properties.get("tsunami"),
                    "significance": properties.get("sig"),
                    "longitude": (
                        coordinates[0]
                        if len(coordinates) > 0
                        else None
                    ),
                    "latitude": (
                        coordinates[1]
                        if len(coordinates) > 1
                        else None
                    ),
                    "depth_km": (
                        coordinates[2]
                        if len(coordinates) > 2
                        else None
                    ),
                }

                l1_records.append(record)

            l1_jsonl = "\n".join(
                json.dumps(record)
                for record in l1_records
            )

            l1_path = (
                f"project2/l1/"
                f"{run_date}/"
                f"earthquakes_l1.jsonl"
            )

            gcs_hook.upload(
                bucket_name=bucket_name,
                object_name=l1_path,
                data=l1_jsonl,
                mime_type="application/json",
            )

            end_time = datetime.now(timezone.utc)

            client = bigquery.Client(
                project=PROJECT_ID
            )

            write_task_log(
                client=client,
                run_id=run_id,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=len(l1_records),
                message=(
                    f"L1 JSONL uploaded to "
                    f"gs://{bucket_name}/{l1_path}"
                ),
            )

            print(
                f"L1 records prepared: "
                f"{len(l1_records)}"
            )

            print(
                f"L1 JSONL uploaded to: "
                f"gs://{bucket_name}/{l1_path}"
            )

        except Exception as e:

            end_time = datetime.now(timezone.utc)

            try:

                client = bigquery.Client(
                    project=PROJECT_ID
                )

                write_task_log(
                    client=client,
                    run_id=run_id,
                    task_name=task_name,
                    status="FAILED",
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=0,
                    message=str(e),
                )

            except Exception as log_error:

                print(
                    f"Unable to write failure log: "
                    f"{log_error}"
                )

            raise

    # ============================================================
    # TASK 3
    # L1 JSONL -> BigQuery L1
    # ============================================================

    @task
    def load_l1_to_bigquery():

        from google.cloud import bigquery

        context = get_current_context()
        dag_run = context["dag_run"]

        run_id = dag_run.run_id
        start_time = datetime.now(timezone.utc)

        task_name = "load_l1_to_bigquery"

        try:

            conf = dag_run.conf or {}

            bucket_name = conf.get("bucket")

            if not bucket_name:
                raise ValueError(
                    "Missing required DAG input: bucket"
                )

            run_date = context["ds"]

            l1_path = (
                f"project2/l1/"
                f"{run_date}/"
                f"earthquakes_l1.jsonl"
            )

            gcs_hook = GCSHook(
                gcp_conn_id="google_cloud_default"
            )

            l1_data = gcs_hook.download(
                bucket_name=bucket_name,
                object_name=l1_path,
            )

            l1_jsonl = l1_data.decode(
                "utf-8"
            )

            if not l1_jsonl.strip():
                raise ValueError(
                    "L1 JSONL file is empty"
                )

            client = bigquery.Client(
                project=PROJECT_ID
            )

            staging_table = (
                f"{PROJECT_ID}."
                f"earthquake_raw."
                f"earthquakes_staging"
            )

            final_table = (
                f"{PROJECT_ID}."
                f"earthquake_raw."
                f"earthquakes"
            )

            job_config = bigquery.LoadJobConfig(
                schema=[
                    bigquery.SchemaField(
                        "earthquake_id",
                        "STRING",
                    ),
                    bigquery.SchemaField(
                        "magnitude",
                        "FLOAT64",
                    ),
                    bigquery.SchemaField(
                        "place",
                        "STRING",
                    ),
                    bigquery.SchemaField(
                        "event_time_ms",
                        "INT64",
                    ),
                    bigquery.SchemaField(
                        "updated_time_ms",
                        "INT64",
                    ),
                    bigquery.SchemaField(
                        "status",
                        "STRING",
                    ),
                    bigquery.SchemaField(
                        "event_type",
                        "STRING",
                    ),
                    bigquery.SchemaField(
                        "magnitude_type",
                        "STRING",
                    ),
                    bigquery.SchemaField(
                        "tsunami",
                        "INT64",
                    ),
                    bigquery.SchemaField(
                        "significance",
                        "INT64",
                    ),
                    bigquery.SchemaField(
                        "longitude",
                        "FLOAT64",
                    ),
                    bigquery.SchemaField(
                        "latitude",
                        "FLOAT64",
                    ),
                    bigquery.SchemaField(
                        "depth_km",
                        "FLOAT64",
                    ),
                ],

                source_format=(
                    bigquery.SourceFormat
                    .NEWLINE_DELIMITED_JSON
                ),

                write_disposition=(
                    bigquery.WriteDisposition
                    .WRITE_TRUNCATE
                ),
            )

            records = [
                json.loads(line)
                for line in l1_jsonl.splitlines()
            ]

            load_job = client.load_table_from_json(
                records,
                staging_table,
                job_config=job_config,
            )

            load_job.result()

            create_final_sql = f"""
            CREATE OR REPLACE TABLE `{final_table}`
            AS
            SELECT
                earthquake_id,
                magnitude,
                place,
                TIMESTAMP_MILLIS(event_time_ms)
                    AS event_time,
                TIMESTAMP_MILLIS(updated_time_ms)
                    AS updated_time,
                status,
                event_type,
                magnitude_type,
                tsunami,
                significance,
                longitude,
                latitude,
                depth_km
            FROM `{staging_table}`;
            """

            final_job = client.query(
                create_final_sql
            )

            final_job.result()

            end_time = datetime.now(timezone.utc)

            write_task_log(
                client=client,
                run_id=run_id,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=len(records),
                message=(
                    f"L1 data loaded into "
                    f"{final_table}"
                ),
            )

            print(
                f"L1 data loaded successfully into "
                f"{final_table}"
            )

            print(
                f"Records loaded: "
                f"{len(records)}"
            )

        except Exception as e:

            end_time = datetime.now(timezone.utc)

            try:

                client = bigquery.Client(
                    project=PROJECT_ID
                )

                write_task_log(
                    client=client,
                    run_id=run_id,
                    task_name=task_name,
                    status="FAILED",
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=0,
                    message=str(e),
                )

            except Exception as log_error:

                print(
                    f"Unable to write failure log: "
                    f"{log_error}"
                )

            raise

    # ============================================================
    # TASK 4
    # DATA QUALITY VALIDATION
    # ============================================================

    @task
    def validate_l1_data():

        from google.cloud import bigquery

        context = get_current_context()
        dag_run = context["dag_run"]

        # Use the Airflow DAG run ID as the single pipeline run ID.
        run_id = dag_run.run_id
        start_time = datetime.now(timezone.utc)

        task_name = "validate_l1_data"

        client = bigquery.Client(
            project=PROJECT_ID
        )

        source_table = (
            f"{PROJECT_ID}."
            f"earthquake_raw."
            f"earthquakes"
        )

        validation_table = (
            f"{PROJECT_ID}."
            f"earthquake_raw."
            f"validation_results"
        )

        failed_records_table = (
            f"{PROJECT_ID}."
            f"earthquake_raw."
            f"failed_records"
        )

        try:

            # ----------------------------------------------------
            # Make retries idempotent.
            # If this task partially completed before retrying,
            # remove validation output for this exact run first.
            # ----------------------------------------------------

            cleanup_sql = f"""
            DELETE FROM `{validation_table}`
            WHERE run_id = @run_id;

            DELETE FROM `{failed_records_table}`
            WHERE run_id = @run_id;
            """

            query_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "run_id",
                        "STRING",
                        run_id,
                    )
                ]
            )

            client.query(
                cleanup_sql,
                job_config=query_config,
            ).result()

            # ----------------------------------------------------
            # Validation results
            # IMPORTANT: validation failures are recorded as data
            # quality results and do NOT fail the Airflow task.
            # ----------------------------------------------------

            validation_sql = f"""
            INSERT INTO `{validation_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
                'earthquake_id_not_null',
                COUNTIF(earthquake_id IS NULL),
                IF(
                    COUNTIF(earthquake_id IS NULL) = 0,
                    'PASS',
                    'FAIL'
                ),
                'earthquake_id must not be NULL'
            FROM `{source_table}`;


            INSERT INTO `{validation_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
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
            FROM `{source_table}`;


            INSERT INTO `{validation_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
                'event_time_not_null',
                COUNTIF(event_time IS NULL),
                IF(
                    COUNTIF(event_time IS NULL) = 0,
                    'PASS',
                    'FAIL'
                ),
                'event_time must not be NULL'
            FROM `{source_table}`;


            INSERT INTO `{validation_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
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
            FROM `{source_table}`;


            INSERT INTO `{validation_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
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
            FROM `{source_table}`;


            INSERT INTO `{validation_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
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
                'depth_km must be between -100 and 1000'
            FROM `{source_table}`;


            -- --------------------------------------------------
            -- Store each invalid record once with all applicable
            -- validation failure reasons.
            -- --------------------------------------------------

            INSERT INTO `{failed_records_table}`
            SELECT
                @run_id,
                CURRENT_TIMESTAMP(),
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
                CONCAT(
                    IF(
                        earthquake_id IS NULL,
                        'earthquake_id is NULL; ',
                        ''
                    ),
                    IF(
                        magnitude IS NULL
                        OR magnitude < -2
                        OR magnitude > 10,
                        'invalid magnitude; ',
                        ''
                    ),
                    IF(
                        event_time IS NULL,
                        'event_time is NULL; ',
                        ''
                    ),
                    IF(
                        latitude IS NULL
                        OR latitude < -90
                        OR latitude > 90,
                        'invalid latitude; ',
                        ''
                    ),
                    IF(
                        longitude IS NULL
                        OR longitude < -180
                        OR longitude > 180,
                        'invalid longitude; ',
                        ''
                    ),
                    IF(
                        depth_km IS NULL
                        OR depth_km < -100
                        OR depth_km > 1000,
                        'invalid depth; ',
                        ''
                    )
                ) AS failure_reason
            FROM `{source_table}`
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
            """

            client.query(
                validation_sql,
                job_config=query_config,
            ).result()

            # ----------------------------------------------------
            # Summarize validation outcome for this run.
            # ----------------------------------------------------

            summary_sql = f"""
            SELECT
                (SELECT COUNT(*) FROM `{source_table}`) AS total_records,
                COUNT(*) AS total_checks,
                COUNTIF(status = 'FAIL') AS failed_checks,
                (
                    SELECT COUNT(*)
                    FROM `{failed_records_table}`
                    WHERE run_id = @run_id
                ) AS failed_record_count
            FROM `{validation_table}`
            WHERE run_id = @run_id
            """

            summary = list(
                client.query(
                    summary_sql,
                    job_config=query_config,
                ).result()
            )[0]

            total_records = summary.total_records
            failed_checks = summary.failed_checks
            failed_records = summary.failed_record_count
            valid_records = total_records - failed_records

            end_time = datetime.now(timezone.utc)

            write_task_log(
                client=client,
                run_id=run_id,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=total_records,
                message=(
                    f"Validation completed. "
                    f"Input records: {total_records}, "
                    f"checks: {summary.total_checks}, "
                    f"failed checks: {failed_checks}, "
                    f"failed records: {failed_records}, "
                    f"valid records: {valid_records}. "
                    f"Data quality failures were recorded "
                    f"without failing the task."
                ),
            )

            print(
                f"Validation completed for run: {run_id}"
            )

            print(
                f"Total validation checks: "
                f"{summary.total_checks}"
            )

            print(
                f"Failed checks: {failed_checks}"
            )

            print(
                f"Failed records: {failed_records}"
            )

            print(
                f"Valid records: {valid_records}"
            )

        except Exception as e:

            end_time = datetime.now(timezone.utc)

            try:
                write_task_log(
                    client=client,
                    run_id=run_id,
                    task_name=task_name,
                    status="FAILED",
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=0,
                    message=str(e),
                )
            except Exception as log_error:
                print(
                    f"Unable to write failure log: "
                    f"{log_error}"
                )

            # Technical failures fail the Airflow task.
            raise

    # ============================================================
    # TASK 5
    # L2 CURRENT + HISTORY
    # ============================================================

    @task
    def build_l2_curated():

        from google.cloud import bigquery

        context = get_current_context()
        dag_run = context["dag_run"]

        # Use the same Airflow run ID used by every other task.
        run_id = dag_run.run_id
        start_time = datetime.now(timezone.utc)

        task_name = "build_l2_curated"

        client = bigquery.Client(
            project=PROJECT_ID
        )

        source_table = (
            f"{PROJECT_ID}."
            f"earthquake_raw."
            f"earthquakes"
        )

        failed_records_table = (
            f"{PROJECT_ID}."
            f"earthquake_raw."
            f"failed_records"
        )

        current_table = (
            f"{PROJECT_ID}."
            f"earthquake_curated."
            f"earthquakes_curated"
        )

        history_table = (
            f"{PROJECT_ID}."
            f"earthquake_curated."
            f"earthquakes_history"
        )

        try:

            # ----------------------------------------------------
            # Create history table if it does not already exist.
            # ----------------------------------------------------

            create_history_sql = f"""
            CREATE TABLE IF NOT EXISTS `{history_table}`
            (
                earthquake_id STRING,
                magnitude FLOAT64,
                place STRING,
                event_time TIMESTAMP,
                updated_time TIMESTAMP,
                status STRING,
                event_type STRING,
                magnitude_type STRING,
                tsunami INT64,
                significance INT64,
                longitude FLOAT64,
                latitude FLOAT64,
                depth_km FLOAT64,
                event_date DATE,
                event_year INT64,
                event_month INT64,
                magnitude_category STRING,
                tsunami_flag BOOL,
                location GEOGRAPHY,
                depth_category STRING,
                load_time TIMESTAMP
            );
            """

            client.query(
                create_history_sql
            ).result()

            # ----------------------------------------------------
            # Build the current validated L2 dataset using the
            # failed_records belonging to THIS Airflow run.
            # ----------------------------------------------------

            build_current_sql = f"""
            CREATE OR REPLACE TABLE `{current_table}`
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
                    WHEN e.magnitude < 2.0
                        THEN 'Micro'
                    WHEN e.magnitude < 4.0
                        THEN 'Minor'
                    WHEN e.magnitude < 5.0
                        THEN 'Light'
                    WHEN e.magnitude < 6.0
                        THEN 'Moderate'
                    WHEN e.magnitude < 7.0
                        THEN 'Strong'
                    ELSE 'Major'
                END AS magnitude_category,

                e.tsunami = 1 AS tsunami_flag,

                ST_GEOGPOINT(
                    e.longitude,
                    e.latitude
                ) AS location,

                CASE
                    WHEN e.depth_km < 70
                        THEN 'Shallow'
                    WHEN e.depth_km < 300
                        THEN 'Intermediate'
                    ELSE 'Deep'
                END AS depth_category,

                CURRENT_TIMESTAMP() AS load_time

            FROM `{source_table}` e

            WHERE NOT EXISTS (
                SELECT 1
                FROM `{failed_records_table}` f
                WHERE
                    f.run_id = @run_id
                    AND f.earthquake_id = e.earthquake_id
            );
            """

            query_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "run_id",
                        "STRING",
                        run_id,
                    )
                ]
            )

            client.query(
                build_current_sql,
                job_config=query_config,
            ).result()

            # ----------------------------------------------------
            # Merge the current validated records into history.
            # The earthquake_id is the business key, making the
            # operation safe to retry.
            # ----------------------------------------------------

            merge_history_sql = f"""
            MERGE `{history_table}` AS target

            USING `{current_table}` AS source

            ON target.earthquake_id = source.earthquake_id

            WHEN MATCHED THEN
                UPDATE SET
                    target.magnitude = source.magnitude,
                    target.place = source.place,
                    target.event_time = source.event_time,
                    target.updated_time = source.updated_time,
                    target.status = source.status,
                    target.event_type = source.event_type,
                    target.magnitude_type = source.magnitude_type,
                    target.tsunami = source.tsunami,
                    target.significance = source.significance,
                    target.longitude = source.longitude,
                    target.latitude = source.latitude,
                    target.depth_km = source.depth_km,
                    target.event_date = source.event_date,
                    target.event_year = source.event_year,
                    target.event_month = source.event_month,
                    target.magnitude_category = source.magnitude_category,
                    target.tsunami_flag = source.tsunami_flag,
                    target.location = source.location,
                    target.depth_category = source.depth_category,
                    target.load_time = source.load_time

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
                    depth_category,
                    load_time
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
                    source.depth_category,
                    source.load_time
                );
            """

            client.query(
                merge_history_sql
            ).result()

            # ----------------------------------------------------
            # Count records written to current L2.
            # ----------------------------------------------------

            count_query = f"""
            SELECT COUNT(*) AS record_count
            FROM `{current_table}`
            """

            current_count = list(
                client.query(count_query).result()
            )[0].record_count

            end_time = datetime.now(timezone.utc)

            write_task_log(
                client=client,
                run_id=run_id,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=current_count,
                message=(
                    "L2 current and history tables "
                    "updated successfully."
                ),
            )

            print(
                "L2 current table updated successfully."
            )

            print(
                "L2 history table updated successfully."
            )

            print(
                f"Current records: {current_count}"
            )

        except Exception as e:

            end_time = datetime.now(timezone.utc)

            try:
                write_task_log(
                    client=client,
                    run_id=run_id,
                    task_name=task_name,
                    status="FAILED",
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=0,
                    message=str(e),
                )
            except Exception as log_error:
                print(
                    f"Unable to write failure log: "
                    f"{log_error}"
                )

            raise

    # ============================================================
    # TASK 6
    # L2 HISTORY -> L3 ANALYTICS
    # ============================================================

    @task
    def build_l3_analytics():

        from google.cloud import bigquery

        context = get_current_context()
        dag_run = context["dag_run"]

        run_id = dag_run.run_id
        start_time = datetime.now(timezone.utc)

        task_name = "build_l3_analytics"

        client = bigquery.Client(
            project=PROJECT_ID
        )

        source_table = (
            f"{PROJECT_ID}."
            f"earthquake_curated."
            f"earthquakes_history"
        )

        daily_table = (
            f"{PROJECT_ID}."
            f"earthquake_analytics."
            f"daily_earthquake_activity"
        )

        magnitude_table = (
            f"{PROJECT_ID}."
            f"earthquake_analytics."
            f"magnitude_distribution"
        )

        significant_table = (
            f"{PROJECT_ID}."
            f"earthquake_analytics."
            f"significant_events"
        )

        try:

            # --------------------------------------------------------
            # Build all L3 analytics tables from L2 history.
            # L3 is therefore separated from the raw/L1 layers.
            # --------------------------------------------------------

            sql = f"""

            CREATE OR REPLACE TABLE `{daily_table}`
            AS
            SELECT
                event_date,
                COUNT(*) AS earthquake_count,
                ROUND(AVG(magnitude), 2) AS average_magnitude,
                ROUND(MAX(magnitude), 2) AS maximum_magnitude,
                ROUND(MIN(magnitude), 2) AS minimum_magnitude,
                COUNTIF(significance >= 600)
                    AS significant_earthquake_count,
                COUNTIF(tsunami_flag = TRUE)
                    AS tsunami_event_count
            FROM `{source_table}`
            GROUP BY event_date
            ORDER BY event_date;


            CREATE OR REPLACE TABLE `{magnitude_table}`
            AS
            SELECT
                magnitude_category,
                COUNT(*) AS earthquake_count,
                ROUND(AVG(magnitude), 2) AS average_magnitude,
                ROUND(MAX(magnitude), 2) AS maximum_magnitude,
                ROUND(MIN(magnitude), 2) AS minimum_magnitude
            FROM `{source_table}`
            GROUP BY magnitude_category
            ORDER BY
                CASE magnitude_category
                    WHEN 'Micro' THEN 1
                    WHEN 'Minor' THEN 2
                    WHEN 'Light' THEN 3
                    WHEN 'Moderate' THEN 4
                    WHEN 'Strong' THEN 5
                    WHEN 'Major' THEN 6
                END;


            CREATE OR REPLACE TABLE `{significant_table}`
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
            FROM `{source_table}`
            WHERE
                magnitude >= 4.0
                OR significance >= 600
                OR tsunami_flag = TRUE
            ORDER BY event_time DESC;

            """

            client.query(sql).result()

            # --------------------------------------------------------
            # Count source records for the audit log.
            # --------------------------------------------------------

            count_sql = f"""
            SELECT COUNT(*) AS record_count
            FROM `{source_table}`
            """

            record_count = list(
                client.query(count_sql).result()
            )[0].record_count

            end_time = datetime.now(timezone.utc)

            write_task_log(
                client=client,
                run_id=run_id,
                task_name=task_name,
                status="SUCCESS",
                start_time=start_time,
                end_time=end_time,
                records_processed=record_count,
                message=(
                    "L3 analytics tables built successfully: "
                    "daily_earthquake_activity, "
                    "magnitude_distribution, "
                    "significant_events."
                ),
            )

            print(
                "L3 analytics tables built successfully."
            )

            print(
                f"L2 history records processed: {record_count}"
            )

        except Exception as e:

            end_time = datetime.now(timezone.utc)

            try:
                write_task_log(
                    client=client,
                    run_id=run_id,
                    task_name=task_name,
                    status="FAILED",
                    start_time=start_time,
                    end_time=end_time,
                    records_processed=0,
                    message=str(e),
                )
            except Exception as log_error:
                print(
                    f"Unable to write failure log: "
                    f"{log_error}"
                )

            raise

    # ============================================================
    # DEPENDENCIES
    # ============================================================

    raw_task = extract_api_and_upload_raw()

    l1_task = load_l1_snapshot()

    bq_l1_task = load_l1_to_bigquery()

    validation_task = validate_l1_data()

    l2_task = build_l2_curated()

    l3_task = build_l3_analytics()


    raw_task >> l1_task >> bq_l1_task >> validation_task >> l2_task >> l3_task