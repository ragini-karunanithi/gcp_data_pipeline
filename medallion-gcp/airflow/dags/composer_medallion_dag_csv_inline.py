from __future__ import annotations
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import ShortCircuitOperator, PythonOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSDeleteObjectsOperator
from airflow.providers.google.cloud.operators.bigquery import GCSToBigQueryOperator, BigQueryInsertJobOperator, BigQueryCheckOperator

PROJECT_ID = Variable.get('gcp_project', default_var='your_project')
BUCKET = Variable.get('gcs_raw_bucket', default_var='consumer-analytics-raw')
LANDING_PREFIX = Variable.get('gcs_landing_prefix', default_var='landing/transactions/')
BRONZE_TABLE = f"{PROJECT_ID}.bronze.transactions_raw"
SILVER_TABLE = f"{PROJECT_ID}.silver.transactions_clean"
GOLD_TABLE = f"{PROJECT_ID}.gold.sales_summary"

BRONZE_SCHEMA = [
    {'name': 'custno', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'txnno', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'event_ts', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'ingest_file', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': '_ingest_ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
]

DEFAULT_ARGS = {
    'owner': 'data-eng',
    'depends_on_past': False,
    'email_on_failure': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='medallion_composer_pipeline_csv_inline',
    default_args=DEFAULT_ARGS,
    description='Medallion pipeline: CSV ingestion from GCS into BigQuery using inline SQL',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
) as dag:

    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket=BUCKET,
        prefix=LANDING_PREFIX,
        gcp_conn_id='google_cloud_default',
    )

    def _has_files(file_list: list[str] | None) -> bool:
        """
        Checks if the provided list of files is not empty or None.

        Args:
            file_list (list[str] | None): A list of file names or None.

        Returns:
            bool: True if file_list is not None and contains at least one file, False otherwise.
        """
        logging.info('GCS files: %s', file_list)
        return bool(file_list)

    check_files = ShortCircuitOperator(
        task_id='check_files',
        python_callable=_has_files,
        op_args=['{{ ti.xcom_pull(task_ids="list_files") }}'],
    )

    load_to_bronze = GCSToBigQueryOperator(
        task_id='load_to_bronze',
        bucket=BUCKET,
        source_objects=[f"{LANDING_PREFIX}*.csv"],
        destination_project_dataset_table=BRONZE_TABLE,
        schema_fields=BRONZE_SCHEMA,
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1,
        field_delimiter=',',
        source_format='CSV',
        create_disposition='CREATE_IF_NEEDED',
        allow_quoted_newlines=True,
        gcp_conn_id='google_cloud_default',
    )

    bronze_to_silver_sql = f"""
    CREATE OR REPLACE TABLE `{SILVER_TABLE}` AS
    SELECT
      TO_HEX(SHA256(CONCAT(COALESCE(txnno, ''), COALESCE(custno, '')))) AS id,
      custno AS user_id,
      SAFE_CAST(amount AS FLOAT64) AS amount,
      PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', TRIM(event_ts)) AS event_ts,
      CURRENT_TIMESTAMP() AS ingest_ts
    FROM `{BRONZE_TABLE}`
    WHERE amount IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txnno ORDER BY _ingest_ts DESC) = 1;
    """

    silver_transform = BigQueryInsertJobOperator(
        task_id='silver_transform',
        configuration={'query': {'query': bronze_to_silver_sql, 'useLegacySql': False}},
        gcp_conn_id='google_cloud_default',
    )

    silver_dq = BigQueryCheckOperator(
        task_id='silver_row_count_check',
        sql=f"SELECT COUNT(1) FROM `{SILVER_TABLE}`",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )

    silver_to_gold_sql = f"""
    CREATE OR REPLACE TABLE `{GOLD_TABLE}` AS
    SELECT
      user_id,
      DATE(event_ts) AS date,
      SUM(amount) AS total_amount,
      COUNT(1) AS txn_count
    FROM `{SILVER_TABLE}`
    GROUP BY user_id, DATE(event_ts);
    """

    gold_aggregate = BigQueryInsertJobOperator(
        task_id='gold_aggregate',
        configuration={'query': {'query': silver_to_gold_sql, 'useLegacySql': False}},
        gcp_conn_id='google_cloud_default',
    )

    gold_dq = BigQueryCheckOperator(
        task_id='gold_row_count_check',
        sql=f"SELECT COUNT(1) FROM `{GOLD_TABLE}`",
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )

    archive_files = GCSDeleteObjectsOperator(
        task_id='archive_files',
        bucket=BUCKET,
        object_name=f"{LANDING_PREFIX}*.csv",
        gcp_conn_id='google_cloud_default',
    )

    def _notify():
        logging.info('CSV Medallion DAG completed successfully')

    notify = PythonOperator(
        task_id='notify',
        python_callable=_notify,
    )

    list_files >> check_files >> load_to_bronze >> silver_transform >> silver_dq >> gold_aggregate >> gold_dq >> archive_files >> notify
# placeholder for main DAG (inline SQL)