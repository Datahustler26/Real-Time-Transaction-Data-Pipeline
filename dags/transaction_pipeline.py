"""
Transaction Pipeline DAG
========================
Main Airflow DAG that orchestrates the end-to-end ETL pipeline for
processing financial transactions.

Pipeline Flow:
    ingest_raw → validate_raw → stage_transactions →
    [load_dim_customer, load_dim_merchant] → load_fact_transactions →
    run_quality_checks → update_monitoring_metrics

Schedule: Every 15 minutes
SLA: < 2 minutes end-to-end
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

# --- Local imports ---
from common.config import PipelineConfig
from utils.snowflake_utils import SnowflakeManager
from utils.data_quality import DataQualityValidator
from utils.slack_alerts import (
    on_failure_callback,
    on_sla_miss_callback,
    send_success_alert,
)

# ============================================
# DAG Configuration
# ============================================
SQL_DIR = Path("/opt/airflow/sql")
CONFIG = PipelineConfig()

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [CONFIG.ALERT_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": CONFIG.MAX_RETRIES,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": on_failure_callback,
    "sla": timedelta(minutes=2),
}


# ============================================
# Task Functions
# ============================================
def ingest_raw_data(**context):
    """
    Pull raw transaction data from S3 staging area into Snowflake raw layer.
    Handles incremental loads based on last processed timestamp.
    """
    sf = SnowflakeManager()
    execution_date = context["execution_date"]

    # Get last processed watermark
    last_watermark = Variable.get(
        "last_processed_watermark",
        default_var=execution_date - timedelta(hours=1),
    )

    # Copy raw data from S3 stage to Snowflake
    ingest_query = f"""
        COPY INTO {CONFIG.RAW_SCHEMA}.raw_transactions
        FROM @{CONFIG.RAW_SCHEMA}.transaction_stage
        FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1
                       FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                       NULL_IF = ('NULL', 'null', ''))
        PATTERN = '.*transactions_.*\\.csv'
        ON_ERROR = 'CONTINUE'
        PURGE = FALSE;
    """

    result = sf.execute_query(ingest_query)
    rows_loaded = result.get("rows_loaded", 0) if result else 0

    # Push metrics to XCom
    context["ti"].xcom_push(key="rows_ingested", value=rows_loaded)
    context["ti"].xcom_push(key="ingest_timestamp", value=str(datetime.utcnow()))

    # Update watermark
    Variable.set("last_processed_watermark", str(execution_date))

    print(f"✅ Ingested {rows_loaded} rows at {execution_date}")
    return rows_loaded


def validate_raw_data(**context):
    """
    Perform schema validation and basic sanity checks on raw data.
    Catches malformed records before transformation.
    """
    sf = SnowflakeManager()

    # Count records in current batch
    count_query = f"""
        SELECT COUNT(*) as record_count,
               COUNT(DISTINCT transaction_id) as unique_txns,
               MIN(transaction_timestamp) as min_ts,
               MAX(transaction_timestamp) as max_ts,
               SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END) as null_ids,
               SUM(CASE WHEN amount IS NULL THEN 1 ELSE 0 END) as null_amounts
        FROM {CONFIG.RAW_SCHEMA}.raw_transactions
        WHERE _loaded_at >= DATEADD('minute', -15, CURRENT_TIMESTAMP());
    """

    result = sf.execute_query(count_query)

    if result:
        record_count = result.get("record_count", 0)
        null_ids = result.get("null_ids", 0)

        if null_ids > 0:
            print(f"⚠️ Found {null_ids} records with NULL transaction IDs")

        if record_count == 0:
            raise ValueError("No records found in current batch — possible ingestion failure")

        context["ti"].xcom_push(key="validated_count", value=record_count)
        print(f"✅ Validated {record_count} records")
        return record_count

    raise ValueError("Raw validation query returned no results")


def stage_transactions(**context):
    """
    Transform raw data into clean staging tables.
    Handles: deduplication, type casting, null imputation, timestamp normalization.
    """
    sf = SnowflakeManager()
    sql_path = SQL_DIR / "staging" / "stg_raw_transactions.sql"
    result = sf.run_sql_file(str(sql_path))

    rows_staged = result.get("rows_affected", 0) if result else 0
    context["ti"].xcom_push(key="rows_staged", value=rows_staged)

    print(f"✅ Staged {rows_staged} transactions")
    return rows_staged


def load_dim_customer(**context):
    """
    Load customer dimension with SCD Type 2 logic.
    Tracks historical changes with effective/expiry dates.
    """
    sf = SnowflakeManager()
    sql_path = SQL_DIR / "marts" / "dim_customer.sql"
    result = sf.run_sql_file(str(sql_path))

    # Also run customer master staging
    master_path = SQL_DIR / "staging" / "stg_customer_master.sql"
    sf.run_sql_file(str(master_path))

    rows_merged = result.get("rows_affected", 0) if result else 0
    context["ti"].xcom_push(key="dim_customer_rows", value=rows_merged)

    print(f"✅ Loaded {rows_merged} customer dimension records (SCD Type 2)")
    return rows_merged


def load_dim_merchant(**context):
    """
    Load merchant dimension table.
    Includes category hierarchy and geographic attributes.
    """
    sf = SnowflakeManager()
    sql_path = SQL_DIR / "marts" / "dim_merchant.sql"
    result = sf.run_sql_file(str(sql_path))

    rows_merged = result.get("rows_affected", 0) if result else 0
    context["ti"].xcom_push(key="dim_merchant_rows", value=rows_merged)

    print(f"✅ Loaded {rows_merged} merchant dimension records")
    return rows_merged


def load_fact_transactions(**context):
    """
    Load fact table with surrogate key lookups from dimensions.
    Handles late-arriving facts and orphan records.
    """
    sf = SnowflakeManager()
    sql_path = SQL_DIR / "marts" / "fct_transactions.sql"
    result = sf.run_sql_file(str(sql_path))

    rows_loaded = result.get("rows_affected", 0) if result else 0
    context["ti"].xcom_push(key="fact_rows_loaded", value=rows_loaded)

    print(f"✅ Loaded {rows_loaded} fact records")
    return rows_loaded


def run_quality_checks(**context):
    """
    Execute Great Expectations validation suite.
    15 checks covering completeness, accuracy, consistency, timeliness.
    """
    validator = DataQualityValidator()
    sf = SnowflakeManager()

    # Pull latest fact data for validation
    sample_query = f"""
        SELECT * FROM {CONFIG.MART_SCHEMA}.fct_transactions
        WHERE processed_at >= DATEADD('minute', -30, CURRENT_TIMESTAMP())
        LIMIT 50000;
    """

    data = sf.execute_query_to_df(sample_query)

    if data is not None and not data.empty:
        results = validator.run_all_checks(data)

        passed = sum(1 for r in results if r["success"])
        failed = sum(1 for r in results if not r["success"])

        context["ti"].xcom_push(key="quality_passed", value=passed)
        context["ti"].xcom_push(key="quality_failed", value=failed)
        context["ti"].xcom_push(key="quality_results", value=results)

        if failed > 0:
            failed_checks = [r["check_name"] for r in results if not r["success"]]
            error_msg = f"❌ {failed} quality checks failed: {', '.join(failed_checks)}"
            print(error_msg)
            # Don't raise — log and alert, let downstream decide
            context["ti"].xcom_push(key="quality_status", value="WARNING")
        else:
            print(f"✅ All {passed} quality checks passed")
            context["ti"].xcom_push(key="quality_status", value="PASSED")

        return {"passed": passed, "failed": failed, "results": results}
    else:
        print("⚠️ No data available for quality checks")
        context["ti"].xcom_push(key="quality_status", value="SKIPPED")
        return {"passed": 0, "failed": 0, "results": []}


def update_monitoring_metrics(**context):
    """
    Push pipeline metrics to Prometheus and update monitoring tables.
    """
    sf = SnowflakeManager()

    # Update data quality metrics table
    quality_sql = SQL_DIR / "monitoring" / "data_quality_metrics.sql"
    sf.run_sql_file(str(quality_sql))

    # Update SLA metrics table
    sla_sql = SQL_DIR / "monitoring" / "pipeline_sla_metrics.sql"
    sf.run_sql_file(str(sla_sql))

    # Collect XCom metrics from upstream tasks
    ti = context["ti"]
    metrics = {
        "rows_ingested": ti.xcom_pull(key="rows_ingested", task_ids="ingest_raw") or 0,
        "rows_staged": ti.xcom_pull(key="rows_staged", task_ids="stage_transactions") or 0,
        "fact_rows": ti.xcom_pull(key="fact_rows_loaded", task_ids="load_facts.load_fact_transactions") or 0,
        "quality_passed": ti.xcom_pull(key="quality_passed", task_ids="run_quality_checks") or 0,
        "quality_failed": ti.xcom_pull(key="quality_failed", task_ids="run_quality_checks") or 0,
    }

    # Push to Prometheus (via pushgateway or custom exporter)
    try:
        from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

        registry = CollectorRegistry()

        for metric_name, value in metrics.items():
            gauge = Gauge(
                f"pipeline_{metric_name}",
                f"Pipeline metric: {metric_name}",
                registry=registry,
            )
            gauge.set(value)

        push_to_gateway(
            CONFIG.PROMETHEUS_PUSHGATEWAY,
            job="transaction_pipeline",
            registry=registry,
        )
        print("✅ Metrics pushed to Prometheus")
    except Exception as e:
        print(f"⚠️ Failed to push metrics to Prometheus: {e}")

    print(f"✅ Monitoring metrics updated: {metrics}")
    return metrics


def notify_success(**context):
    """Send success notification with pipeline summary."""
    ti = context["ti"]
    metrics = ti.xcom_pull(
        key="return_value",
        task_ids="update_monitoring_metrics",
    )
    send_success_alert(context, metrics)


# ============================================
# DAG Definition
# ============================================
with DAG(
    dag_id="transaction_pipeline",
    default_args=default_args,
    description="End-to-end ETL pipeline for financial transaction processing",
    schedule_interval="*/15 * * * *",  # Every 15 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["production", "transactions", "etl", "snowflake"],
    sla_miss_callback=on_sla_miss_callback,
    doc_md=__doc__,
) as dag:

    # --- Start ---
    start = DummyOperator(task_id="start")

    # --- Ingest ---
    ingest_raw = PythonOperator(
        task_id="ingest_raw",
        python_callable=ingest_raw_data,
        provide_context=True,
    )

    # --- Validate ---
    validate_raw = PythonOperator(
        task_id="validate_raw",
        python_callable=validate_raw_data,
        provide_context=True,
    )

    # --- Stage ---
    stage_txns = PythonOperator(
        task_id="stage_transactions",
        python_callable=stage_transactions,
        provide_context=True,
    )

    # --- Load Dimensions (parallel) ---
    with TaskGroup(group_id="load_dimensions") as load_dims:
        dim_customer = PythonOperator(
            task_id="load_dim_customer",
            python_callable=load_dim_customer,
            provide_context=True,
        )

        dim_merchant = PythonOperator(
            task_id="load_dim_merchant",
            python_callable=load_dim_merchant,
            provide_context=True,
        )

    # --- Load Facts ---
    with TaskGroup(group_id="load_facts") as load_facts:
        fact_txns = PythonOperator(
            task_id="load_fact_transactions",
            python_callable=load_fact_transactions,
            provide_context=True,
        )

    # --- Quality Checks ---
    quality_checks = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
        provide_context=True,
    )

    # --- Update Metrics ---
    update_metrics = PythonOperator(
        task_id="update_monitoring_metrics",
        python_callable=update_monitoring_metrics,
        provide_context=True,
    )

    # --- Notify ---
    notify = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success,
        provide_context=True,
        trigger_rule="all_success",
    )

    # --- End ---
    end = DummyOperator(task_id="end", trigger_rule="none_failed")

    # ============================================
    # Task Dependencies
    # ============================================
    start >> ingest_raw >> validate_raw >> stage_txns
    stage_txns >> load_dims >> load_facts
    load_facts >> quality_checks >> update_metrics >> notify >> end
