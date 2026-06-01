"""Sales enrichment DAG — FIXED version.

What changed from the baseline:

1. RETRIES with exponential backoff
   - retries=3, retry_delay=5min, retry_exponential_backoff=True,
     max_retry_delay=30min
   - A transient Lambda timeout now recovers automatically on retry,
     with the gap growing 5m → 10m → 20m to avoid hammering a
     struggling downstream.

2. on_failure_callback for Slack
   - Posts a Block Kit alert to the SLACK_ALERT_WEBHOOK_URL endpoint
     with DAG/task/try-number/exception. Real-time visibility instead
     of "noticed the red square the next morning".
   - Fires AFTER all retries are exhausted, so transient blips don't
     spam the channel.

3. execution_timeout on the Lambda task
   - Caps the Lambda invocation at 10 minutes. A hung Lambda call
     used to pin an Airflow worker indefinitely. Now it self-cancels
     and the retry logic kicks in.

4. STRUCTURED JSON logging
   - All log lines are one JSON object per event, queryable in
     CloudWatch Logs Insights or Datadog. Replaces print() statements
     that hid operational signal in prose.

All four mechanisms are config-driven and read from env vars where
appropriate, so the same DAG file works for dev (faster retries) and
production (real Slack channel).
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from plugins.callbacks import slack_on_failure
from plugins.logging_config import get_logger

log = get_logger("dags.sales_enrichment")


# ---------------------------------------------------------------------------
# Task functions - now using structured logging instead of print()
# ---------------------------------------------------------------------------

def extract_sales(**context):
    log.info("extract_started", extra={"run_id": context.get("run_id")})
    records = [{"order_id": f"ORD-{i:04d}", "amount": 100 + i} for i in range(50)]
    log.info("extract_completed", extra={"record_count": len(records)})
    return records


def call_lambda_for_enrichment(**context):
    """Invoke the downstream enrichment Lambda.

    execution_timeout on this task (set via default_args) caps the whole
    invocation. retries kick in automatically on TimeoutError.
    """
    ti = context["task_instance"]
    records = ti.xcom_pull(task_ids="extract_sales") or []
    log.info(
        "lambda_invoked",
        extra={
            "record_count": len(records),
            "try_number": ti.try_number,
            "lambda_function": os.environ.get("ENRICHMENT_LAMBDA_NAME", "sales-enrichment"),
        },
    )

    # In production this is boto3.client('lambda').invoke(...). For the
    # demo we simulate the call; the real implementation would propagate
    # the Lambda's response payload back via XCom.
    import time
    time.sleep(0.1)

    log.info("lambda_returned", extra={"record_count": len(records), "duration_ms": 100})
    return {"enriched_count": len(records)}


def write_results(**context):
    ti = context["task_instance"]
    result = ti.xcom_pull(task_ids="call_lambda") or {}
    log.info(
        "results_written",
        extra={"enriched_count": result.get("enriched_count", 0), "destination": "analytics.sales_enriched"},
    )


# ---------------------------------------------------------------------------
# DAG definition with the four resilience features
# ---------------------------------------------------------------------------

# Env-driven so dev / prod can tune independently without code changes
DEFAULT_RETRIES = int(os.environ.get("DAG_DEFAULT_RETRIES", "3"))
DEFAULT_RETRY_DELAY_MIN = int(os.environ.get("DAG_RETRY_DELAY_MIN", "5"))
DEFAULT_MAX_RETRY_DELAY_MIN = int(os.environ.get("DAG_MAX_RETRY_DELAY_MIN", "30"))
DEFAULT_EXECUTION_TIMEOUT_MIN = int(os.environ.get("DAG_EXECUTION_TIMEOUT_MIN", "10"))

default_args = {
    "owner": "data-platform",
    "retries": DEFAULT_RETRIES,
    "retry_delay": timedelta(minutes=DEFAULT_RETRY_DELAY_MIN),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=DEFAULT_MAX_RETRY_DELAY_MIN),
    # on_failure_callback fires only after all retries are exhausted -
    # transient blips don't spam the channel
    "on_failure_callback": slack_on_failure,
    # execution_timeout caps an individual task instance; hung Lambda
    # calls self-cancel and let the retry logic try again
    "execution_timeout": timedelta(minutes=DEFAULT_EXECUTION_TIMEOUT_MIN),
}

dag = DAG(
    dag_id="sales_enrichment_fixed",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["sales", "enrichment", "resilient"],
    doc_md=__doc__,
)

extract = PythonOperator(
    task_id="extract_sales",
    python_callable=extract_sales,
    dag=dag,
)

enrich = PythonOperator(
    task_id="call_lambda",
    python_callable=call_lambda_for_enrichment,
    dag=dag,
)

write = PythonOperator(
    task_id="write_results",
    python_callable=write_results,
    dag=dag,
)

extract >> enrich >> write
