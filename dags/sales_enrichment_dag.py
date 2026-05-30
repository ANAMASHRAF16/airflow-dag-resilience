"""Sales enrichment DAG — BROKEN baseline.

Triggers daily, invokes a downstream Lambda to enrich sales records,
writes results to a downstream table.

Problems with this DAG (Activity 5 will fix):
1. NO RETRIES — a single transient Lambda timeout fails the run silently.
2. NO on_failure_callback — when the task fails, nobody is paged.
3. NO execution_timeout — a hung Lambda call ties up an Airflow worker
   slot indefinitely.
4. print() everywhere — no structured logging, no JSON fields you can
   query in CloudWatch Logs Insights or Datadog.

Result: when production Lambda hits its 5-minute timeout on a bad batch,
the DAG fails, Airflow's UI shows it red, and nobody notices until
someone happens to look at the dashboard the next morning.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def extract_sales():
    print("Extracting sales records from upstream warehouse...")
    # In production: query warehouse, return rows
    return [{"order_id": f"ORD-{i:04d}", "amount": 100 + i} for i in range(50)]


def call_lambda_for_enrichment():
    """Invokes the downstream Lambda. Hard-coded timeout, no retry."""
    print("Invoking enrichment Lambda...")
    # In production this is boto3.client('lambda').invoke(...)
    # For the demo we simulate it - a real implementation would call
    # the LambdaInvokeFunctionOperator
    import time
    time.sleep(1)
    print("Lambda returned (in production this could hang for 5+ minutes)")


def write_results():
    print("Writing enriched results to downstream table...")


# No default_args - no retries, no execution_timeout, no callbacks
dag = DAG(
    dag_id="sales_enrichment_broken",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
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
