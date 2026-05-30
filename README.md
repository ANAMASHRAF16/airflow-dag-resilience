# Airflow DAG Resilience (Activity 5)

A daily Airflow DAG that enriches sales records by invoking a downstream Lambda. The baseline (`main` branch) demonstrates the anti-pattern Activity 5 fixes: when the Lambda times out, the DAG fails silently, no retries fire, no on-call is paged, and the failure is only noticed the next morning when someone happens to look at the dashboard.

The fix branch adds four production-grade resilience features:

- **Retries with exponential backoff** — transient Lambda timeouts auto-recover without human intervention
- **Slack `on_failure_callback`** — every task-level failure pages the team in real time
- **`execution_timeout`** on the Lambda task — a hung call cancels itself instead of pinning a worker
- **Structured JSON logging** — every log line is a queryable record in CloudWatch Logs Insights / Datadog

## Local development

```bash
pip install -r requirements.txt

# Run the DAG-structure tests (no Airflow scheduler needed)
pytest tests/ -v

# Optional: load the DAG into a local Airflow instance
export AIRFLOW_HOME=$(pwd)
airflow db migrate
airflow dags list-import-errors
```

See `dags/sales_enrichment_dag.py` for the DAG definition and `plugins/` for the failure callback + logging config.
