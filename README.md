# Airflow DAG Resilience (Activity 5)

[![CI](https://github.com/ANAMASHRAF16/airflow-dag-resilience/actions/workflows/ci.yml/badge.svg)](https://github.com/ANAMASHRAF16/airflow-dag-resilience/actions/workflows/ci.yml)

A daily Airflow DAG that enriches sales records by invoking a downstream Lambda. The baseline (`main` branch) demonstrates the anti-pattern Activity 5 fixes: when the Lambda times out, the DAG fails silently, no retries fire, no on-call is paged, and the failure is only noticed the next morning when someone happens to look at the dashboard.

The fix branch adds four production-grade resilience features:

- **Retries with exponential backoff** — `retries=3`, `retry_delay=5m`, `retry_exponential_backoff=True`, `max_retry_delay=30m`. Transient Lambda timeouts auto-recover without human intervention; the gap between retries grows (5m → 10m → 20m) to avoid hammering a struggling downstream.
- **Slack `on_failure_callback`** — every task failure posts a Block Kit alert to a webhook with DAG id, task id, try number, and the exception. Fires only after retries are exhausted, so transient blips don't spam the channel.
- **`execution_timeout`** — caps the Lambda task at 10 minutes. A hung call self-cancels and the retry logic takes over, instead of pinning an Airflow worker indefinitely.
- **Structured JSON logging** — every log line is one JSON object with `timestamp`, `level`, `logger`, `message`, plus any `extra` fields the caller passed. Replaces `print()` so CloudWatch Logs Insights and Datadog can query operational events as columns.

## Local development

```bash
pip install -r requirements.txt

# Run the DAG-structure tests (no Airflow scheduler needed)
export AIRFLOW_HOME=$(pwd)/.airflow_test_home
pytest tests/ -v

# Optional: load the DAG into a local Airflow webserver
airflow db migrate
airflow dags list-import-errors          # should show 0 errors
airflow webserver --port 8080            # browse to localhost:8080
```

See `dags/sales_enrichment_dag.py` for the DAG definition and `plugins/` for the failure callback + structured logging config.

## Environment variables

| Var | Default | Purpose |
|---|---|---|
| `SLACK_ALERT_WEBHOOK_URL` | unset | Slack webhook URL; if unset the callback logs a warning and returns without raising |
| `ENRICHMENT_LAMBDA_NAME` | `sales-enrichment` | Lambda function name (surfaced in structured logs for traceability) |
| `DAG_DEFAULT_RETRIES` | `3` | Override retry count for dev environments (set lower for faster iteration) |
| `DAG_RETRY_DELAY_MIN` | `5` | First retry delay in minutes; subsequent retries grow exponentially |
| `DAG_MAX_RETRY_DELAY_MIN` | `30` | Cap on retry delay so backoff doesn't push the next try days out |
| `DAG_EXECUTION_TIMEOUT_MIN` | `10` | Per-task timeout; should be slightly longer than the Lambda's own timeout |

## Trade-offs documented

- **Retries on the Airflow task, not inside the Lambda invocation.** Airflow's retry mechanism is observable in the UI (each try shows as a separate run instance), survives Airflow worker restarts, and integrates with the SLA and alerting machinery. A retry loop inside `boto3.invoke()` would hide the failures from Airflow.
- **on_failure_callback fires only after all retries are exhausted.** This is the default Airflow behaviour and the right one — transient blips that the retry logic recovers from shouldn't page anyone. If you want per-retry alerts, add `on_retry_callback` separately.
- **Slack webhook over Slack SDK.** Webhooks need only a URL — no API token, no scopes, no token rotation. The trade-off is fewer features (no DM-style routing, no rich interactivity beyond Block Kit), which is fine for an alerting use case.
- **Structured JSON via Python's stdlib logging.** Could have used `structlog` or `loguru` for richer ergonomics. Stdlib `logging` has zero added dependencies, integrates seamlessly with Airflow's own logger, and the per-line JSON output is what downstream log indexers want anyway.
