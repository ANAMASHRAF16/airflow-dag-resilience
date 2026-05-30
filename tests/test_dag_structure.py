"""DAG-bag structural tests.

Verify the fixed DAG has the four resilience features Activity 5
requires:

1. retries >= 3 with retry_exponential_backoff=True
2. on_failure_callback set to plugins.callbacks.slack_on_failure
3. execution_timeout configured
4. Uses structured JSON logger (not print)

We use Airflow's DagBag to parse the DAG file without running the
scheduler, which means the tests run fast in CI without any Airflow
backend setup.
"""

import sys
from datetime import timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def dag_bag():
    """Parse the DAGs directory once per module."""
    from airflow.models import DagBag
    bag = DagBag(dag_folder=str(ROOT / "dags"), include_examples=False)
    assert not bag.import_errors, f"DAG import errors: {bag.import_errors}"
    return bag


@pytest.fixture(scope="module")
def fixed_dag(dag_bag):
    dag = dag_bag.get_dag("sales_enrichment_fixed")
    assert dag is not None, "sales_enrichment_fixed DAG not found in DagBag"
    return dag


# ---------- 1. Retries with exponential backoff ----------

def test_default_retries_at_least_three(fixed_dag):
    """Every task inherits retries from default_args - must be >= 3."""
    assert fixed_dag.default_args.get("retries", 0) >= 3, (
        "Default retries must be >= 3 so a single transient Lambda timeout "
        "doesn't fail the run permanently."
    )


def test_retry_delay_is_configured(fixed_dag):
    delay = fixed_dag.default_args.get("retry_delay")
    assert isinstance(delay, timedelta), "retry_delay must be a timedelta"
    assert delay.total_seconds() > 0, "retry_delay must be positive"


def test_retry_exponential_backoff_enabled(fixed_dag):
    """Exponential backoff prevents hammering a struggling downstream."""
    assert fixed_dag.default_args.get("retry_exponential_backoff") is True, (
        "retry_exponential_backoff must be True so retry gaps grow on "
        "successive failures (5m -> 10m -> 20m)."
    )


def test_max_retry_delay_bounded(fixed_dag):
    """Exponential backoff without a cap would push the next retry to days out."""
    cap = fixed_dag.default_args.get("max_retry_delay")
    assert isinstance(cap, timedelta), "max_retry_delay must be a timedelta"
    assert cap.total_seconds() > 0, "max_retry_delay must be positive"


# ---------- 2. on_failure_callback for Slack ----------

def test_on_failure_callback_is_set(fixed_dag):
    cb = fixed_dag.default_args.get("on_failure_callback")
    assert cb is not None, (
        "on_failure_callback must be set so task failures page the team "
        "in real time."
    )
    # Verify it's our slack callback, not something accidentally swapped in
    assert cb.__name__ == "slack_on_failure", (
        f"Expected on_failure_callback to be slack_on_failure, got {cb.__name__}"
    )


def test_slack_callback_module_path(fixed_dag):
    cb = fixed_dag.default_args["on_failure_callback"]
    assert "plugins.callbacks" in cb.__module__, (
        f"Slack callback should live in plugins.callbacks, found in {cb.__module__}"
    )


# ---------- 3. execution_timeout ----------

def test_execution_timeout_configured(fixed_dag):
    timeout = fixed_dag.default_args.get("execution_timeout")
    assert isinstance(timeout, timedelta), (
        "execution_timeout must be a timedelta - a hung Lambda call without "
        "this pins an Airflow worker slot indefinitely."
    )
    assert timeout.total_seconds() > 0


def test_execution_timeout_reasonable(fixed_dag):
    """Timeout should be longer than expected Lambda runtime but bounded.

    Anything over 30 minutes is too lenient for an enrichment task.
    """
    timeout = fixed_dag.default_args["execution_timeout"]
    assert timeout.total_seconds() <= 30 * 60, (
        "execution_timeout > 30 min is probably too lenient for an "
        "enrichment task - tighten it and let retries handle the rest."
    )


# ---------- 4. Structured logging (no print() in dags/) ----------

def test_dag_file_uses_structured_logger_not_print():
    """Grep the dag source - print() should not appear in the fix."""
    dag_source = (ROOT / "dags" / "sales_enrichment_dag.py").read_text(encoding="utf-8")
    # The docstring mentions print() as the anti-pattern we're replacing,
    # which is allowed. We're checking for active code calls.
    code_lines = [
        line for line in dag_source.split("\n")
        if "print(" in line and not line.strip().startswith("#") and '"""' not in line
    ]
    # The docstring lines (within triple-quoted blocks) can contain the word
    # print as documentation - filter those out by tracking docstring state.
    in_docstring = False
    real_print_calls = []
    for line in dag_source.split("\n"):
        stripped = line.strip()
        if stripped.startswith('"""') or stripped.endswith('"""'):
            in_docstring = not in_docstring if stripped.count('"""') == 1 else in_docstring
            continue
        if in_docstring:
            continue
        if "print(" in line and not stripped.startswith("#"):
            real_print_calls.append(line.strip())
    assert not real_print_calls, (
        f"Found print() calls in fixed DAG; should use the JSON logger. "
        f"Offences:\n  " + "\n  ".join(real_print_calls)
    )


def test_dag_imports_get_logger():
    """The DAG file should import the JSON logger helper."""
    dag_source = (ROOT / "dags" / "sales_enrichment_dag.py").read_text(encoding="utf-8")
    assert "from plugins.logging_config import get_logger" in dag_source, (
        "The fixed DAG must import get_logger from plugins.logging_config "
        "to use structured JSON logging."
    )


# ---------- DAG sanity ----------

def test_task_count(fixed_dag):
    """DAG should have the three expected tasks."""
    task_ids = {t.task_id for t in fixed_dag.tasks}
    assert task_ids == {"extract_sales", "call_lambda", "write_results"}, (
        f"Expected 3 tasks; got {task_ids}"
    )


def test_task_ordering(fixed_dag):
    """extract -> call_lambda -> write_results."""
    extract = fixed_dag.get_task("extract_sales")
    enrich = fixed_dag.get_task("call_lambda")
    write = fixed_dag.get_task("write_results")
    assert enrich in extract.downstream_list
    assert write in enrich.downstream_list
