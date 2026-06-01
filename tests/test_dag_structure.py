"""DAG source-level structural tests.

Verify the fixed DAG has the four resilience features Activity 5 requires:

1. retries >= 3 with retry_exponential_backoff=True
2. on_failure_callback set to plugins.callbacks.slack_on_failure
3. execution_timeout configured
4. Uses the structured JSON logger (not print)

DESIGN NOTE: An earlier version of these tests used Airflow's DagBag to
parse the DAG and inspect the live DAG object. That broke on Airflow
3.x where the package was restructured (no top-level `airflow/__init__.py`
and `airflow.models.dagbag` moved). Rather than pin to a specific Airflow
version (which fights with the providers packages), we now parse the
DAG file's source AST directly. Trade-off: slightly less rigorous than
runtime inspection (we check syntax, not evaluated values), but completely
Airflow-version-independent and runs on plain Python with no Airflow
install at all.
"""

import ast
import re
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parent.parent
DAG_FILE = ROOT / "dags" / "sales_enrichment_dag.py"
sys.path.insert(0, str(ROOT))


@pytest.fixture(scope="module")
def dag_source() -> str:
    return DAG_FILE.read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def default_args_dict(dag_source) -> dict:
    """Parse the source and return the literal-value parts of default_args.

    Walks the AST, finds `default_args = { ... }`, and returns a dict of
    {key_name: ast_node_for_the_value}. Callers can then check whether
    a key is present and inspect the value node.
    """
    tree = ast.parse(dag_source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "default_args":
                    if not isinstance(node.value, ast.Dict):
                        pytest.fail("default_args must be a dict literal")
                    out = {}
                    for key, value in zip(node.value.keys, node.value.values):
                        if isinstance(key, ast.Constant) and isinstance(key.value, str):
                            out[key.value] = value
                    return out
    pytest.fail("default_args dict not found in DAG source")


# ---------- 1. Retries with exponential backoff ----------

def test_retries_at_least_three(default_args_dict, dag_source):
    node = default_args_dict.get("retries")
    assert node is not None, "default_args must include 'retries'"
    # retries can be a literal (retries=3) or an env-driven variable
    if isinstance(node, ast.Constant) and isinstance(node.value, int):
        assert node.value >= 3, f"retries literal is {node.value}, must be >= 3"
    else:
        src = ast.unparse(node)
        assert "DEFAULT_RETRIES" in src or "retries" in src.lower(), (
            f"retries value '{src}' should reference a retries variable"
        )
        retries_default = re.search(r'DAG_DEFAULT_RETRIES["\'],\s*["\'](\d+)["\']', dag_source)
        if retries_default:
            assert int(retries_default.group(1)) >= 3, (
                f"DAG_DEFAULT_RETRIES default is {retries_default.group(1)}, must be >= 3"
            )


def test_retry_delay_is_configured(default_args_dict):
    node = default_args_dict.get("retry_delay")
    assert node is not None, "default_args must include 'retry_delay'"
    src = ast.unparse(node)
    assert "timedelta" in src, f"retry_delay should be a timedelta, got: {src}"


def test_retry_exponential_backoff_enabled(default_args_dict):
    node = default_args_dict.get("retry_exponential_backoff")
    assert node is not None, "default_args must include 'retry_exponential_backoff'"
    assert isinstance(node, ast.Constant) and node.value is True, (
        f"retry_exponential_backoff must be True; got: {ast.unparse(node)}"
    )


def test_max_retry_delay_bounded(default_args_dict):
    node = default_args_dict.get("max_retry_delay")
    assert node is not None, (
        "default_args must include 'max_retry_delay' so exponential backoff "
        "doesn't push the next retry days out"
    )
    src = ast.unparse(node)
    assert "timedelta" in src, f"max_retry_delay should be a timedelta, got: {src}"


# ---------- 2. on_failure_callback for Slack ----------

def test_on_failure_callback_is_set(default_args_dict):
    node = default_args_dict.get("on_failure_callback")
    assert node is not None, (
        "default_args must include 'on_failure_callback' so task failures page the team"
    )
    src = ast.unparse(node)
    assert "slack_on_failure" in src, (
        f"on_failure_callback must reference slack_on_failure; got: {src}"
    )


def test_slack_callback_imported_from_plugins(dag_source):
    assert re.search(r"from\s+plugins\.callbacks\s+import\s+slack_on_failure", dag_source), (
        "The fixed DAG must import slack_on_failure from plugins.callbacks"
    )


# ---------- 3. execution_timeout ----------

def test_execution_timeout_configured(default_args_dict):
    node = default_args_dict.get("execution_timeout")
    assert node is not None, (
        "default_args must include 'execution_timeout' - a hung Lambda call without "
        "this pins an Airflow worker slot indefinitely"
    )
    src = ast.unparse(node)
    assert "timedelta" in src, f"execution_timeout should be a timedelta, got: {src}"


def test_execution_timeout_reasonable(dag_source):
    """Default execution_timeout should be <= 30 minutes."""
    match = re.search(r'DAG_EXECUTION_TIMEOUT_MIN["\'],\s*["\'](\d+)["\']', dag_source)
    if match:
        default_min = int(match.group(1))
        assert default_min <= 30, (
            f"DAG_EXECUTION_TIMEOUT_MIN default is {default_min} minutes; "
            f"anything > 30 min is too lenient - tighten it and let retries handle the rest"
        )


# ---------- 4. Structured JSON logging (no print() in dags/) ----------

def test_dag_file_uses_structured_logger_not_print(dag_source):
    """Source should not contain active print() calls outside docstrings."""
    cleaned = re.sub(r'""".*?"""', "", dag_source, flags=re.DOTALL)
    cleaned = re.sub(r"'''.*?'''", "", cleaned, flags=re.DOTALL)
    cleaned_lines = [re.sub(r"\s*#.*$", "", line) for line in cleaned.split("\n")]
    cleaned = "\n".join(cleaned_lines)

    offenders = [line.strip() for line in cleaned.split("\n") if "print(" in line]
    assert not offenders, (
        f"Found print() calls in fixed DAG; should use the JSON logger:\n  "
        + "\n  ".join(offenders)
    )


def test_dag_imports_get_logger(dag_source):
    assert re.search(r"from\s+plugins\.logging_config\s+import\s+get_logger", dag_source), (
        "The fixed DAG must import get_logger from plugins.logging_config"
    )


def test_dag_uses_logger_in_tasks(dag_source):
    """At least one task function should call the logger."""
    assert re.search(r"log\.(info|error|warning|debug)\(", dag_source), (
        "DAG tasks should use the structured logger (log.info / log.error / etc.) "
        "instead of print()"
    )


# ---------- DAG sanity ----------

def test_three_python_operators_defined(dag_source):
    matches = re.findall(r"PythonOperator\s*\(", dag_source)
    assert len(matches) == 3, f"Expected 3 PythonOperator declarations; found {len(matches)}"


def test_task_ordering_is_extract_then_enrich_then_write(dag_source):
    assert re.search(r"extract\s*>>\s*enrich\s*>>\s*write", dag_source), (
        "Task ordering should be extract >> enrich >> write"
    )
