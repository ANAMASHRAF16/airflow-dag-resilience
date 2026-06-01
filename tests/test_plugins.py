"""Tests for plugins/callbacks.py and plugins/logging_config.py.

These don't need Airflow's DagBag - they test the plugin code directly.
"""

import json
import logging
import os
import sys
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from plugins.callbacks import slack_on_failure
from plugins.logging_config import JsonFormatter, get_logger


# ---------- JSON logging ----------

def test_json_formatter_emits_valid_json():
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="hello", args=(), exc_info=None,
    )
    output = JsonFormatter().format(record)
    parsed = json.loads(output)
    assert parsed["message"] == "hello"
    assert parsed["level"] == "INFO"
    assert "timestamp" in parsed


def test_json_formatter_includes_extra_fields():
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname="", lineno=0,
        msg="event", args=(), exc_info=None,
    )
    record.batch_size = 50
    record.lambda_arn = "arn:aws:lambda:..."
    parsed = json.loads(JsonFormatter().format(record))
    assert parsed["batch_size"] == 50
    assert parsed["lambda_arn"] == "arn:aws:lambda:..."


def test_get_logger_does_not_duplicate_handlers():
    """Calling get_logger twice should not add a second handler."""
    log1 = get_logger("test_no_duplicate")
    log2 = get_logger("test_no_duplicate")
    assert log1 is log2
    # The logger should have exactly one JsonFormatter handler
    json_handlers = [h for h in log1.handlers if isinstance(h.formatter, JsonFormatter)]
    assert len(json_handlers) == 1


# ---------- Slack failure callback ----------

def _fake_context(exception=Exception("Lambda timed out after 300s")):
    ti = MagicMock()
    ti.dag_id = "sales_enrichment_fixed"
    ti.task_id = "call_lambda"
    ti.try_number = 4
    ti.execution_date = "2026-05-30T03:00:00Z"
    ti.log_url = "https://airflow.internal/log/..."
    return {"task_instance": ti, "exception": exception}


def test_slack_callback_does_not_crash_when_webhook_unset(monkeypatch):
    """A missing SLACK_ALERT_WEBHOOK_URL must NOT crash the task on top of the original failure."""
    monkeypatch.delenv("SLACK_ALERT_WEBHOOK_URL", raising=False)
    # Should return silently, not raise
    slack_on_failure(_fake_context())


def test_slack_callback_posts_when_webhook_set(monkeypatch):
    """With webhook configured, the callback should POST to it."""
    monkeypatch.setenv("SLACK_ALERT_WEBHOOK_URL", "https://hooks.slack.com/services/FAKE")

    with patch("requests.post") as fake_post:
        fake_post.return_value.status_code = 200
        fake_post.return_value.raise_for_status = MagicMock()
        slack_on_failure(_fake_context())
        fake_post.assert_called_once()
        url, kwargs = fake_post.call_args[0][0], fake_post.call_args[1]
        assert "FAKE" in url
        body = kwargs["json"]
        # Block Kit shape: has blocks list
        assert "blocks" in body
        # Summary block mentions the failing task
        summary_text = json.dumps(body)
        assert "sales_enrichment_fixed" in summary_text
        assert "call_lambda" in summary_text
        assert "try 4" in summary_text


def test_slack_callback_logs_structured_event_even_on_webhook_failure(monkeypatch):
    """If Slack is down we still need a queryable audit trail in CloudWatch."""
    monkeypatch.setenv("SLACK_ALERT_WEBHOOK_URL", "https://hooks.slack.com/services/FAKE")
    with patch("requests.post", side_effect=Exception("connection refused")):
        # Should swallow the exception - the original failure has priority
        slack_on_failure(_fake_context())
