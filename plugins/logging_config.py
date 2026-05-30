"""Structured JSON logging for Airflow tasks.

Every log line becomes one JSON object so CloudWatch Logs Insights and
Datadog can parse it as structured fields. Replaces the print() and
plain-text logging that hides operational signal in prose.

Usage inside a DAG task:

    from plugins.logging_config import get_logger
    log = get_logger(__name__)
    log.info("lambda_invoked", extra={"batch_size": 50, "lambda_arn": "..."})

Output (one line per event):

    {"timestamp": "2026-05-30T09:32:11Z", "level": "INFO",
     "logger": "tasks.call_lambda", "message": "lambda_invoked",
     "batch_size": 50, "lambda_arn": "..."}
"""

import json
import logging
from datetime import datetime, timezone


class JsonFormatter(logging.Formatter):
    # Keys that LogRecord always has but we don't want to flatten into the
    # JSON output unless the caller explicitly passed them via `extra`.
    _RESERVED = {
        "args", "asctime", "created", "exc_info", "exc_text", "filename",
        "funcName", "levelname", "levelno", "lineno", "module", "msecs",
        "message", "msg", "name", "pathname", "process", "processName",
        "relativeCreated", "stack_info", "thread", "threadName",
        "taskName",
    }

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        # Pull anything the caller passed via `extra=` into the top level
        for key, value in record.__dict__.items():
            if key in self._RESERVED or key.startswith("_"):
                continue
            payload[key] = value
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured with the JSON formatter.

    Safe to call repeatedly - we don't add a second handler on re-import,
    which would otherwise duplicate every log line.
    """
    logger = logging.getLogger(name)
    if any(isinstance(h.formatter, JsonFormatter) for h in logger.handlers):
        return logger

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
