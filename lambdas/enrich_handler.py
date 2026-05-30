"""Stub Lambda handler that the Airflow DAG invokes.

In production this enriches sales records (e.g., adds tax info, currency
conversion). For the activity demo it just sleeps to simulate the timeout
condition the broken DAG fails silently on.

Triggers the broken DAG's silent-failure mode when SIMULATE_TIMEOUT is
truthy in the event payload - the function sleeps past the Lambda's
configured timeout.
"""

import json
import os
import time


def handler(event, context):
    if event.get("SIMULATE_TIMEOUT") or os.environ.get("SIMULATE_TIMEOUT"):
        # Sleep longer than the Lambda's configured timeout to trigger the
        # exact failure condition the broken DAG mishandles silently.
        time.sleep(900)

    records = event.get("records", [])
    enriched = [{**r, "enriched_at": context.aws_request_id if context else "test"} for r in records]
    return {
        "statusCode": 200,
        "body": json.dumps({"count": len(enriched), "records": enriched}),
    }
