"""Unit tests for quality/alerts.py — testing pure logic, no real Airflow run needed."""
from datetime import datetime
from unittest.mock import patch

import pytest

from quality.alerts import _format_failure_message, notify_failure


class FakeTaskInstance:
    """A minimal stand-in for Airflow's real TaskInstance object."""
    def __init__(self, dag_id, task_id, try_number=1, log_url="http://fake-log-url"):
        self.dag_id = dag_id
        self.task_id = task_id
        self.try_number = try_number
        self.log_url = log_url


def test_format_failure_message_includes_alert_prefix():
    """The message should start with '[ALERT] {dag_id}.{task_id} failed at' as required by TICKET-101."""
    context = {
        "task_instance": FakeTaskInstance("test_dag", "test_task"),
        "run_id": "manual__test_run",
        "exception": Exception("boom"),
    }

    message = _format_failure_message(context)

    assert message.startswith("[ALERT] test_dag.test_task failed at")


def test_format_failure_message_includes_exception_details():
    """The exception text should appear somewhere in the message for debugging."""
    context = {
        "task_instance": FakeTaskInstance("test_dag", "test_task"),
        "run_id": "manual__test_run",
        "exception": Exception("something specific went wrong"),
    }

    message = _format_failure_message(context)

    assert "something specific went wrong" in message


def test_format_failure_message_handles_missing_fields_gracefully():
    """Even with an incomplete context dict, this should not crash."""
    context = {}  # deliberately empty

    message = _format_failure_message(context)

    assert "unknown_dag" in message
    assert "unknown_task" in message


def test_notify_failure_does_not_raise_when_context_is_broken():
    """notify_failure must never itself crash and re-fail the task — this is the whole point of TICKET-101."""
    broken_context = {"task_instance": "not even a real object"}

    # This should not raise, no matter what garbage context is passed in.
    notify_failure(broken_context)


def test_notify_failure_skips_slack_when_webhook_not_configured(monkeypatch):
    """When SLACK_WEBHOOK_URL is unset, notify_failure should log and return without error."""
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)

    context = {
        "task_instance": FakeTaskInstance("test_dag", "test_task"),
        "run_id": "manual__test_run",
        "exception": Exception("boom"),
    }

    # Should complete without raising, even with no webhook configured.
    notify_failure(context)