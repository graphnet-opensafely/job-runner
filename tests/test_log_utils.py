from datetime import datetime
import logging
import time

import pytest

from jobrunner.models import Job, JobRequest
from jobrunner import log_utils, local_run


FROZEN_TIMESTAMP = 1608568119.1467905
FROZEN_TIMESTRING = datetime.utcfromtimestamp(FROZEN_TIMESTAMP).isoformat()

repo_url='https://github.com/opensafely/project'
test_job = Job(id='id', action='action', repo_url=repo_url)
test_request = JobRequest(
    id='request',
    repo_url=repo_url,
    workspace="workspace",
    commit='commit',
    requested_actions=['action'],
    database_name='dummy',
)


def test_formatting_filter():
    record = logging.makeLogRecord({})
    assert log_utils.formatting_filter(record)
    assert record.action == ""

    record = logging.makeLogRecord({"job": test_job})
    assert log_utils.formatting_filter(record)
    assert record.action == "action: "
    assert record.tags == "project=project action=action id=id"

    record = logging.makeLogRecord({"job": test_job, "status_code": "code"})
    assert log_utils.formatting_filter(record)
    assert record.tags == "status=code project=project action=action id=id"

    record = logging.makeLogRecord({"job": test_job, "job_request": test_request})
    assert log_utils.formatting_filter(record)
    assert record.tags == "project=project action=action id=id req=request"


def test_jobrunner_formatter_default(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: FROZEN_TIMESTAMP)
    record = logging.makeLogRecord({
        "msg": "message",
        "job": test_job,
        "job_request": test_request,
        "status_code": "status",
    })
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(log_utils.DEFAULT_FORMAT, style='{')
    assert formatter.format(record) == (
        "2020-12-21 16:28:39.146Z message "
        "status=status project=project action=action id=id req=request"
    )


def test_jobrunner_formatter_local_run(monkeypatch):
    monkeypatch.setattr(time, 'time', lambda: FROZEN_TIMESTAMP)
    record = logging.makeLogRecord({
        "msg": "message",
        "job": test_job,
        "job_request": test_request,
        "status_code": "status",
    })
    log_utils.formatting_filter(record)
    formatter = log_utils.JobRunnerFormatter(local_run.LOCAL_RUN_FORMAT, style='{')
    assert formatter.format(record) == "action: message"