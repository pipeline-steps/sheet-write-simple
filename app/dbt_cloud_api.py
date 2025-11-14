import enum
import requests
import time
import os
from datetime import datetime


CONFIG_ARG = "config_path"
DEFAULT_CONFIG = "/etc/config/config.json"


# These are documented on the dbt Cloud API docs
class DbtJobRunStatus(enum.IntEnum):
    NONE = 0
    QUEUED = 1
    STARTING = 2
    RUNNING = 3
    SUCCESS = 10
    ERROR = 20
    CANCELLED = 30


class LatestRunStatus:
    """
    Represents the latest run status for a job.
    """
    def __init__(self, terminated: bool, result: str, start_time: datetime, end_time: datetime, restartable: bool):
        self.terminated = terminated
        self.result = result
        self.start_time = start_time
        self.end_time = end_time
        self.restartable = restartable

    def __repr__(self):
        return (
            f"LatestRunStatus(terminated={self.terminated}, result='{self.result}', "
            f"start_time={self.start_time}, end_time={self.end_time}, restartable={self.restartable})"
        )


def _trigger_job(account_id, job_id, api_key, job_body) -> int:
    res = requests.post(
        url=f"https://emea.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/",
        headers={'Authorization': f"Token {api_key}"},
        json=job_body
    )

    try:
        res.raise_for_status()
    except:
        print(f"API token (last four): ...{api_key[-4:]}")
        raise

    response_payload = res.json()
    return response_payload['data']['id']


def _get_job_run_status(account_id, api_key, job_run_id):
    res = requests.get(
        url=f"https://emea.dbt.com/api/v2/accounts/{account_id}/runs/{job_run_id}/",
        headers={'Authorization': f"Token {api_key}"},
    )

    res.raise_for_status()
    response_payload = res.json()
    return response_payload['data']


def read_latest_run_status(account_id, job_id, api_key) -> LatestRunStatus:
    """
    Get the latest run status for a given job ID.
    """
    res = requests.get(
        url=f"https://emea.dbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/runs/",
        headers={'Authorization': f"Token {api_key}"},
    )

    res.raise_for_status()
    response_payload = res.json()
    latest_run = response_payload['data'][0]  # Assuming data is ordered by most recent run first

    terminated = latest_run['status'] in [DbtJobRunStatus.SUCCESS, DbtJobRunStatus.ERROR, DbtJobRunStatus.CANCELLED]
    result = DbtJobRunStatus(latest_run['status']).name.lower()
    start_time = datetime.fromisoformat(latest_run['started_at']) if latest_run.get('started_at') else None
    end_time = datetime.fromisoformat(latest_run['finished_at']) if latest_run.get('finished_at') else None
    restartable = latest_run['status'] == DbtJobRunStatus.ERROR

    return LatestRunStatus(
        terminated=terminated,
        result=result,
        start_time=start_time,
        end_time=end_time,
        restartable=restartable
    )


def trigger_new_run(account_id, job_id, api_key, command):
    """
    Trigger a new run for a given job ID.
    """
    info = f'run {os.environ.get("RUN")} of pipeline {os.environ.get("PIPELINE_NAME")}:{os.environ.get("PIPELINE_VERSION")} in namespace {os.environ.get("NAMESPACE")}'
    job_body = {
        'cause': "Triggered by " + info,
        "steps_override": [command]
    }
    job_run_id = _trigger_job(account_id, job_id, api_key, job_body)
    print(f"Triggered new run with job_run_id = {job_run_id}")
    return job_run_id


def retry_failed_run(account_id, job_id, api_key, command):
    """
    Retry the latest failed run for a given job ID.
    """
    latest_run_status = read_latest_run_status(account_id, job_id, api_key)
    if latest_run_status.restartable:
        print("Retrying failed run...")
        return trigger_new_run(account_id, job_id, api_key, command)
    else:
        raise Exception(f"The latest run is not restartable. Status: {latest_run_status.result}")


# Example Usage
if __name__ == "__main__":
    account_id = "12345"
    job_id = "67890"
    api_key = "your_api_key_here"
    command = "dbt run --models my_model"

    # Read the latest run status
    latest_status = read_latest_run_status(account_id, job_id, api_key)
    print(latest_status)

    # Trigger a new run
    new_run_id = trigger_new_run(account_id, job_id, api_key, command)

    # Retry a failed run
    try:
        retry_run_id = retry_failed_run(account_id, job_id, api_key, command)
    except Exception as e:
        print(e)
