import enum
from dbt_cloud_api import DbtJobRunStatus,read_latest_run_status

class WatchdogStatus(enum.IntEnum):
    """
    The possible output status values (and thus also possible input stati on dependencies)
    """
    # positive stati for normal operation
    WAITING = 1 # some dependencies are not SUCCEEDED but could become so (i.e. WAITING, PENDING or RUNNING)
    PENDING = 2 # all dependencies are SUCCEEDED, this job could start, but isn't running, yet (last run's timestamp is before all dependencies's start time)
    RUNNING = 3 # all dependencies are SUCCEEDED and this job is running
    SUCCEEDED = 4 # all dependencies are SUCCEEDED and this job is also SUCCEEDED
    # negative stati for abnormal operation:
    # - type 1: problem caused by upstream dependencies
    BLOCKED = -1 # at least one dependency has a negative status
    OUTDATED = -2 # all dependencies were successful, there is at least one dependency with a time stamp after this run's start time (and the run's start time is after the earliest possible start time)
    # - type 2: problem with this job: all dependencies are SUCCEEDED, this job's start time is after the maximum timestamp,
    CANCELLED = -3 # all dependencies are SUCCEEDED, this job's start time is after the maximum timestamp, and was cancelled
    RETRYABLE =  -4 # this job has started and ran into an error which is retryable
    ERROR =  -5 # this job has started and ran into an error and is marked not retryable
    TIMEOUT = -6 # this status is assigned by rules, e.g. if a status is PENDING for longer than a specified duration


def determine_status(config, all_successful, all_positive, max_timestamp):
    """
    Determine the status of this step depending on dependencies and latest run status
    """
    if not all_successful:
        # some dependencies not successful (yet), the result can be irrespective of current job
        return status_from_dependencies(all_positive, max_timestamp)

    # if all dependencies are were executed successfully we need to know what the current job's state is
    job_status = read_latest_run_status(config.account_id, config.job_id, config.api_key)
    job_found = (job_status.result != DbtJobRunStatus.NONE) and (job_status.start_time is not None)
    # determine the earliest time (for current day) for which we expect the job to be started
    earliest_start_time = determine_earliest_start_time(config)
    # if last job start time is before the earliest possible start time, ignore it (it must be a run from previous day)
    if job_found and job_status.start_time < earliest_start_time:
        job_found = False
    if job_found:
        # there is a current job with start timestamp, use this:
        return status_from_active_job(job_status)
    #  current job does not exist, or has DBT status QUEUEDor STARTING, all three cases count as PENDING
    status = WatchdogStatus.PENDING
    # if there is no dependency, take the earliest possible start time as timestamp
    timestamp = max_timestamp if max_timestamp is not None else earliest_start_time
    return {"status": status, "timestamp": timestamp}
    return {"status": WatchdogStatus.PENDING, "timestamp": job_status.start_time}


def status_from_dependencies(all_positive, max_timestamp):
    # if all stati are positive we are still waiting for success,
    # if at least one dependency has a negative status this run is blocked from being executed
    return WatchdogStatus.WAITING if all_positive else WatchdogStatus.BLOCKED


def determine_earliest_start_time(config):
    TODO


def status_from_active_job(job_status, max_timestamp):
    if max_timestamp is not None and job_status.start_time < max_timestamp:
        # there is a job, but it was started before the maximum timestamp (of dependencies)
        return {"status": WatchdogStatus.OUTDATED, "timestamp": max_timestamp}
    if job_status.result == DbtJobRunStatus.SUCCESS:
        return {"status": WatchdogStatus.SUCCEEDED, "timestamp": job_status.end_time}
    if job_status.result == DbtJobRunStatus.RUNNING:
        return {"status": WatchdogStatus.RUNNING, "timestamp": job_status.start_time}
    if job_status.result == DbtJobRunStatus.CANCELLED:
        return {"status": WatchdogStatus.CANCELLED, "timestamp": job_status.end_time}
    if job_status.result == DbtJobRunStatus.ERROR:
        # for error case we make the distinction if DBT cloud marks the job as restartable or not
        status = WatchdogStatus.RETRYABLE if job_status.restartable else WatchdogStatus.ERROR
        return {"status": status, "timestamp": job_status.end_time}
    raise f"Job with start time {job_status.start_time} has unexpected status: {job_status.result}"


