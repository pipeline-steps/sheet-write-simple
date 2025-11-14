from google.cloud import monitoring_v3
from datetime import datetime, timedelta

def list_quota_usage(project_id: str):
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project_id}"

    # Time window: last 24h
    end_time = datetime.utcnow()
    start_time = end_time - timedelta(hours=24)

    interval = monitoring_v3.TimeInterval({
        "end_time": {"seconds": int(end_time.timestamp())},
        "start_time": {"seconds": int(start_time.timestamp())},
    })

    # Quota-related metrics live under the "serviceruntime.googleapis.com/quota/" namespace
    results = client.list_time_series(
        request={
            "name": project_name,
            "filter": 'metric.type = starts_with("serviceruntime.googleapis.com/quota/")',
            "interval": interval,
            "view": monitoring_v3.ListTimeSeriesRequest.TimeSeriesView.FULL,
        }
    )

    for ts in results:
        metric_type = ts.metric.type
        resource = ts.resource.labels
        values = [point.value.int64_value for point in ts.points if point.value.int64_value]

        if not values:
            continue

        current = values[0]
        maximum = max(values)

        print(f"\n🔹 Metric: {metric_type}")
        print(f"   Resource: {resource}")
        print(f"   Current: {current}")
        print(f"   Max (last 24h): {maximum}")

if __name__ == "__main__":
    project_id = "breuninger-dataprocessing-prod"
    list_quota_usage(project_id)
