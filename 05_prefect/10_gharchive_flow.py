import gzip
import json
from collections import Counter
import requests
from prefect import flow, task


URL = "https://data.gharchive.org/2025-01-01-{hour}.json.gz"


@task(retries=3, retry_delay_seconds=10)
def count_events_for_hour(hour):
    # stream one hourly file and return {event_type: count}
    counts = Counter()
    bad_lines = 0
    with requests.get(URL.format(hour=hour), stream=True, timeout=60) as r:
        r.raise_for_status()  # HTTP errors propagate so Prefect retries the task
        # decompress the gzip stream on the fly, one JSON event per line
        with gzip.GzipFile(fileobj=r.raw) as gz:
            for line in gz:
                try:
                    counts[json.loads(line)["type"]] += 1
                except (json.JSONDecodeError, KeyError):
                    bad_lines += 1  # skip bad records without failing the whole hour
    print(f"hour {hour}: {counts.total()} events ({bad_lines} skipped)")
    return dict(counts)


@task
def combine_counts(hourly_counts):
    # Total counts by type and order them
    # print those to screen and return them
    pass


@flow(log_prints=True)
def gharchive_flow(hours: list[int] = list(range(1, 4))):
    # take the range and create a prefect .map over the hours
    hourly = count_events_for_hour.map(hours).result()
    totals = combine_counts(hourly)
    for k, v in totals.items():
        print(k, v)


if __name__ == "__main__":
    gharchive_flow()
