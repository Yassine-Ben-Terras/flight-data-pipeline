import os
import signal
import subprocess
import threading

stop = threading.Event()
for sig in (signal.SIGINT, signal.SIGTERM):
    signal.signal(sig, lambda *_: stop.set())
while not stop.is_set():
    # build applies models and their dependent tests in DAG order.
    result = subprocess.run(["dbt", "build"], check=False)
    freshness = subprocess.run(["dbt", "source", "freshness"], check=False)
    print(f"dbt_build_exit={result.returncode} freshness_exit={freshness.returncode}", flush=True)
    stop.wait(int(os.getenv("DBT_INTERVAL_SECONDS", "300")))
