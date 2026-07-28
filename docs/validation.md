# Validation evidence

Last checked: **9 October 2026 (UTC)**, on the local Docker Desktop deployment.
These are executed local checks; a successful GitHub Actions run is not claimed.
The repository contains a CI workflow for the same pipeline tests.

## Current checks

| Check | Result | Scope |
|---|---|---|
| Local Python tests | 40 passed, 2 integration tests skipped | Validation, credit tiers and global/regional pacing, retries, OAuth and API envelope handling |
| Container Python tests | **42 passed** | Same units plus real Kafka/PostgreSQL replay, rollback, rejects, older-state protection and retention |
| Container dbt build | **22 successful nodes**, no failures or warnings | 5 models, 1 seed, 16 data tests; dbt core 1.9.4 / PostgreSQL adapter 1.9.0 |
| Airline reference | 20 dashboard groups, 28 unique prefixes | Generated dbt seed has 34 rows including regional, demo and unknown reference entries |
| Grafana SQL | **18 panel queries passed** | Both live-only dashboards queried through the Grafana datasource API |
| Grafana rendering | Visually checked | World map, airline counts, traffic analytics, ingestion health and credit budget |
| Secret exclusion | Passed | Supplied OpenSky credential values absent from publishable files; `.env` and `.secrets` ignored |

The container test run emitted two harmless pytest-cache permission warnings because
the application runs as a non-root user. No test was skipped in that run. The integration
fixtures use a dedicated temporary database and Kafka topic, with teardown after tests;
they do not insert synthetic rows into the live warehouse.

## Commands used

```bash
python -m pytest -q tests
docker compose build producer dbt-runner
docker compose --profile test run --rm --no-deps tests
docker compose stop dbt-runner
docker compose run --rm --no-deps dbt-runner dbt build
docker compose up -d --no-deps dbt-runner
```

The scheduled dbt loop is stopped for manual builds to prevent overlapping writes,
and resumed afterward. Container builds used the dependency versions pinned in the
Dockerfiles and `requirements.txt`.

## Live ingestion evidence

The first worldwide poll published **9,429 state vectors**, after the collector was
switched from a regional request to global coverage. The consumer reached `lag=0`.
One subsequent check found 8,563 valid recent positions spanning latitudes -50.15 to
69.42 and longitudes -169.92 to 179.69. These are point-in-time observations, not a
throughput benchmark or guaranteed coverage. Some API states lack a usable position
and are quarantined by this map-oriented pipeline.

The [dashboard gallery](dashboards.md) contains five real screenshots of the latest
interface. The visible volume change in historical charts reflects the scope change
from regional to worldwide ingestion. The traffic view filters the fixed 20 airlines;
pipeline health measures all live ingestion.

## Earlier correctness checks

On 8 October, the isolated dbt model smoke test exercised both initial and incremental
builds. Staging grew from 32 to 33 rows after insertion of a newly visible observation
with an older ingestion timestamp, confirming that the anti-join does not rely on
monotonic transaction timestamps. That local test used dbt core 1.11.8 / adapter 1.10.0;
the current container validation above uses the project's pinned 1.9 versions.

## Boundaries of the evidence

- No continuous 24-hour benchmark or availability claim has been established.
- Screenshots contain real observations, but do not prove complete worldwide coverage.
- No notification delivery test or prolonged failure/recovery exercise is claimed.
- A single-node Kafka/PostgreSQL stack is not a highly available production deployment.
- CI execution on GitHub remains to be verified after the repository is pushed.
