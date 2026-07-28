# Portfolio case study

## Problem

Aircraft state data changes continuously. A useful worldwide observatory needs to
handle unreliable networks, duplicates, invalid observations, and restricted API
budgets without hiding gaps or overstating its analytics.

## Solution

I built a containerized pipeline that authenticates to OpenSky with OAuth2, buffers
worldwide observations in Kafka, validates and loads them transactionally into
PostgreSQL, models them with dbt, and visualizes the data and pipeline health in
Grafana. The traffic view focuses on a fixed set of 20 major airlines. Synthetic fixtures
make the ingestion tests reproducible without API credentials; dashboards stay live-only.

## Decisions to explain in an interview

- **Why Kafka?** It separates source polling from sink availability and provides a
  bounded replay window. It cannot recover snapshots lost before publication.
- **Why Python rather than Spark?** The volume is bounded by API credits, and the
  ingestion work is validation and row-level writes, not distributed computation.
- **Why this primary key?** An aircraft-position timestamp identifies an observation
  and is compatible with daily PostgreSQL partitioning.
- **Why commit ordering?** Committing the offset first risks data loss. Committing the
  database first permits replay, which the idempotent sink absorbs.
- **Why not a simple timestamp watermark?** Commit order can differ from timestamp
  order. An anti-join catches newly visible keys without assuming monotonic commits.
- **What is a flight?** This source gives observations. The project does not invent
  flight legs, arrivals, or departures from incomplete position snapshots.
- **What would change at scale?** Replicated infrastructure, batching/COPY or staging
  merges, a warehouse sized to measured query load, and an orchestrator if dependencies grow.

## Five-minute demonstration

1. Show the architecture and worldwide credit budget (4 credits per call, 96-second interval) (45 seconds).
2. Open Flight operations and explain the fixed 20-airline selection (60 seconds).
3. Open Pipeline health and explain freshness, ingestion latency, throughput and credits (45 seconds).
4. Run the integration test and explain forced rollback, replay, and older-state protection (90 seconds).
5. Show dbt models and discuss the single-node and measurement limitations (60 seconds).

## Suggested resume wording

> Built an observable worldwide aircraft-data pipeline using Python, Kafka,
> PostgreSQL, dbt, and Grafana; implemented transactional idempotent ingestion,
> data-quality quarantine, incremental modeling, OAuth2 authentication, and
> automated replay/rollback integration tests.

Add measured volume or latency only after collecting reproducible evidence.
Use the [current dashboard gallery](dashboards.md) and [validation evidence](validation.md).
Explain why observed aircraft counts do not represent total fleet sizes or completed flights.
