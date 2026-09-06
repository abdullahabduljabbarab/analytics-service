# Engineering Report

## What this is

An analytics service. It consumes the events every other ABS service commits, from
the ledger, the orchestrator and the risk engine, keeps a durable analytical copy
in BigQuery, and serves read-optimized projections derived from it. It owns
derived analytical truth, never financial truth: it never calls or writes back to
any other service and emits no events. The system is built around one property a
projection service must have, and the flagship proof of this repo: the analytical
read model is disposable and rebuildable exactly from the retained event history.

## Architecture

```
   TRANSACTIONAL WORLD                        ANALYTICAL WORLD

   Ledger ───────────┐
                     │
   Orchestrator ─────┼── Pub/Sub ──▶ analytics-service ──▶ BigQuery raw_events
                     │                     │ append (streaming)      │
   Risk ─────────────┘                     │                         │ pure projections
                                     scheduled refresh ──────────────┤
                                           │                         ▼
                                           ▼                  materialized tables
                                    Analytics Query API ◀─────────────┘
```

FastAPI and Pydantic handle validation, routing and the OpenAPI spec. The store
is BigQuery, reached with the runtime service account's own IAM, no Cloud SQL, no
connection string, no secret. Deployed on Cloud Run with a Cloud Run Job for the
scheduled refresh, CI/CD via GitHub Actions using keyless Workload Identity
Federation, and infrastructure in Terraform.

## Key engineering decisions

**A different storage model: BigQuery, not another OLTP database.** Analytics is
scan-and-aggregate over an append-only history, which is what a columnar
warehouse is for. Forcing it into a transactional PostgreSQL for symmetry would
be backwards; this draws a clean OLTP/OLAP line across the ecosystem.

**raw_events is a durable analytical copy, canonical only within analytics.** The
claim "rebuildable from events" is only defensible if the events are retained, and
Pub/Sub retention is finite. So the service keeps every consumed event and rebuilds
from that, never from the broker. The upstream services stay authoritative for the
facts they committed.

**Ingestion is separate from projection.** The consumer's only durable, acked
action is persisting the raw event; projections are disposable derived state,
materialized by a scheduled refresh off the ack path. A projection failure leaves
analytics stale but loses no event, and a rebuild repairs it exactly.

**Append-only ingestion, dedup on read.** Ingestion is a streaming insert, not
per-event DML, so it has no per-table concurrency limit (a MERGE-per-event design
was tried and broke live at ~20 concurrent DML). The one-per-event_id invariant is
enforced on read: the refresh reads `raw_events` deduplicated by `event_id`.

**Deterministic, rebuildable projections.** Every projection is a pure function of
the raw history, materialized as tables the API serves. A rebuild destroys them
and recomputes from `raw_events`, and a watermark (`raw_event_count`, `as_of`)
makes the rebuild rigorous: the same raw set produces the same projection.

**A store port with two adapters, one contract.** The in-memory store backs local
runs and the full CI suite; the BigQuery adapter backs production. Both satisfy one
behavioural contract, run in full against the in-memory store in CI and as a smoke
subset against a temporary dataset at deploy, so BigQuery is not needed on every
push.

**A strict sink that emits nothing.** No publish path, no broker client, no
outbox. Its isolation extends ABS-REQ-006: it can be stopped, broken or hours
behind while payments settle normally.

## Numbers

| Metric | Value |
|--------|-------|
| Test count | 44 (plus a BigQuery smoke run at deploy) |
| Store backends | 2 (in-memory, BigQuery), one shared contract |
| API endpoints | 8 (health, 6 analytics reads, 1 consumer) |
| Projections | 6 (overview, payments, risk, providers, accounts, timeseries) + watermark |
| Upstream streams consumed | 3 (transaction, payment, risk events) |
| Event wire formats handled | 2 (full envelope; attributes + payload) |
| ABS requirements owned | isolation (006), duplicate tolerance (008), correlation continuity (009) |

## Test categories

| Category | What they prove |
|----------|-----------------|
| Envelope | id lifting, aggregate_id fallback, UTC normalization |
| Projections | the known-scenario aggregates, order independence, event_id dedup, the watermark, and that the streaming builder is byte-for-byte identical to the reference build |
| Store contract | one behavioural suite (first event accepted, same id contributes once, independent ids, correlations survive, reset removes derived state only, rebuild reproduces, replay-twice identical) run against the in-memory store |
| Admin | refresh materializes, rebuild reproduces, status reports |
| API | the consumer (both wire formats), the read endpoints with a watermark, ingest-does-not-refresh, auth, and bad-envelope handling |
| BigQuery smoke | schema, streaming ingestion, dedup, and destroy-and-rebuild against a real temporary dataset (at deploy) |

## Findings under live load

Two cross-service issues surfaced only when analytics ran against the live
ecosystem, and both were corrected to the platform-appropriate pattern:

- **BigQuery DML concurrency.** A MERGE per pushed event failed at burst volume
  (`Too many DML statements outstanding, limit is 20`). Ingestion moved to
  append-only streaming inserts with dedup on read.
- **The ledger's wire format.** The ledger carries `event_id`/`event_type` in
  Pub/Sub attributes with the payload in the data, unlike the orchestrator and
  risk engine which send a full envelope. The consumer now normalizes both.

## Cloud architecture

| Component | Service | Region |
|-----------|---------|--------|
| API runtime | Cloud Run (dedicated runtime SA) | europe-west2 (London) |
| Analytical store | BigQuery dataset (IAM-scoped, no secret) | europe-west2 |
| Scheduled refresh | Cloud Run Job | europe-west2 |
| Container registry | Artifact Registry | europe-west2 |
| Event bus | Pub/Sub (three push subscriptions, one dead-letter; no publish path) | europe-west2 |
| CI/CD | GitHub Actions, keyless via Workload Identity Federation | Ubuntu runners |
| IaC | Terraform | All service resources declared |

## V&V matrix

The full requirement-to-test mapping is in [VV_PLAN.md](VV_PLAN.md). The read-path
SLOs and load test are in [SLO.md](SLO.md), the STRIDE threat model in
[THREAT_MODEL.md](THREAT_MODEL.md), and the decisions as ADRs in
[DECISIONS.md](DECISIONS.md).

## Design trade-offs

**Append-then-dedup-on-read over MERGE.** Streaming ingestion scales and matches
how BigQuery is meant to be used; the cost is that a just-ingested row is queryable
within a few seconds, not instantly (visible eventual consistency), and the
physical table can hold duplicate rows (a periodic compaction is optional
maintenance, not required for correctness).

**Python-computed projections over in-warehouse SQL.** Aggregating in the service
makes the rebuild exact and unit-testable and keeps the read path a cheap lookup;
at a scale where the history no longer fits a chunked pass, aggregation would move
into BigQuery SQL or a batch job, which the ADRs anticipate but do not pre-build.

**A dedicated runtime identity over the default compute SA.** Both the service and
the refresh Job run as a runtime service account scoped to exactly the BigQuery
dataset, rather than the broad default compute account.
