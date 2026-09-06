# Service Level Objectives

Analytics is not on a synchronous money path: nothing waits on it, and its work
happens off the request. Its request-facing surface is the read API, and each read
serves a materialized projection from BigQuery. BigQuery is a columnar analytical
warehouse, so a point read of a small projection row carries interactive-query
latency (a few hundred milliseconds), not the single-digit-millisecond index
lookup the OLTP services get from PostgreSQL. That is the deliberate OLTP/OLAP
trade-off of this service, and the SLOs below are set to it. `GET /health` touches
no store and is the low-latency baseline.

## Defined SLOs

| Metric | Target | Measurement |
|--------|--------|-------------|
| Availability | 99.5% | Percentage of non-5xx responses during load test |
| Read p50 | < 1000ms | Median for the `/analytics/*` endpoints |
| Read p95 | < 2000ms | 95th percentile for the `/analytics/*` endpoints |
| Health p50 | < 100ms | Median for `GET /health` |
| Error rate | < 1% | Percentage of 5xx responses |
| Throughput | > 5 req/s sustained | Aggregate under 5 concurrent users |
| Rebuild integrity | deterministic | Destroy and regenerate from `raw_events` yields identical aggregates at the same watermark |

## Load Test Configuration

- Tool: Locust ([`scripts/loadtest.py`](../scripts/loadtest.py))
- Target: `https://analytics-service-eppidgbmxa-nw.a.run.app`
- Users: 5 concurrent
- Duration: 60 seconds
- Workload mix: overview (weight 5), payments and risk (2 each), providers, per-account and health (1 each)
- The ingest endpoint is not in the mix: it is authenticated (a Pub/Sub OIDC token), so it is exercised live by the real subscriptions.

## Load Test Results

Run on 2026-09-06 against the live Cloud Run deployment, over a materialized read
model of 152 events. 315 requests over 60 seconds at 5 concurrent users.

| Metric | Target | Measured | Status |
|--------|--------|----------|--------|
| Availability | 99.5% | 100% (0 of 315 failed) | pass |
| Read p50 | < 1000ms | 620ms | pass |
| Read p95 | < 2000ms | 800ms | pass |
| Health p50 | < 100ms | 33ms | pass |
| Error rate (5xx) | < 1% | 0% | pass |
| Throughput | > 5 req/s | 5.29 req/s | pass |

Per-endpoint medians: health 33ms; overview 630ms, payments 620ms, risk 630ms,
providers 660ms, per-account 630ms. The occasional p99 spike (~2.5s) is BigQuery
query queueing on a cold path. A read makes two BigQuery queries (the watermark and
the projection), which is where the few-hundred-millisecond floor comes from; the
warehouse latency dominates, not the service.

**Improvement path.** The projections are tiny and change only on refresh, so a
low-latency serving layer would cache the materialized rows (in memory or a KV
store) and read BigQuery only to rebuild them. That would cut read latency to the
OLTP range while keeping BigQuery as the durable, rebuildable store. It is noted
here as a deliberate future optimization rather than pre-built, since the read path
is not on any money path and the current latency is acceptable for analytics.

## The event-processing path

The consumer is measured by its guarantees, not request latency, verified live:

- All three streams (transaction, payment, risk) feed `raw_events`, the ledger's
  attribute-style events consumed alongside the others' envelope style.
- Ingestion is append-only streaming, which absorbed a burst that the earlier
  per-event MERGE design could not (BigQuery's ~20 concurrent-DML limit).
- The read model is deterministically rebuildable: destroying and regenerating the
  projections from `raw_events` reproduced every aggregate byte-for-byte at the
  same watermark.

## Post-Load Verification

After the load test, `GET /health` returns 200 with the BigQuery backend, and the
read model remains served from the materialized projections. Because every
aggregate is a deterministic function of the retained raw history, a refresh or
rebuild reproduces it exactly, which is the service's integrity property rather
than a balance to reconcile.
