# Analytics Service: MVP

## Thesis

Prove that a downstream service can consume the ecosystem's whole event history,
serve useful analytical aggregates, and reproduce that entire read model exactly
by replaying the retained events, all while being so isolated that it can be down
or broken without touching a single payment. The interesting engineering is event
sourcing and CQRS: idempotent ingestion, deterministic projection, and a
provable rebuild, with a clean split between transactional and analytical
workloads.

## In scope

1. **Idempotent ingestion** of committed events from three topics into a durable
   `raw_events` history in BigQuery, deduplicated on `event_id`.
2. **Deterministic projections** derived purely from that history: payments,
   risk, providers, accounts, timeseries, overview.
3. **A provable rebuild**: destroy the materialized projections and regenerate
   them from `raw_events`, identical result.
4. **A store contract** satisfied by both an in-memory store (CI) and a BigQuery
   adapter (production).
5. **A read-only API** for the aggregates, plus an authenticated Pub/Sub consumer.
6. **Keyless deployment** to Cloud Run with BigQuery, no Cloud SQL, no secrets
   unless genuinely required.

## Out of scope

- Machine learning, dashboards beyond proof, real-time balances.
- Any write path to financial state; the service emits nothing.
- Kafka / Spark / Dataflow unless a real scaling need appears.

## Milestones

- **M1: Deterministic analytics core.** The ABS envelope, the pure projection
  functions, the `AnalyticsStore` port, the in-memory adapter, and the shared
  store contract, with the design-time docs. No cloud. (This milestone.)
- **M2: BigQuery.** `raw_events` and projection schemas, the BigQuery adapter
  with `MERGE`-based idempotent ingestion, schema-as-code, and chunked rebuild
  that does not assume all history fits in memory. A smoke run of the contract
  against a temporary dataset.
- **M3: Service.** The authenticated `/events/pubsub` consumer, the read
  endpoints, and an internal rebuild command (not a public mutation).
- **M4: Deployment.** Dockerfile, CI against the in-memory store, and Terraform:
  Cloud Run, a BigQuery dataset, push subscriptions on all three topics with a
  dead-letter, and Workload Identity Federation. No Cloud SQL, no Alembic, no
  outbox.
- **M5: Evidence.** All three streams feeding it, duplicate-without-double-count,
  aggregate query results, the rebuild proof, analytics-down-while-a-payment-
  settles, and correlation ids in the raw history.

## Definition of done

- Ingestion is idempotent on `event_id`, proven by the store contract and live.
- Projections are deterministic and rebuildable: destroy and regenerate from
  `raw_events` yields identical aggregates, proven in tests and shown live.
- The service can be down or failing without changing a payment's outcome.
- CI runs the full suite against the in-memory store with no cloud; the BigQuery
  adapter is smoke-tested against a temporary dataset; deployment uses no
  long-lived key.
