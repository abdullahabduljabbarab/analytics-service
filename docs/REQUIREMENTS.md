# Analytics Service: Requirements

The analytics service is the ecosystem's analytical read model. It consumes
committed events from three topics and derives read-optimized aggregates from a
durable analytical copy of those events. It owns derived analytical truth, never
financial truth, and can be stopped, broken or behind without any effect on the
transactional platform.

## Functional requirements

### FR-1: Consume committed events from three streams
The service subscribes to `transaction-events`, `payment-events` and
`risk-events`, and reacts only to events those services have already durably
committed. It is strictly downstream and never participates in producing them.

### FR-2: Idempotent ingestion into a durable raw history
Every consumed event is persisted once, keyed on `event_id`, into `raw_events`,
the durable analytical history. At-least-once redelivery of the same event
produces no second row and no second analytical contribution. (ABS-REQ-008)

### FR-3: Ingestion separate from projection
The consumer's durable contract is validate then persist the raw event then ack.
Projections are derived state built from `raw_events`, never part of the ack
path, so a projection failure leaves analytics stale but loses no event.

### FR-4: Deterministic projections
Each projection (payments, risk, providers, accounts, timeseries, overview) is a
pure, deterministic function of the raw history: the same events always produce
the same aggregates, independent of arrival order. Duplicate defence is at the
input boundary, by `event_id` canonicalization, not by per-metric `payment_id`
counting. Monetary aggregates use `Decimal` (never float), ABS is single-currency
so amounts add directly, and daily buckets are by `occurred_at` in UTC.

### FR-5: Deterministic rebuild
The materialized projections can be destroyed and regenerated from `raw_events`,
producing identical aggregates. The raw history is untouched by a rebuild. This
is the service's defining property.

### FR-6: Read API over the projections
The service exposes read-only endpoints for the aggregates (overview, payments,
risk, providers, per-account activity, timeseries). There are no mutation
endpoints for analytical data.

### FR-7: Correlation continuity
Every raw event retains its originating `correlation_id`, so a row in the
analytical history traces back to the same request as the payment and risk
decision that produced it. (ABS-REQ-009)

## Non-functional requirements

### NFR-1: Isolation from financial state (extends ABS-REQ-006)
Analytics never reads or writes any financial system and emits no events. It can
be stopped, broken or hours behind while payments settle normally. Derived
analytical state never becomes authoritative financial state.

### NFR-2: Explicit eventual consistency
Analytics may lag the transactional platform. Its answers are analytical, not
authoritative current balances, and must not be presented as such; authoritative
balances belong to the ledger. Every projection carries a watermark
(`raw_event_count`, `as_of`) naming the raw-event boundary it was built from, so
the lag is visible and a rebuild is compared against the exact same input set.

### NFR-6: Projection refresh is off the ack path
Ingestion acks after persisting the raw event; projections are materialized by a
separate, scheduled refresh, never a full history scan per message. A refresh and
a from-scratch rebuild share the same projection code and cannot disagree.

### NFR-3: OLTP / OLAP separation
The analytical store is BigQuery, chosen for read-optimized analytical workloads,
not another transactional PostgreSQL database adopted for architectural symmetry.

### NFR-4: Testable without the cloud
The deterministic core and the store contract run in full against an in-memory
store in CI, with no BigQuery dependency. The BigQuery adapter is verified by a
smoke subset of the same contract against a temporary dataset during deployment.

### NFR-5: Authenticated ingress
The event-ingest endpoint is reachable only by the platform's own push
subscriptions, verified at the application layer with an OIDC token. The read and
health endpoints are public by scope decision. (Delivered in later milestones.)

## Out of scope

- Machine learning, forecasting, or anomaly detection.
- Dashboards inside this repo beyond what proves the service.
- Real-time authoritative balances (the ledger owns those).
- Any write path back into financial state. There is none, by design.
- Stream-processing infrastructure (Kafka, Spark, Dataflow) unless an actual
  scaling requirement appears.
