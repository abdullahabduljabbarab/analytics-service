# Architecture Decision Records

Decisions specific to the analytics service. Ecosystem-wide decisions (the
authoritative ledger, the ABS event envelope, Workload Identity Federation for
CI) are recorded in the services that own them and reused here.

## ADR-001: BigQuery as the analytical store, not Cloud SQL

**Status:** Accepted

**Context:** The other services keep their state in PostgreSQL on the shared
Cloud SQL instance. It would be symmetrical to give analytics one too.

**Decision:** Analytics uses BigQuery. Its workload is analytical (scan-and-
aggregate over an append-only event history), which is what a columnar warehouse
is for, and the opposite of the row-oriented transactional access the other
services need.

**Consequences:** The one subsystem whose purpose is read-optimized analytics is
not forced into a transactional database for architectural symmetry. It also
draws a clean OLTP/OLAP line across the ecosystem. The cost is a different local
story (no local BigQuery), addressed by the store abstraction in ADR-006.

## ADR-002: raw_events is the durable analytical history, canonical only within analytics

**Status:** Accepted

**Context:** The claim "the read model is rebuildable from the events" is only
defensible if the events are retained somewhere durable. Pub/Sub retention is
finite and is not an event store.

**Decision:** The service persists every consumed event into a `raw_events` table
in BigQuery, and rebuilds all projections from that, never from the broker.
`raw_events` is canonical *within analytics*; it is a durable analytical copy of
facts the upstream services remain authoritative for.

**Consequences:** Rebuild is genuinely defensible, because the history it replays
is retained by this service, not borrowed from a broker that may have dropped it.
The wording is careful on purpose: analytics does not claim to be the ecosystem's
event store.

## ADR-003: Ingestion is separate from projection

**Status:** Accepted

**Context:** A consumer could try to insert the raw event and update every
projection in one step and treat that as atomic. Across a table and derived
aggregates that is fragile, and it conflates a durable fact with disposable
derived state.

**Decision:** The consumer's only durable action is to validate and idempotently
persist the raw event, then ack. Projections are materialized from `raw_events`
by a separate refresh and are disposable.

**Consequences:** A projection failure leaves analytics stale but loses no event,
and a rebuild repairs it. The durable invariant is small and clear (the raw
history), and the derived state is plainly throwaway, which is exactly what makes
the rebuild proof meaningful.

## ADR-004: One event per event_id, enforced by MERGE and by input canonicalization

**Status:** Accepted

**Context:** Delivery is at-least-once. BigQuery does not enforce a unique
constraint the way PostgreSQL did, so "one row per event_id" is a *logical*
invariant here, not a physical guarantee, and a check-then-insert would race
between a duplicate's two deliveries.

**Decision:** Two layers, both keyed on `event_id`, not on `payment_id`.
Ingestion is a single idempotent `MERGE` on `event_id` (insert when absent, do
nothing when present), the primary mechanism. Independently, every projection
first canonicalizes its input to one logical event per `event_id` before
computing anything, so even if the physical table ever held a duplicate row, each
projection sees the event once. Payment-specific metrics may then key on
`payment_id` for their own semantics (a payment has one received amount however
many lifecycle events it emits), but that is metric semantics, not the dedup
mechanism, distinct `payment_id` is *not* a general duplicate defence, because
the ecosystem also has transaction events, risk evaluations, and several
lifecycle events per payment.

**Consequences:** Redelivery is safe at the write with no read-modify-write race,
and the read model is defensible against duplicate rows independently of the
write (ABS-REQ-008). A future refinement, not needed for the current milestones:
the same `event_id` arriving with a *different* payload is an integrity conflict
(collision or corruption), distinct from an innocent duplicate delivery, and
would be surfaced rather than silently ignored.

## ADR-005: Projections are pure Python over raw_events, materialized

**Status:** Accepted

**Context:** Projections could be BigQuery SQL views, materialized views, or
computed in the service. The choice must serve determinism, testability and a
provable rebuild.

**Decision:** Projections are pure, deterministic Python functions of the raw
events, and the results are materialized as tables the read API serves. A refresh
recomputes them from `raw_events`; a rebuild drops and recomputes them.

**Consequences:** The exact rebuild is unit-testable with no cloud, because the
same functions run in the in-memory store and the BigQuery adapter, and the read
path is a cheap read of a small materialized result rather than a scan on every
request. The trade-off is that aggregation happens in the service rather than in
the warehouse; at a scale where the history no longer fits a chunked pass, the
aggregation would move into BigQuery SQL or a batch job, which this ADR
anticipates but does not pre-build (no Spark/Dataflow until a real need appears).

## ADR-006: A store port with in-memory and BigQuery adapters, one shared contract

**Status:** Accepted

**Context:** BigQuery cannot run in CI the way local PostgreSQL did, but the
service logic (idempotent ingest, projection, rebuild) must be fully tested.

**Decision:** Ingestion, projection and rebuild sit behind an `AnalyticsStore`
port. An in-memory adapter backs local runs and the full test suite; a BigQuery
adapter backs production. Both must satisfy one shared behavioural contract, run
in full against the in-memory store in CI and as a smoke subset against a
temporary BigQuery dataset during deployment.

**Consequences:** CI is fast and cloud-free, and the in-memory store is not a
convenient fake that could drift, it is held to the same contract as production.
BigQuery is exercised where it matters (deploy/live) without needing it on every
push.

## ADR-007: A strict sink that emits nothing

**Status:** Accepted

**Context:** The other consumers (risk, notification) still publish or could. A
projection service has no reason to.

**Decision:** Analytics emits no events, has no outbox and no broker client, and
has no path that writes to or blocks any financial system.

**Consequences:** Its isolation extends ABS-REQ-006: it can be stopped, broken or
behind while payments settle normally. There is simply no upstream write path to
compromise or to go wrong. This also removes the outbox and publisher machinery
the producing services carry.

## ADR-008: No Cloud SQL, no Alembic, no Secret Manager

**Status:** Accepted

**Context:** The parity instinct is to copy the other services' infrastructure.
Most of it does not belong here.

**Decision:** BigQuery is reached with the Cloud Run service account's own
credentials (Application Default Credentials), so there is no connection string,
no database password and no Secret Manager entry, and no relational schema, so no
Alembic. Infrastructure is added only where it is genuinely needed.

**Consequences:** A smaller, honest footprint that matches the service's shape
rather than mechanically mirroring the others. Access to BigQuery is governed by
IAM on the runtime service account, not by a secret.

## ADR-009: Projection refresh is a separate, scheduled operation

**Status:** Accepted

**Context:** Ingestion persists raw events and acks; projections are derived from
the raw history. Something has to trigger a projection refresh, and it must not
be a full history scan on every Pub/Sub message (which would be O(history) per
event).

**Decision:** Three distinct operations. **Ingest** persists one raw event and
acks, cheap, per-message. **Refresh** materializes the projections from the
current raw history, run on a schedule or on demand (Cloud Scheduler calling an
authenticated refresh, or a small Cloud Run Job using the same application
package), not per message. **Rebuild** is reset-then-refresh: destroy the derived
projections and recompute them from `raw_events` to prove equivalence. At
portfolio scale a refresh recomputes from the whole history; the chunked pass in
Milestone 2 keeps that from assuming the history fits in memory.

**Consequences:** The ack path stays cheap and the analytical work is batched off
it. Analytics lags the platform by at most a refresh interval, which is made
explicit by the watermark (ADR-010). Refresh and rebuild share the same pure
projection code, so a scheduled refresh and a from-scratch rebuild cannot
disagree.

## ADR-010: Money semantics, UTC buckets, and a projection watermark

**Status:** Accepted

**Context:** This is analytics, but it is still finance: aggregating money with
floats, mixing currencies, or bucketing by the wrong clock would all be quietly
wrong. And because analytics is eventually consistent, its answers need to say
what they are current through.

**Decision:** Monetary aggregates use `Decimal` in the service and `NUMERIC` in
BigQuery, never float, and are rendered as fixed-point strings. ABS is
single-currency (the event contracts carry a bare `amount` with no currency
dimension), so amounts add directly; a multi-currency ecosystem would group
aggregates by currency, and this is recorded so the assumption is explicit rather
than accidental. Daily buckets are by `occurred_at` in UTC (business time), not
`ingested_at`. Every projection refresh records a watermark, `raw_event_count`
and `as_of` (the latest ingest time in the set), so eventual consistency is
visible and a rebuild is compared against the exact same raw-event boundary.

**Consequences:** Aggregates are financially sound and reproducible. The watermark
makes the rebuild proof rigorous: "same raw-event set produces the same
projection", not merely "a rebuild ran". It is a deterministic function of the
raw set (not wall-clock), so a rebuild reproduces it identically; the wall-clock
"last refreshed" time is tracked separately as operational metadata and is
deliberately not part of the compared projection content.
