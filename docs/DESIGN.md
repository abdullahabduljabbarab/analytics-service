# Analytics Service: Design

## Purpose

The analytics service is the ecosystem's analytical edge. It consumes the events every other service commits, from the ledger, the orchestrator and the risk engine, and turns them into read-optimized aggregates: payment volumes and settlement rates, risk decision distributions, provider performance, per-account activity, and daily platform metrics. It owns derived analytical truth, never financial truth. It never calls or writes back to the ledger, orchestrator, risk engine or notification service, and it emits no events of its own.

Its engineering thesis is one property: **the analytical read model is disposable, and rebuildable exactly from the retained event history.** Destroy every projection and regenerate it from the raw events, and the numbers come back identical. That is what event sourcing buys, and it is the flagship proof of this repo.

## Two worlds

```
   TRANSACTIONAL WORLD                        ANALYTICAL WORLD

   Ledger ───────────┐
                     │
   Orchestrator ─────┼── Pub/Sub ──▶ analytics-service
                     │                     │  idempotent ingest
   Risk ─────────────┘                     ▼
                                    BigQuery raw_events
                                           │  pure projections
                          ┌────────────────┼────────────────┐
                          ▼                ▼                 ▼
                       payments          risk            providers
                          │                │                 │
                          └────────────────┼─────────────────┘
                                           ▼
                                   Analytics Query API
```

The transactional services stay authoritative for the facts they commit. Analytics keeps a durable analytical *copy* of those facts in `raw_events` and derives everything from it. Nothing here is authoritative financial state; nothing here can move money.

## Strictly downstream, and eventually consistent

Analytics subscribes to `transaction-events`, `payment-events` and `risk-events`, and has no client for any other service and no publish path. Its isolation extends ABS-REQ-006: it can be stopped, broken, or hours behind while payments keep settling normally. Because it lags the transactional platform by design, its answers are explicitly analytical and never presented as authoritative current balances. Account balances belong to the ledger; analytics reports on activity, not truth.

## Ingestion and projection are separate concerns

This separation is the core of the design. The consumer does one durable thing:

```
event arrives
    ▼
validate the ABS envelope
    ▼
idempotently persist to raw_events   (keyed on event_id)
    ▼
ACK
```

Projections are **not** part of that path. They are disposable derived state, materialized from `raw_events` by a refresh and thrown away and recomputed by a rebuild. This gives a clean failure property:

```
raw event persisted
      │
      ├── projection succeeds → analytics current
      │
      └── projection fails → analytics temporarily stale
                             financial platform unaffected
                             a rebuild repairs everything
```

Trying to make "insert raw event, then update five projections" atomic would be fragile and pointless; the raw history is the durable invariant, and everything else is rebuildable from it.

## Keeping projections current

If ingestion does not build projections, something must. It is *not* a full
history scan on every message, that would be O(history) per event. Instead there
are three operations:

```
INGEST   one raw event persisted, then ACK          cheap, per message
REFRESH  materialize projections from raw history    scheduled / on demand
REBUILD  reset then refresh, to prove equivalence    operational / test
```

A **refresh** is triggered off the ack path, by Cloud Scheduler calling an authenticated refresh, or a small Cloud Run Job using the same application package. Analytics then lags the platform by at most a refresh interval, which the watermark below makes explicit. A **rebuild** is reset-then-refresh over the same raw set, and is the flagship proof rather than a routine operation.

## Idempotent ingestion, and duplicate defence by event_id

Delivery is at-least-once, so the same event arrives more than once. "One row per `event_id`" is a *logical* invariant, not a physical BigQuery constraint. It is upheld in two layers, both on `event_id`: ingestion is a single idempotent `MERGE` on `event_id` (the primary mechanism), and every projection independently canonicalizes its input to one logical event per `event_id` before computing anything, so a duplicate row could never double-count. Distinct `payment_id` is deliberately *not* the dedup mechanism, the ecosystem has transaction events, risk evaluations and several lifecycle events per payment; payment-specific metrics key on `payment_id` only for their own semantics (one received amount per payment), on top of the event_id canonicalization.

## The watermark: eventual consistency made visible

Every projection carries a watermark, `raw_event_count` and `as_of` (the latest ingest time in the set it was built from). This does two things. It makes the service's eventual consistency visible rather than merely documented (a portal can show "current through 18:42 UTC"), and it makes the rebuild proof rigorous: determinism means *the same raw-event set produces the same projection*, so the rebuild is compared against the same watermark, not merely re-run. The watermark is a deterministic function of the raw set, so a rebuild reproduces it identically; the wall-clock "last refreshed" time is separate operational metadata, not part of the compared projection content.

## The raw event history

`raw_events` is the durable analytical history and the source of every projection. One row per `event_id`:

`event_id`, `event_type`, `event_version`, `occurred_at`, `producer`, `correlation_id`, `causation_id`, `aggregate_id`, `account_id`, `payment_id`, `payload` (JSON), `ingested_at`.

`account_id` and `payment_id` are lifted into nullable convenience columns *where the event contract defines an unambiguous single value*; the payload stays authoritative, and events whose contract has no such value leave them null. The `correlation_id` is kept so a row in the analytical history traces back to the same request as the payment and risk decision that produced it (ABS-REQ-009).

### Event contract audit

The projections were written only after auditing what the three producers actually emit, the same discipline that caught the missing `account_id` on the orchestrator's events. Two findings shaped the design. First, **ledger transaction events carry no account fields** (their payload is `transaction_id`, `type`, `amount`, `idempotency_key`, `reference`), because a double-entry transaction concerns more than one account. So per-account analytics derives from *payment* events, which do carry `account_id`, and transactions contribute only at the platform level (a count, and volume). No metric requires a field the contracts do not provide, so analytics never calls upstream to fill a gap.

Second, discovered live as the first consumer of `transaction-events`: **the ledger emits events in a different wire shape** than the orchestrator and risk engine. The orchestrator and risk put the full ABS envelope in the Pub/Sub message data; the ledger puts the payload in the data and carries `event_id` and `event_type` in Pub/Sub message *attributes*. Analytics normalizes both into one envelope at ingest, so it consumes all three streams uniformly without asking the ledger to change a format its own consumers already use.

## Projections

Each projection is a pure, deterministic function of the raw history:

- **payments**: total, settled, failed, rejected, total value, settlement success rate
- **risk**: allow / review / block distribution and average score
- **providers**: per-provider succeeded / failed / unknown and success rate
- **accounts**: per-account payment count, settled count and value
- **timeseries**: payments, settled and value per day
- **overview**: the headline numbers, combining the above

Whether these are physical tables, views or materialized views in BigQuery is chosen and argued in [DECISIONS.md](DECISIONS.md): they are materialized tables, recomputed by pure Python over `raw_events`, which is what makes the rebuild exact and unit-testable. Money is `Decimal` in the service and `NUMERIC` in BigQuery, never float; ABS is single-currency so amounts add directly (a multi-currency ecosystem would group by currency); and daily buckets are by `occurred_at` in UTC, business time, not ingest time.

## The store contract

Ingestion, projection and rebuild sit behind an `AnalyticsStore` port with two implementations: an in-memory store (local and the full test suite) and a BigQuery adapter (production). Both satisfy one shared behavioural contract: a first event is accepted, the same `event_id` twice contributes once, different ids contribute independently, correlations survive persistence, a reset removes derived state only, a rebuild reproduces the original result, and replaying the whole history twice is identical. The in-memory store runs the contract in full in CI; the BigQuery adapter runs a smoke subset against a temporary dataset during live verification, so BigQuery is not needed on every push.

## The API

A small read-only surface: `GET /health`, `GET /analytics/overview`, `/analytics/payments`, `/analytics/risk`, `/analytics/providers`, `/analytics/accounts/{account_id}`, and `/analytics/timeseries`. Plus the authenticated `POST /events/pubsub` ingest. There are no mutation endpoints for analytical data; the rebuild is an engineering/operational operation, not a public write. (API arrives in Milestone 3.)

## What this is not

It is not a dashboarding product, not a machine-learning system, and not a real-time balance service. It never emits an event, never writes upstream, and holds no credential that could move money. It is a downstream projection service whose job is to turn the event history into answers, and to be able to throw those answers away and reproduce them exactly.
