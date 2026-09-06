# Production Log

A running record of what was built, in what order, and why. Newest last.

## Milestone 1: Deterministic analytics core

**Goal:** The event-sourced heart of the service, with no cloud dependency, so
the projection logic and the rebuild guarantee are proven before any
infrastructure exists.

**Built:**
- `app/envelope.py`: the ABS event envelope (`AbsEvent`) and the normalized
  `RawEvent` analytics retains. Ingest lifts `account_id` and `payment_id` out of
  the payload so every projection sees them uniformly, keeps the full payload and
  the `correlation_id`, and always has an `occurred_at` to bucket daily rollups
  on.
- `app/projections.py`: the deterministic read model. Pure functions from raw
  events to aggregates: `payments`, `risk`, `providers`, `account`/`accounts`,
  `timeseries`, and `overview`. Counts are over distinct payment ids and values
  are picked in a canonical order, so projections are order-independent and
  robust to a duplicated raw input (belt and braces on the store's dedup).
- `app/store.py`: the `AnalyticsStore` port and the `InMemoryStore` adapter.
  Ingestion (idempotent raw persistence keyed on `event_id`) is separated from
  projection (materialized, disposable derived state) by construction: `refresh`
  recomputes projections from the raw history, `reset` drops them, `rebuild`
  regenerates them. `get_store` selects a backend.
- `app/config.py`: BigQuery project/dataset/location, `PUBSUB_PUSH_SA`, and the
  store backend selector. No database URL, no secret.
- Scaffold: `requirements.txt` (no cloud dependency yet), `pyproject.toml`,
  `.gitignore`, `LICENSE`.
- `docs/`: `DESIGN.md`, `REQUIREMENTS.md`, `MVP.md`, `DECISIONS.md` (8 ADRs), and
  this log.

**Tests:** 22, all against the pure core and the in-memory store, no cloud.
- `test_envelope.py`: id lifting, the `aggregate_id` fallback, and occurred-at
  normalization.
- `test_projections.py`: the known-scenario aggregates, order independence, a
  duplicated raw event counting once, provider success rate, risk average,
  daily buckets, per-account scoping, and the empty history.
- `store_contract.py` + `test_inmemory_store.py`: the shared store contract (a
  first event accepted, the same id twice contributing once, independent ids,
  correlations surviving, reset removing derived state only, rebuild reproducing
  the original, replay-twice identical), plus the flagship proof that a full
  rebuild regenerates every projection identically while the raw history is
  untouched.

**Decisions recorded:** ADR-001 (BigQuery, not Cloud SQL), ADR-002 (raw_events as
the durable analytical history), ADR-003 (ingestion separate from projection),
ADR-004 (one event per event_id via MERGE and input canonicalization), ADR-005
(pure Python materialized projections), ADR-006 (store port + shared contract),
ADR-007 (strict sink, emits nothing), ADR-008 (no Cloud SQL / Alembic / Secret
Manager), ADR-009 (projection refresh is a separate scheduled operation),
ADR-010 (money/UTC semantics and the projection watermark).

**Event contract audit (done before writing projections).** Verified what the
three producers actually emit. Payment events carry what is needed
(`payment.received` has `amount` and `account_id`, provider events carry
`provider`); risk events carry `decision` and `score`. The finding that shaped
the design: **ledger transaction events carry no account fields** (payload is
`transaction_id`, `type`, `amount`, `idempotency_key`, `reference`), because a
double-entry transaction concerns more than one account. So per-account analytics
derives from payment events, transactions contribute only platform-level counts,
and no metric needs a field the contracts lack, so analytics never calls
upstream. (The ecosystem EVENT_CATALOGUE overstates the transaction payload; not
corrected here since analytics does not depend on those fields.)

**Precision changes applied before freezing M1** (from design review): duplicate
defence canonicalizes projection input by `event_id`, not by per-metric
`payment_id`; "one row per event_id" framed as a logical invariant (BigQuery has
no unique constraint), upheld by `MERGE` plus input canonicalization; the
projection **refresh** mechanism defined as a separate scheduled operation
distinct from ingest and rebuild; a deterministic **watermark**
(`raw_event_count`, `as_of`) added so eventual consistency is visible and the
rebuild compares against the same raw-event set; `account_id`/`payment_id` framed
as nullable convenience columns with the payload authoritative; and money kept as
`Decimal`, single-currency, with UTC daily bucketing.

**State:** `ruff check` clean, 25 tests passing. No infrastructure yet; the
BigQuery adapter and its schema are Milestone 2.

## Milestone 2: BigQuery

**Goal:** The production analytical store: the durable `raw_events` history and
the materialized projections in BigQuery, with idempotent ingestion and a
rebuild that does not assume the whole history fits in memory.

**Built:**
- `app/projections.py` (extended): a `StreamingProjectionBuilder` that folds
  events one at a time, keeping only O(distinct payments / accounts / days)
  state rather than O(events). Its precondition is one event per `event_id`
  (which `raw_events` guarantees via MERGE), so it does no in-memory dedup. A
  test asserts its output is byte-for-byte identical to the reference list build,
  so the streaming and reference paths cannot drift. This is what lets a rebuild
  stream `raw_events` rather than load it all.
- `app/bigquery_store.py`: the `BigQueryStore` adapter, the production backend
  behind the same store contract. Two tables defined as code:
  - `raw_events` (one logical row per `event_id`): ingestion is a single
    `MERGE` on `event_id`, so at-least-once redelivery cannot race, and
    `num_dml_affected_rows` tells whether the event was new.
  - `projections` (one row per projection name, JSON plus the watermark): a
    refresh streams `raw_events` in canonical order through the streaming
    builder and writes the results with DML (strongly consistent, so a read
    right after a refresh sees the new values, unlike a streaming insert's
    buffer). `reset` deletes the projection rows; `rebuild` is reset then
    refresh. `ensure_tables` creates the dataset and tables idempotently, the
    analytics analogue of running migrations on start.
  - The module imports the BigQuery client and is loaded only when the store
    backend is "bigquery"; CI (backend "memory") never imports it, so the suite
    needs no cloud.
- `requirements.txt`: added `google-cloud-bigquery`.

**Tests:** +3 (28 total, in CI) plus 1 skipped BigQuery smoke.
- `test_projections.py`: the streaming builder matches the reference build (on
  the known scenario and on empty history) and is order-independent for the
  scenario, all in CI with no cloud.
- `test_bigquery_smoke.py`: a subset of the store contract against a real
  temporary dataset (schema creation, MERGE idempotency, streaming refresh,
  destroy-and-rebuild equivalence), skipped unless `RUN_BQ_SMOKE=1`, run during
  deployment/live verification so BigQuery is exercised where it matters without
  being needed on every push.

**State:** `ruff check` clean, 28 tests passing (1 skipped). The adapter is
code-complete and unit-proven at its streaming core; the live smoke against a
temporary dataset runs at deployment. Next: M3, the service and API.

## Milestone 3: Service and API

**Goal:** The HTTP surface, an authenticated consumer that ingests without
touching projections, read endpoints over the materialized aggregates, and the
refresh/rebuild as an engineering command rather than a public mutation.

**Built:**
- `app/main.py`: the FastAPI app.
  - `POST /events/pubsub`: the authenticated consumer. It verifies the Pub/Sub
    OIDC token (same pattern as the risk engine and notification service, skipped
    when `PUBSUB_PUSH_SA` is unset), validates the ABS envelope, and persists the
    raw event, that is the whole acked action. Projections are not refreshed on
    this path (ADR-003), so the consumer never rescans history; it returns
    `recorded` or `duplicate`.
  - `GET /health`: a liveness probe returning the store backend.
  - `GET /analytics/overview`, `/payments`, `/risk`, `/providers`, `/timeseries`,
    and `/accounts/{account_id}`: read-only endpoints over the materialized
    projections, each carrying the watermark so eventual consistency is visible.
  - No public mutation endpoint for analytical data.
- `app/admin.py`: the operational commands, `refresh`, `rebuild` and `status`,
  as a CLI (`python -m app.admin ...`), not HTTP. In production these run as a
  Cloud Run Job against the shared BigQuery store: a scheduled `refresh` keeps
  the read model current off the ingest path, and `rebuild` is the flagship proof
  (destroy and regenerate from `raw_events`). `main` runs `ensure_tables` first
  when the backend supports it.
- `app/schemas.py`: the Pub/Sub push envelope.

**Tests:** +14 (42 total, 1 BigQuery smoke skipped).
- `test_api.py`: health; ingestion records but does not refresh (the separation
  made visible); read-after-refresh shows the aggregates with a watermark; push
  idempotent across redelivery (`recorded` then `duplicate`); every read endpoint
  carries a watermark; the account endpoint and its empty case; authentication
  required when configured; 400s for a bad envelope and missing data.
- `test_admin.py`: refresh materializes, rebuild reproduces the same result,
  status reports raw count and watermark, and status before any refresh shows an
  empty watermark.

**State:** `ruff check` clean, 42 tests passing (1 skipped). The service is
functionally complete against the in-memory store. Next: M4, deployment
(Dockerfile, CI against the in-memory store, Terraform with a BigQuery dataset,
three push subscriptions plus a dead-letter, a scheduled refresh job, and WIF).

## Milestone 4: Deployment

**Goal:** Ship to Cloud Run keylessly with BigQuery as the store, consuming all
three upstream topics, with a scheduled refresh, and the whole footprint in
Terraform, no Cloud SQL, no secret, no outbox.

**Built:**
- `Dockerfile`, `start.sh`, `.dockerignore`: a slim Python 3.12 image that runs
  `python -m app.admin ensure` on start (creates the BigQuery dataset and tables
  if absent, the analytics analogue of running migrations) then serves uvicorn.
- `.github/workflows/ci.yml`: lint, then test against the in-memory store, no
  database service and no cloud, since the suite defaults to
  `STORE_BACKEND=memory`. On a push to main, a deploy job authenticates via
  Workload Identity Federation and deploys both the API service and the
  `analytics-refresh` Cloud Run Job from the same image, with
  `STORE_BACKEND=bigquery` and the BigQuery dataset in the environment.
- `.github/workflows/terraform.yml`: fmt, init, validate on Ubuntu.
- `terraform/`: the full footprint.
  - A **BigQuery dataset** (`analytics`) as the store, with the runtime service
    account granted `dataEditor` on the dataset and `jobUser` on the project, no
    Cloud SQL, no connection string, no secret.
  - Artifact Registry, the Cloud Run service (public invoker, ingest protected at
    the app layer), and the `analytics-refresh` Cloud Run Job.
  - A dedicated push identity and **three** push subscriptions, one on each of
    `transaction-events`, `payment-events` and `risk-events`, all into the single
    `/events/pubsub`, with a shared dead-letter topic.
  - A **Cloud Scheduler** job that runs the refresh Job on a schedule, off the
    ingest path, with its own identity permitted to execute it.
  - A least-privilege deploy account bound to the analytics-service repository
    through the shared WIF pool (referenced, not recreated).
- No publish path, no broker client, no outbox: analytics is a strict sink and
  the infrastructure reflects that (it only subscribes).

**State:** `ruff check` clean, 42 tests passing (1 skipped). Deployment is defined
and CI is wired; the live apply, the BigQuery smoke, and the end-to-end evidence
(including the flagship rebuild proof) are Milestone 5.
