# Security

## What this project is

A portfolio demonstration of event-sourced analytics and CQRS over a cloud data
warehouse. It is not a production analytics system, consumes simulated event
volume, and handles no real personal or financial data. It owns derived
analytical state only and can never move money.

## Boundaries

**No secret, no database credential.** The analytical store is BigQuery, reached
with the Cloud Run runtime service account's own IAM (Application Default
Credentials). There is no connection string, no database password, and no Secret
Manager entry to leak. Access is governed by IAM on a dedicated runtime identity,
not by a shared credential.

**Least-privilege runtime identity.** The Cloud Run service and the refresh Job
run as `analytics-service-runtime`, granted dataset-level `dataEditor` on the
analytics dataset and project-level `jobUser`, and nothing else, rather than the
broad default compute service account.

**Keyless CI.** The deploy pipeline authenticates to GCP through Workload Identity
Federation: GitHub Actions presents a short-lived OIDC token that GCP exchanges to
impersonate a repository-scoped deploy service account. No long-lived
service-account key is stored in the repository.

**No access to financial state.** Analytics never reads or writes the ledger,
orchestrator or risk engine, holds no credential to any of them, and has no
publish path. A compromise of analytics cannot move money or alter a payment; its
blast radius is its own derived read model, which is rebuildable from the retained
events.

**Input validation.** The Pub/Sub push envelope (in either wire shape) and the
read-API parameters are validated through Pydantic and FastAPI before the service
layer. A missing `event_id`/`event_type`, an undecodable payload, or a malformed
envelope is rejected with a 400.

**Idempotency and replay.** Ingestion is append-only and the one-per-event_id
invariant is enforced on read, so at-least-once redelivery never changes an
aggregate. The read model is deterministically rebuildable from `raw_events`.

**SQL safety.** All BigQuery access uses parameterized queries; no raw string
interpolation of untrusted values into SQL.

**Identifiers.** Events, payments and accounts use UUIDs; no sequential ids are
exposed.

**Error responses and logging.** Structured JSON errors with no stack traces; logs
record metadata, not payloads.

**HTTPS.** Terminated by Cloud Run. The application does not handle TLS.

## Authentication

The ingest endpoint is authenticated; the read and health endpoints are open by
scope decision. `POST /events/pubsub` is the only surface that mutates state (it
appends to the raw history), so it is protected: each of the three push
subscriptions attaches a Google OIDC token minted for a dedicated push service
account, and the endpoint verifies that token and the account it was issued to
before applying any event. `GET /health`, `/docs` and the `/analytics/*` reads
stay public, a deliberate scope decision, safe because analytics moves no money and
the reads expose only aggregate metrics over its own derived model.

## Known limitations

- No authentication on the read endpoints. In production, aggregate access might
  be scoped to authenticated internal callers; today the reads expose only
  simulated aggregate metrics.
- The refresh runs on a schedule, so aggregates lag the transactional platform by
  up to a refresh interval, made explicit by the watermark. This is by design
  (eventual consistency), not a security gap.
- Physical `raw_events` rows are not deduplicated at write (dedup is on read); a
  periodic compaction is optional maintenance.
- No rate limiting. Would be added via Cloud Armor or middleware.
- Encryption at rest relies on BigQuery's managed encryption. Customer-managed
  keys are outside project scope.
