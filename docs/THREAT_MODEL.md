# Threat Model (STRIDE)

## Scope

This threat model covers the analytics service: a downstream, event-sourced
projection service deployed on GCP Cloud Run with a BigQuery store, fed by three
Pub/Sub push subscriptions and exposing a read API. It does not cover the other
services' own threat models, network-level DDoS, or physical security of cloud
infrastructure.

## Assets

| Asset | Sensitivity | Location |
|-------|-------------|----------|
| Raw event history (`raw_events`) | Medium (the durable analytical copy; rebuild source) | BigQuery |
| Materialized projections | Low (derived, disposable, rebuildable) | BigQuery |
| The integrity of an aggregate (deterministic from history) | Medium | Enforced by pure projections + event_id dedup |
| Runtime identity (BigQuery access) | High | GCP IAM, dedicated runtime SA |
| Deploy identity (WIF) | Critical | GCP IAM, no key stored |
| Isolation from financial state | Critical (defining property) | Architecture (no write path exists) |

## Threat Analysis

### S: Spoofing

| Threat | Mitigation | Test |
|--------|------------|------|
| Attacker forges events to skew aggregates | The ingest endpoint requires a Google OIDC token minted for the dedicated push service account, verified before any event is applied. | `test_push_requires_authentication_when_configured`; live, an unauthenticated call returns 401 |
| Attacker replays events to inflate counts | Ingestion is append-only and the one-per-event_id invariant is enforced on read, so a redelivery never changes an aggregate. | `test_same_event_id_twice_contributes_once`, `test_duplicate_event_id_is_canonicalized_away` |

### T: Tampering

| Threat | Mitigation | Test |
|--------|------------|------|
| Corrupting the derived read model | Projections are disposable and deterministically rebuildable from `raw_events`; a tampered projection is repaired by a rebuild. | `test_full_rebuild_regenerates_every_projection_identically`; live rebuild proof |
| SQL injection via API or ingest | Parameterized BigQuery queries; Pydantic validates and coerces input first. | `test_push_rejects_invalid_envelope`, `test_account_endpoint` |
| Malformed or mis-shaped envelopes | Both wire shapes are validated; an envelope without event_id/event_type is a 400. | `test_push_rejects_invalid_envelope`, `test_ingests_ledger_attribute_style_events` |

### R: Repudiation

| Threat | Mitigation | Test |
|--------|------------|------|
| Denial that an aggregate reflects real events | Every aggregate is a deterministic function of the retained raw history and carries a watermark naming the exact raw-event set it was built from. | `test_watermark_reflects_the_canonical_raw_set`; live rebuild with matching watermark |
| An event cannot be tied to a request | Each raw event retains its `correlation_id`, so a row traces to the same request as the payment and risk decision that produced it. | live, one correlation_id spanned events across the orchestrator and risk engine |

### I: Information Disclosure

| Threat | Mitigation | Test |
|--------|------------|------|
| Enumeration of accounts | Accounts and payments are keyed by UUID; the account endpoint is fetched by id, not a sequential key. | `test_account_endpoint_unknown_account_is_empty` |
| Stack traces or internals in responses | Structured JSON errors, no stack traces. | FastAPI exception handling |
| Payload leakage in logs | Logs record metadata, not payloads. | Log usage in `app/main.py` |

### D: Denial of Service

| Threat | Mitigation | Test |
|--------|------------|------|
| A poison event retried forever | A dead-letter policy on each subscription bounds delivery attempts. | Dead-letter policy in `terraform/` |
| Ingestion overwhelmed by a burst | Append-only streaming inserts have no per-table concurrency limit (unlike the DML path they replaced); Cloud Run autoscales. | the DML-limit finding and fix; live burst ingestion |
| Expensive read queries | Reads hit small materialized projection rows, not scans of the raw history. | read endpoints serve materialized tables |

### E: Elevation of Privilege

| Threat | Mitigation | Test |
|--------|------------|------|
| Compromise of analytics to move money | Analytics holds no financial credential and has no write path to financial state; there is nothing to elevate to. | strict-sink design (ABS-REQ-006) |
| Over-broad BigQuery access | The runtime identity has dataset-level dataEditor and project jobUser only, not the broad default compute SA. | runtime SA IAM in `terraform/` |
| Compromise of the deploy identity | Keyless: a repository-scoped Workload Identity binding, not a stored key. | WIF binding in `terraform/` |

## Mitigations Not Yet Implemented

| Gap | Risk | Priority |
|-----|------|----------|
| Authentication on the read endpoints | Low: exposes only simulated aggregates | Would scope to internal callers in production |
| Physical dedup compaction of `raw_events` | Low: storage growth only; correctness unaffected | Optional maintenance |
| Rate limiting | Low: API abuse | Would add via Cloud Armor or middleware |

## Requirement-to-Test Traceability

| Requirement | Tests |
|-------------|-------|
| Isolation from financial state (ABS-REQ-006) | strict-sink design; live, analytics erroring never touched a payment |
| Consumers tolerate duplicate delivery (ABS-REQ-008) | `test_same_event_id_twice_contributes_once`, `test_duplicate_event_id_is_canonicalized_away`, `test_replaying_raw_history_twice_is_identical` |
| One correlation id across services (ABS-REQ-009) | live correlation trace across producers in `raw_events` |
| Deterministic, rebuildable read model | `test_full_rebuild_regenerates_every_projection_identically`, `test_streaming_builder_matches_the_reference_build`; live rebuild proof |
