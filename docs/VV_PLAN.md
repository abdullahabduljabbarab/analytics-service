# Verification and Validation Plan

## Approach

Every requirement the analytics service owns is verified by an automated test, and
every behaviour that can be driven deterministically is additionally proven against
the live BigQuery deployment. CI runs the full suite (44 tests) against the
in-memory store on every push, with no cloud and no database service, and the
BigQuery adapter is smoke-tested against a temporary dataset at deploy. The
requirements are the ABS system requirements the service is accountable for,
defined in [SYSTEM_REQUIREMENTS.md](https://github.com/abdullahabduljabbarab/abs-financial-systems/blob/main/SYSTEM_REQUIREMENTS.md).

## Requirement-to-Test Mapping

| Requirement | Verification | Test / Evidence |
|-------------|-------------|-----------------|
| ABS-REQ-006 (isolation from financial state) | By design + Live | Strict sink: no write path, no broker client; live, analytics hit ingestion errors and lagged while payments settled normally, unaffected |
| ABS-REQ-008 (consumers tolerate duplicate delivery) | Automated + Live | `test_same_event_id_twice_contributes_once`, `test_duplicate_event_id_is_canonicalized_away`, `test_replaying_raw_history_twice_is_identical`; live, redelivered events left aggregates unchanged |
| ABS-REQ-009 (one correlation_id across services) | Automated + Live | `test_correlations_survive_persistence`; live, one correlation_id spanned 7 events across the orchestrator and risk engine in `raw_events` |
| Deterministic, rebuildable read model | Automated + Live | `test_full_rebuild_regenerates_every_projection_identically`, `test_streaming_builder_matches_the_reference_build`; live, destroy-and-rebuild reproduced every aggregate byte-for-byte at the same watermark |

## Behavioural Coverage

| Behaviour | Verification | Test / Evidence |
|-----------|-------------|-----------------|
| Projections match the known scenario | Automated | `test_overview_matches_the_known_scenario`, `test_providers_success_rate`, `test_risk_average_score`, `test_timeseries_buckets_by_day`, `test_account_activity_is_scoped_to_the_account` |
| Projections are order-independent and dedup by event_id | Automated | `test_projections_are_order_independent`, `test_a_duplicated_raw_event_contributes_once`, `test_duplicate_event_id_is_canonicalized_away` |
| Money is summed once per payment; empty history is zero | Automated | `test_amount_is_summed_once_per_payment_even_if_received_repeats`, `test_empty_history_is_all_zeros` |
| The watermark reflects the raw set deterministically | Automated | `test_watermark_reflects_the_canonical_raw_set`, `test_watermark_of_empty_history` |
| The streaming builder equals the reference build | Automated | `test_streaming_builder_matches_the_reference_build`, `test_streaming_builder_matches_reference_on_empty_history`, `test_streaming_builder_is_order_independent_for_the_scenario` |
| Ingestion is idempotent and separate from projection | Automated | `test_ingest_records_but_does_not_refresh`, `test_push_is_idempotent_across_redelivery`, store contract |
| The store contract holds (accept, dedup, correlations, reset, rebuild, replay) | Automated | `StoreContract` run against `InMemoryStore` |
| A full rebuild regenerates every projection identically | Automated + Live | `test_full_rebuild_regenerates_every_projection_identically`; live rebuild proof |
| Refresh / rebuild / status operate on a store | Automated | `test_refresh_materializes_projections`, `test_rebuild_reproduces_the_same_result`, `test_status_reports_raw_count_and_watermark` |
| The consumer accepts both wire formats | Automated + Live | `test_ingests_ledger_attribute_style_events`; live, ledger transaction events ingested alongside envelope-style events |
| Read endpoints serve aggregates with a watermark | Automated | `test_read_after_refresh_shows_the_aggregates`, `test_read_endpoints_carry_a_watermark`, `test_account_endpoint` |
| The ingest endpoint requires authentication; bad envelopes rejected | Automated + Live | `test_push_requires_authentication_when_configured`, `test_push_rejects_invalid_envelope`, `test_push_rejects_missing_message_data`; live 401 |
| The BigQuery adapter honours the store contract | Smoke (deploy) | `test_bigquery_adapter_honours_the_store_contract` against a temporary dataset |
| Lint and infrastructure validity | CI evidence | ruff on push; `terraform fmt`, `init`, `validate` in the Terraform workflow |

## Live Verification

Driven against the deployed service and the rest of the live ecosystem:

- `GET /health` returns 200 with the BigQuery backend, and `ensure` creates the
  `raw_events` and `projections` tables on start.
- An unauthenticated `POST /events/pubsub` returns 401; all three push
  subscriptions (transaction, payment, risk) deliver to the endpoint.
- All three streams feed `raw_events` (payment, risk and transaction events), the
  ledger's attribute-style events consumed alongside the others' envelope style.
- The refresh Job materializes the projections from `raw_events`, and the read API
  serves them with a watermark (152 events, a live overview of 27 payments, 16
  settled, a risk distribution and provider and transaction counts).
- The flagship rebuild: the materialized projections were destroyed (raw history
  intact at 152), then regenerated from `raw_events`, reproducing every aggregate
  byte-for-byte at the identical watermark.
- One correlation_id spanned events across the orchestrator and risk engine in the
  analytical history.

## Acceptance Criteria

The analytics service passes V&V when:

- Every ABS requirement it owns has an automated test, and every deterministic
  behaviour is additionally shown live.
- CI is green: ruff, the full suite against the in-memory store, and Terraform
  validation; the BigQuery adapter is smoke-tested at deploy.
- The live read model is served from BigQuery and is deterministically
  rebuildable from the retained event history, and no analytics behaviour, error
  or lag included, changes a payment's outcome.
