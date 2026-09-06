"""The envelope validates and normalizes: it lifts account and payment ids out
of the payload and always has an occurred date to bucket on.
"""

from datetime import datetime, timezone

from app.envelope import AbsEvent, to_raw_event


def test_lifts_ids_from_the_payload():
    raw = to_raw_event(
        AbsEvent(
            event_id="e1",
            event_type="payment.settled",
            occurred_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
            correlation_id="corr-1",
            payload={"payment_id": "p1", "account_id": "a1"},
        )
    )
    assert raw.payment_id == "p1"
    assert raw.account_id == "a1"
    assert raw.correlation_id == "corr-1"
    assert raw.occurred_date == "2026-09-01"


def test_payment_id_falls_back_to_aggregate_id():
    raw = to_raw_event(
        AbsEvent(event_id="e2", event_type="payment.received", aggregate_id="agg-9", payload={})
    )
    assert raw.payment_id == "agg-9"


def test_missing_occurred_at_falls_back_to_ingest_time():
    raw = to_raw_event(AbsEvent(event_id="e3", event_type="x", payload={}))
    assert raw.occurred_at is not None
    assert raw.occurred_at.tzinfo is not None


def test_naive_occurred_at_is_treated_as_utc():
    raw = to_raw_event(
        AbsEvent(event_id="e4", event_type="x", occurred_at=datetime(2026, 9, 1, 10, 0), payload={})
    )
    assert raw.occurred_at.tzinfo is not None
    assert raw.occurred_date == "2026-09-01"
