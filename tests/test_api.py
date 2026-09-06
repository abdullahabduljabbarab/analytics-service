"""The HTTP surface: health, the authenticated Pub/Sub consumer, and the read
endpoints. Ingestion and projection are separate, so a read is empty until an
(engineering) refresh runs.
"""

import base64
import json
from uuid import uuid4

from app import admin, config


def _envelope(event_type, payload=None, *, event_id=None, correlation_id=None):
    payload = payload or {}
    return {
        "event_id": event_id or str(uuid4()),
        "event_type": event_type,
        "event_version": 1,
        "occurred_at": "2026-09-01T12:00:00+00:00",
        "producer": "test",
        "correlation_id": correlation_id,
        "aggregate_id": payload.get("payment_id"),
        "payload": payload,
    }


def _push(envelope):
    data = base64.b64encode(json.dumps(envelope).encode("utf-8")).decode("utf-8")
    return {"message": {"data": data}, "subscription": "test"}


def _post(client, envelope):
    return client.post("/events/pubsub", json=_push(envelope))


def _push_ledger_style(event_type, payload, *, event_id=None):
    # The ledger's wire shape: the payload in the data, event_id and event_type
    # in Pub/Sub message attributes rather than in an ABS envelope.
    data = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return {
        "message": {
            "data": data,
            "attributes": {"event_id": event_id or str(uuid4()), "event_type": event_type},
        },
        "subscription": "test",
    }


def _load_one_payment(client, pid="p1", account="a1"):
    _post(client, _envelope("payment.received", {"payment_id": pid, "account_id": account, "amount": "100.00"}))
    _post(client, _envelope("risk.evaluated", {"payment_id": pid, "account_id": account, "decision": "allow", "score": 10}))
    _post(client, _envelope("payment.settled", {"payment_id": pid, "account_id": account}))


def test_health_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_ingest_records_but_does_not_refresh(client, store):
    _load_one_payment(client)
    # The raw events are persisted...
    assert store.raw_count() == 3
    # ...but the projections are not touched by ingestion.
    assert client.get("/analytics/overview").json()["overview"] == {}


def test_read_after_refresh_shows_the_aggregates(client, store):
    _load_one_payment(client)
    admin.refresh(store)

    body = client.get("/analytics/overview").json()
    assert body["overview"]["payments"] == 1
    assert body["overview"]["settled"] == 1
    assert body["overview"]["risk_allow"] == 1
    assert body["watermark"]["raw_event_count"] == 3
    assert body["watermark"]["as_of"] is not None


def test_push_is_idempotent_across_redelivery(client, store):
    env = _envelope("payment.received", {"payment_id": "p9", "account_id": "a9", "amount": "50.00"})
    first = _post(client, env)
    second = _post(client, env)
    assert first.json()["status"] == "recorded"
    assert second.json()["status"] == "duplicate"
    assert store.raw_count() == 1

    admin.refresh(store)
    assert client.get("/analytics/payments").json()["payments"]["total"] == 1


def test_read_endpoints_carry_a_watermark(client, store):
    _load_one_payment(client)
    admin.refresh(store)
    for path, key in [
        ("/analytics/payments", "payments"),
        ("/analytics/risk", "risk"),
        ("/analytics/providers", "providers"),
        ("/analytics/timeseries", "timeseries"),
    ]:
        body = client.get(path).json()
        assert "watermark" in body
        assert key in body


def test_account_endpoint(client, store):
    _load_one_payment(client, pid="pA", account="acct-1")
    admin.refresh(store)
    body = client.get("/analytics/accounts/acct-1").json()
    assert body["account"]["account_id"] == "acct-1"
    assert body["account"]["payments"] == 1
    assert body["account"]["settled"] == 1


def test_account_endpoint_unknown_account_is_empty(client, store):
    admin.refresh(store)
    body = client.get("/analytics/accounts/nobody").json()
    assert body["account"]["payments"] == 0


def test_push_requires_authentication_when_configured(client, monkeypatch):
    monkeypatch.setattr(config, "PUBSUB_PUSH_SA", "analytics-push@proj.iam.gserviceaccount.com")
    resp = _post(client, _envelope("payment.settled", {"payment_id": "p", "account_id": "a"}))
    assert resp.status_code == 401


def test_push_rejects_invalid_envelope(client):
    resp = client.post("/events/pubsub", json=_push({"nope": True}))
    assert resp.status_code == 400


def test_push_rejects_missing_message_data(client):
    resp = client.post("/events/pubsub", json={"message": {}})
    assert resp.status_code == 400


def test_ingests_ledger_attribute_style_events(client, store):
    # The ledger carries event_id/event_type in Pub/Sub attributes with the
    # payload in the data; analytics normalizes that into the same envelope.
    resp = client.post(
        "/events/pubsub",
        json=_push_ledger_style("transaction.deposit", {"transaction_id": str(uuid4()), "amount": "100.00"}),
    )
    assert resp.status_code == 200
    admin.refresh(store)
    assert client.get("/analytics/overview").json()["overview"]["transactions"] == 1
