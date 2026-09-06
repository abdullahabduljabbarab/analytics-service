"""Shared test helpers: build raw events and a known scenario with known
aggregate outcomes, so projection and rebuild tests assert against fixed
numbers.
"""

import uuid
from datetime import datetime, timezone

from app.envelope import AbsEvent, RawEvent, to_raw_event

_BASE_DAY = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)


def ev(
    event_type: str,
    payload: dict | None = None,
    *,
    event_id: str | None = None,
    occurred_at: datetime | None = None,
    correlation_id: str | None = None,
    producer: str | None = None,
) -> RawEvent:
    payload = payload or {}
    return to_raw_event(
        AbsEvent(
            event_id=event_id or str(uuid.uuid4()),
            event_type=event_type,
            occurred_at=occurred_at or _BASE_DAY,
            correlation_id=correlation_id,
            producer=producer,
            aggregate_id=payload.get("payment_id"),
            payload=payload,
        )
    )


def known_scenario() -> list[RawEvent]:
    """Three payments with known outcomes:
      P1: received 100, risk allow (score 10), NorthPay succeeded, settled
      P2: received 200, risk review (score 55), NorthPay failed, failed
      P3: received 300, risk block (score 80), rejected
    plus one ledger transfer. Aggregate outcomes are asserted in the tests.
    """
    a1, a2, a3 = (str(uuid.uuid4()) for _ in range(3))
    p1, p2, p3 = (str(uuid.uuid4()) for _ in range(3))
    return [
        ev("payment.received", {"payment_id": p1, "account_id": a1, "amount": "100.00", "destination": "acme"}, producer="payment-orchestrator"),
        ev("risk.evaluated", {"payment_id": p1, "account_id": a1, "decision": "allow", "score": 10}, producer="risk-engine"),
        ev("payment.provider_succeeded", {"payment_id": p1, "provider": "NorthPay"}, producer="payment-orchestrator"),
        ev("payment.settled", {"payment_id": p1, "account_id": a1}, producer="payment-orchestrator"),

        ev("payment.received", {"payment_id": p2, "account_id": a2, "amount": "200.00", "destination": "globex"}, producer="payment-orchestrator"),
        ev("risk.evaluated", {"payment_id": p2, "account_id": a2, "decision": "review", "score": 55}, producer="risk-engine"),
        ev("payment.provider_failed", {"payment_id": p2, "provider": "NorthPay"}, producer="payment-orchestrator"),
        ev("payment.failed", {"payment_id": p2, "account_id": a2, "reason": "provider_failed"}, producer="payment-orchestrator"),

        ev("payment.received", {"payment_id": p3, "account_id": a3, "amount": "300.00", "destination": "initech"}, producer="payment-orchestrator"),
        ev("risk.evaluated", {"payment_id": p3, "account_id": a3, "decision": "block", "score": 80}, producer="risk-engine"),
        ev("payment.rejected", {"payment_id": p3, "account_id": a3, "reasons": ["HIGH_VALUE"], "score": 80}, producer="payment-orchestrator"),

        ev("transaction.transfer", {"transaction_id": str(uuid.uuid4()), "amount": "100.00"}, producer="ledger-api"),
    ]
