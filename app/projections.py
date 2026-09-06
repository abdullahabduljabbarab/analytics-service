"""Projections: pure functions from raw events to read-optimized aggregates.

These are the analytical read model. Each is a deterministic function of the raw
event history and nothing else, no clock, no database, no ordering assumption
about how events arrived. That is what makes the read model rebuildable: destroy
the materialized projections, run these functions over the same raw events again,
and the result is identical.

Duplicate defence is at the input boundary, not inside individual metrics. Every
projection first canonicalizes its input to one logical event per `event_id`
(`_canonical`), so even if the physical store somehow held a duplicate row, each
projection sees the event once. Payment-specific metrics may then additionally
key on `payment_id` according to their own semantics (a payment has one received
amount however many lifecycle events it emits), but that is metric semantics, not
the dedup mechanism.

Money is `Decimal` throughout and rendered as a fixed-point string; ABS is
single-currency, so amounts add directly (a multi-currency ecosystem would group
by currency instead). Daily buckets are by `occurred_at` in UTC, business time,
not ingest time.
"""

from __future__ import annotations

import decimal
from collections import defaultdict
from collections.abc import Iterable
from decimal import Decimal

from app.envelope import RawEvent

RECEIVED = "payment.received"
SETTLED = "payment.settled"
FAILED = "payment.failed"
REJECTED = "payment.rejected"
PROVIDER_SUCCEEDED = "payment.provider_succeeded"
PROVIDER_FAILED = "payment.provider_failed"
PROVIDER_UNKNOWN = "payment.unknown"
RISK_EVALUATED = "risk.evaluated"
TRANSACTION_TYPES = {
    "transaction.transfer",
    "transaction.deposit",
    "transaction.withdrawal",
}

_PROVIDER_EVENTS = {PROVIDER_SUCCEEDED, PROVIDER_FAILED, PROVIDER_UNKNOWN}


def _ordered(events: Iterable[RawEvent]) -> list[RawEvent]:
    """Canonical order for any projection that must pick a value deterministically:
    business time, then event id as a stable tie-break."""
    return sorted(events, key=lambda e: (e.occurred_at, e.event_id))


def _canonical(events: Iterable[RawEvent]) -> list[RawEvent]:
    """Normalize the input to one logical event per event_id, in canonical order.
    This is the general duplicate defence: projections are computed over the
    canonical stream, so a duplicated physical row cannot double-count."""
    seen: dict[str, RawEvent] = {}
    for e in _ordered(events):
        seen.setdefault(e.event_id, e)
    return list(seen.values())


def _money(value) -> Decimal:
    try:
        return Decimal(str(value))
    except (decimal.InvalidOperation, TypeError):
        return Decimal("0")


def _distinct_payments(events: Iterable[RawEvent], event_type: str) -> set[str]:
    return {e.payment_id for e in events if e.event_type == event_type and e.payment_id}


def payments(events: Iterable[RawEvent]) -> dict:
    events = _canonical(events)
    received: dict[str, Decimal] = {}
    for e in _ordered(events):
        if e.event_type == RECEIVED and e.payment_id:
            received.setdefault(e.payment_id, _money(e.payload.get("amount")))

    settled = len(_distinct_payments(events, SETTLED))
    failed = len(_distinct_payments(events, FAILED))
    rejected = len(_distinct_payments(events, REJECTED))
    concluded = settled + failed + rejected
    total_value = sum(received.values(), Decimal("0"))
    return {
        "total": len(received),
        "settled": settled,
        "failed": failed,
        "rejected": rejected,
        "total_value": f"{total_value:.2f}",
        "settlement_success_rate": round(settled / concluded, 4) if concluded else 0.0,
    }


def risk(events: Iterable[RawEvent]) -> dict:
    decisions: dict[str, tuple[str | None, object]] = {}
    for e in _canonical(events):
        if e.event_type == RISK_EVALUATED and e.payment_id:
            decisions.setdefault(
                e.payment_id, (e.payload.get("decision"), e.payload.get("score"))
            )
    counts = {"allow": 0, "review": 0, "block": 0}
    scores: list[float] = []
    for decision, score in decisions.values():
        if decision in counts:
            counts[decision] += 1
        if isinstance(score, (int, float)):
            scores.append(float(score))
    return {
        "total": len(decisions),
        "allow": counts["allow"],
        "review": counts["review"],
        "block": counts["block"],
        "average_score": round(sum(scores) / len(scores), 2) if scores else 0.0,
    }


def providers(events: Iterable[RawEvent]) -> dict:
    stats: dict[str, dict[str, int]] = defaultdict(
        lambda: {"succeeded": 0, "failed": 0, "unknown": 0}
    )
    for e in _canonical(events):
        if e.event_type not in _PROVIDER_EVENTS:
            continue
        provider = e.payload.get("provider") or "unknown"
        if e.event_type == PROVIDER_SUCCEEDED:
            stats[provider]["succeeded"] += 1
        elif e.event_type == PROVIDER_FAILED:
            stats[provider]["failed"] += 1
        else:
            stats[provider]["unknown"] += 1

    out: dict[str, dict] = {}
    for provider, s in stats.items():
        total = s["succeeded"] + s["failed"] + s["unknown"]
        out[provider] = {
            **s,
            "total": total,
            "success_rate": round(s["succeeded"] / total, 4) if total else 0.0,
        }
    return dict(sorted(out.items()))


def account(events: Iterable[RawEvent], account_id: str) -> dict:
    evs = [e for e in _canonical(events) if e.account_id == account_id]
    received = {e.payment_id for e in evs if e.event_type == RECEIVED and e.payment_id}
    settled = {e.payment_id for e in evs if e.event_type == SETTLED and e.payment_id}
    value = sum(
        (_money(e.payload.get("amount")) for e in evs if e.event_type == RECEIVED),
        Decimal("0"),
    )
    return {
        "account_id": account_id,
        "payments": len(received),
        "settled": len(settled),
        "total_value": f"{value:.2f}",
        "events": len(evs),
        "last_activity": evs[-1].occurred_at.isoformat() if evs else None,
    }


def accounts(events: Iterable[RawEvent]) -> dict:
    events = _canonical(events)
    ids = sorted({e.account_id for e in events if e.account_id})
    return {aid: account(events, aid) for aid in ids}


def timeseries(events: Iterable[RawEvent]) -> list[dict]:
    days: dict[str, dict] = defaultdict(
        lambda: {"payments": set(), "settled": set(), "value": Decimal("0")}
    )
    for e in _canonical(events):
        day = days[e.occurred_date]
        if e.event_type == RECEIVED and e.payment_id:
            if e.payment_id not in day["payments"]:
                day["payments"].add(e.payment_id)
                day["value"] += _money(e.payload.get("amount"))
        elif e.event_type == SETTLED and e.payment_id:
            day["settled"].add(e.payment_id)
    return [
        {
            "date": d,
            "payments": len(v["payments"]),
            "settled": len(v["settled"]),
            "value": f"{v['value']:.2f}",
        }
        for d, v in sorted(days.items())
    ]


def overview(events: Iterable[RawEvent]) -> dict:
    events = _canonical(events)
    p = payments(events)
    r = risk(events)
    pr = providers(events)
    transactions = len(
        {e.event_id for e in events if e.event_type in TRANSACTION_TYPES}
    )
    return {
        "payments": p["total"],
        "settled": p["settled"],
        "failed": p["failed"],
        "rejected": p["rejected"],
        "total_value": p["total_value"],
        "settlement_success_rate": p["settlement_success_rate"],
        "risk_allow": r["allow"],
        "risk_review": r["review"],
        "risk_block": r["block"],
        "average_risk_score": r["average_score"],
        "provider_failures": sum(s["failed"] for s in pr.values()),
        "transactions": transactions,
        "events": len(events),
    }


def watermark(events: Iterable[RawEvent]) -> dict:
    """The boundary a projection was built from, so eventual consistency is
    visible and a rebuild can be compared against the exact same raw-event set.
    Deterministic in the raw history: `as_of` is the latest ingest time in the
    canonical set, not wall-clock now(), so a rebuild reproduces it identically."""
    events = _canonical(events)
    if not events:
        return {"raw_event_count": 0, "as_of": None}
    return {
        "raw_event_count": len(events),
        "as_of": max(e.ingested_at for e in events).isoformat(),
    }


def build_projections(events: Iterable[RawEvent]) -> dict:
    """Every materialized projection plus its watermark, derived from the raw
    history in one pass. This is what the store persists and what a rebuild
    recomputes. Because the watermark is a deterministic function of the raw set,
    the whole result is identical across a destroy-and-rebuild of the same
    events."""
    events = _canonical(events)
    return {
        "watermark": watermark(events),
        "overview": overview(events),
        "payments": payments(events),
        "risk": risk(events),
        "providers": providers(events),
        "accounts": accounts(events),
        "timeseries": timeseries(events),
    }
