"""Projections are deterministic, order-independent, and robust to duplicated
raw input. The known scenario has fixed aggregate outcomes.
"""

import random

from app import projections
from tests.factories import ev, known_scenario


def test_overview_matches_the_known_scenario():
    o = projections.overview(known_scenario())
    assert o["payments"] == 3
    assert o["settled"] == 1
    assert o["failed"] == 1
    assert o["rejected"] == 1
    assert o["total_value"] == "600.00"
    assert o["settlement_success_rate"] == round(1 / 3, 4)
    assert o["risk_allow"] == 1
    assert o["risk_review"] == 1
    assert o["risk_block"] == 1
    assert o["average_risk_score"] == round((10 + 55 + 80) / 3, 2)
    assert o["provider_failures"] == 1
    assert o["transactions"] == 1


def test_projections_are_order_independent():
    events = known_scenario()
    shuffled = events[:]
    random.Random(1).shuffle(shuffled)
    assert projections.build_projections(events) == projections.build_projections(shuffled)


def test_a_duplicated_raw_event_contributes_once():
    events = known_scenario()
    settled = next(e for e in events if e.event_type == "payment.settled")
    # Same payment id, but a projection must not count it twice even if the same
    # settled fact appears again in the raw input.
    with_dup = events + [settled]
    assert projections.payments(events)["settled"] == projections.payments(with_dup)["settled"]


def test_providers_success_rate():
    pr = projections.providers(known_scenario())
    assert pr["NorthPay"]["succeeded"] == 1
    assert pr["NorthPay"]["failed"] == 1
    assert pr["NorthPay"]["success_rate"] == 0.5


def test_risk_average_score():
    r = projections.risk(known_scenario())
    assert r["total"] == 3
    assert r["average_score"] == round((10 + 55 + 80) / 3, 2)


def test_timeseries_buckets_by_day():
    ts = projections.timeseries(known_scenario())
    assert len(ts) == 1
    day = ts[0]
    assert day["date"] == "2026-09-01"
    assert day["payments"] == 3
    assert day["settled"] == 1
    assert day["value"] == "600.00"


def test_account_activity_is_scoped_to_the_account():
    events = known_scenario()
    account_id = next(e.account_id for e in events if e.event_type == "payment.settled")
    a = projections.account(events, account_id)
    assert a["account_id"] == account_id
    assert a["payments"] == 1
    assert a["settled"] == 1
    assert a["total_value"] == "100.00"


def test_empty_history_is_all_zeros():
    o = projections.overview([])
    assert o["payments"] == 0
    assert o["total_value"] == "0.00"
    assert o["settlement_success_rate"] == 0.0


def test_amount_is_summed_once_per_payment_even_if_received_repeats():
    p = "11111111-1111-1111-1111-111111111111"
    e1 = ev("payment.received", {"payment_id": p, "account_id": "a", "amount": "100.00"})
    # A second received with the same payment id must not double the value.
    e2 = ev("payment.received", {"payment_id": p, "account_id": "a", "amount": "100.00"})
    assert projections.payments([e1, e2])["total_value"] == "100.00"
    assert projections.payments([e1, e2])["total"] == 1


def test_duplicate_event_id_is_canonicalized_away():
    # The same physical event row appearing twice (identical event_id) must not
    # double-count in any projection: dedup is at the input boundary, by event_id.
    e = ev("risk.evaluated", {"payment_id": "p", "account_id": "a", "decision": "allow", "score": 10}, event_id="dup-1")
    assert projections.risk([e, e, e])["allow"] == 1
    assert projections.risk([e, e, e])["total"] == 1


def test_watermark_reflects_the_canonical_raw_set():
    events = known_scenario()
    wm = projections.watermark(events)
    distinct = len({e.event_id for e in events})
    assert wm["raw_event_count"] == distinct
    assert wm["as_of"] is not None
    # Deterministic in the raw set: same events in, same watermark out.
    assert projections.watermark(events) == wm


def test_watermark_of_empty_history():
    assert projections.watermark([]) == {"raw_event_count": 0, "as_of": None}
