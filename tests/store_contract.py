"""The store contract: one behavioural suite every AnalyticsStore backend must
satisfy. The in-memory store runs it in full in CI; the BigQuery adapter runs a
smoke subset of it against a temporary dataset during live verification.

A backend is correct if ingestion is idempotent on event_id, projections are
disposable derived state, and the read model is deterministically rebuildable
from the retained raw history. Subclass with a `make_store` that returns a fresh,
empty store.
"""

from tests.factories import ev, known_scenario


def _load(store, events):
    for e in events:
        store.record_event(e)


class StoreContract:
    def make_store(self):
        raise NotImplementedError

    def test_first_event_is_accepted(self):
        store = self.make_store()
        assert store.record_event(ev("payment.received", {"payment_id": "p", "amount": "10.00"})) is True
        assert store.raw_count() == 1

    def test_same_event_id_twice_contributes_once(self):
        store = self.make_store()
        e = ev("payment.received", {"payment_id": "p", "account_id": "a", "amount": "10.00"})
        assert store.record_event(e) is True
        assert store.record_event(e) is False
        assert store.raw_count() == 1
        store.refresh()
        assert store.projection("overview")["payments"] == 1

    def test_different_event_ids_contribute_independently(self):
        store = self.make_store()
        store.record_event(ev("payment.received", {"payment_id": "p1", "amount": "10.00"}))
        store.record_event(ev("payment.received", {"payment_id": "p2", "amount": "20.00"}))
        assert store.raw_count() == 2
        store.refresh()
        assert store.projection("overview")["payments"] == 2
        assert store.projection("overview")["total_value"] == "30.00"

    def test_correlations_survive_persistence(self):
        store = self.make_store()
        e = ev("payment.settled", {"payment_id": "p", "account_id": "a"}, correlation_id="corr-123")
        store.record_event(e)
        stored = {r.event_id: r for r in store.raw_events()}[e.event_id]
        assert stored.correlation_id == "corr-123"

    def test_reset_removes_derived_state_only(self):
        store = self.make_store()
        events = known_scenario()
        _load(store, events)
        store.refresh()
        assert store.projection("overview")["payments"] == 3

        store.reset()
        assert store.raw_count() == len(events)  # raw history untouched
        assert store.projection("overview") == {}  # derived state gone

    def test_rebuild_reproduces_the_original_result(self):
        store = self.make_store()
        _load(store, known_scenario())
        store.refresh()
        before = store.projection("overview")

        store.reset()
        store.rebuild()
        assert store.projection("overview") == before

    def test_replaying_raw_history_twice_is_identical(self):
        events = known_scenario()

        once = self.make_store()
        _load(once, events)
        once.refresh()

        twice = self.make_store()
        _load(twice, events)
        _load(twice, events)  # every event redelivered
        twice.refresh()

        assert twice.raw_count() == once.raw_count()
        assert twice.projection("overview") == once.projection("overview")
