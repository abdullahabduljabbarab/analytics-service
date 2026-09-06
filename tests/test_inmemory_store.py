"""The in-memory store must satisfy the full store contract, plus the flagship
rebuild proof over every projection, not just the overview.
"""

from app.store import InMemoryStore
from tests.factories import known_scenario
from tests.store_contract import StoreContract


class TestInMemoryStore(StoreContract):
    def make_store(self):
        return InMemoryStore()


def test_ingestion_does_not_touch_projections():
    # Recording raw events leaves projections cold until an explicit refresh:
    # ingestion and projection are separate concerns.
    store = InMemoryStore()
    for e in known_scenario():
        store.record_event(e)
    assert store.raw_count() == len(known_scenario())
    assert store.projection("overview") == {}  # nothing derived yet


def test_full_rebuild_regenerates_every_projection_identically():
    store = InMemoryStore()
    for e in known_scenario():
        store.record_event(e)
    store.refresh()

    names = ("watermark", "overview", "payments", "risk", "providers", "accounts", "timeseries")
    before = {name: store.projection(name) for name in names}

    # Destroy the derived analytical state entirely, then regenerate it from the
    # retained raw history.
    store.reset()
    assert store.projection("overview") == {}
    assert store.watermark() == {"raw_event_count": 0, "as_of": None}
    store.rebuild()

    after = {name: store.projection(name) for name in names}
    # Same raw-event set in, identical projections and watermark out.
    assert after == before
    # The raw history was never touched by any of this.
    assert store.raw_count() == len(known_scenario())
