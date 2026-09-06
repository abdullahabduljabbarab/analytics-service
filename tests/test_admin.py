"""The operational refresh / rebuild / status commands over a store."""

from app import admin
from app.store import InMemoryStore
from tests.factories import known_scenario


def _loaded_store():
    store = InMemoryStore()
    for e in known_scenario():
        store.record_event(e)
    return store


def test_refresh_materializes_projections():
    store = _loaded_store()
    wm = admin.refresh(store)
    assert wm["raw_event_count"] == len(known_scenario())
    assert store.projection("overview")["payments"] == 3


def test_rebuild_reproduces_the_same_result():
    store = _loaded_store()
    admin.refresh(store)
    before = store.projection("overview")
    result = admin.rebuild(store)
    assert store.projection("overview") == before
    assert result["raw_event_count"] == len(known_scenario())


def test_status_reports_raw_count_and_watermark():
    store = _loaded_store()
    admin.refresh(store)
    s = admin.status(store)
    assert s["raw_count"] == len(known_scenario())
    assert s["watermark"]["raw_event_count"] == len(known_scenario())


def test_status_before_any_refresh():
    store = _loaded_store()
    s = admin.status(store)
    assert s["raw_count"] == len(known_scenario())
    # Nothing materialized yet.
    assert s["watermark"] == {"raw_event_count": 0, "as_of": None}
