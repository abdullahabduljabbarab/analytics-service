"""Smoke test for the BigQuery adapter against a real, temporary dataset.

Skipped by default. Run it during deployment or live verification, with
Application Default Credentials that can create and drop a dataset:

    RUN_BQ_SMOKE=1 pytest tests/test_bigquery_smoke.py

It exercises the parts of the store contract that only BigQuery can prove:
schema creation, MERGE-based idempotent ingestion, a streaming refresh, and a
destroy-and-rebuild producing the same result. The full behavioural contract
runs against the in-memory store in ordinary CI; this confirms the BigQuery
adapter honours the same contract without needing BigQuery on every push.
"""

import os
import time
import uuid

import pytest

from tests.factories import known_scenario

# Streaming inserts are queryable within a few seconds, not instantly; give the
# buffer time before asserting on counts or refreshing.
_BUFFER_WAIT = 20

RUN = os.getenv("RUN_BQ_SMOKE") == "1"

pytestmark = pytest.mark.skipif(
    not RUN, reason="set RUN_BQ_SMOKE=1 to run against a temporary BigQuery dataset"
)


def _temp_store():
    from app.bigquery_store import BigQueryStore

    store = BigQueryStore(dataset=f"analytics_smoke_{uuid.uuid4().hex[:8]}")
    store.ensure_tables()
    return store


def _drop(store):
    store._client.delete_dataset(
        f"{store._project}.{store._dataset}", delete_contents=True, not_found_ok=True
    )


def test_bigquery_adapter_honours_the_store_contract():
    store = _temp_store()
    try:
        events = known_scenario()

        # First ingestion appends every event.
        for e in events:
            store.record_event(e)
        time.sleep(_BUFFER_WAIT)
        assert store.raw_count() == len(events)

        # Idempotency on read: redelivering the whole set adds physical rows but
        # not logical ones (dedup by event_id keeps the distinct count stable).
        for e in events:
            store.record_event(e)
        time.sleep(_BUFFER_WAIT)
        assert store.raw_count() == len(events)

        # Streaming refresh materializes the projections from the deduplicated
        # raw history.
        store.refresh()
        overview = store.projection("overview")
        assert overview["payments"] == 3
        assert overview["settled"] == 1
        assert overview["total_value"] == "600.00"
        assert store.watermark()["raw_event_count"] == len(events)

        before = store.projection("overview")

        # Reset removes derived state only; the raw history is untouched.
        store.reset()
        assert store.projection("overview") == {}
        assert store.raw_count() == len(events)

        # Rebuild from raw_events reproduces the original projections exactly.
        store.rebuild()
        assert store.projection("overview") == before
    finally:
        _drop(store)
