"""Operational commands for the analytical projections.

These are deliberately not HTTP endpoints: refreshing and rebuilding are
engineering/operational actions, not a public analytical mutation API. In
production they run as a Cloud Run Job against the shared BigQuery store, so a
scheduled `refresh` keeps the read model current off the ingest path, and
`rebuild` is the flagship proof, destroy the derived projections and regenerate
them from `raw_events`.

    python -m app.admin refresh
    python -m app.admin rebuild
    python -m app.admin status

Because the job and the service share the BigQuery store, a refresh here is
visible to the running service; with the in-memory store (local, tests) it acts
on whatever store instance it is given.
"""

from __future__ import annotations

import sys

from app import config
from app.store import AnalyticsStore, get_store


def refresh(store: AnalyticsStore) -> dict:
    """Materialize the projections from the current raw history."""
    store.refresh()
    return store.watermark()


def rebuild(store: AnalyticsStore) -> dict:
    """Destroy the materialized projections and regenerate them from raw_events."""
    store.rebuild()
    return store.watermark()


def status(store: AnalyticsStore) -> dict:
    """Report the raw history size and the current projection watermark."""
    return {"raw_count": store.raw_count(), "watermark": store.watermark()}


_COMMANDS = {"refresh": refresh, "rebuild": rebuild, "status": status}


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    if not argv or argv[0] not in _COMMANDS:
        print(f"usage: python -m app.admin {{{'|'.join(_COMMANDS)}}}", file=sys.stderr)
        return 2
    store = get_store(config.STORE_BACKEND)
    if hasattr(store, "ensure_tables"):
        store.ensure_tables()
    result = _COMMANDS[argv[0]](store)
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
