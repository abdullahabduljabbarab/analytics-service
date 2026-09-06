"""The analytics store: a durable raw event history plus disposable projections.

The store draws a hard line between two concerns, which is the heart of the
design:

- **Ingestion** persists a raw event idempotently, keyed on `event_id`. This is
  the only durable write, and it is what the consumer acks on. Recording the same
  event again is a no-op.
- **Projection** is disposable derived state, materialized from the raw history
  by `refresh` and thrown away and recomputed by `rebuild`. A projection can be
  stale, wrong, or absent without any effect on the raw history or on the
  financial platform; a rebuild repairs it exactly.

Any backend (the in-memory store here, the BigQuery adapter in production) must
satisfy the same contract, verified by one shared behavioural suite.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Protocol, runtime_checkable

from app import projections
from app.envelope import RawEvent

# The materialized projections a store holds, and the empty value each reads as
# before any refresh, so a query never fails just because projections are cold.
_EMPTY: dict[str, object] = {
    "watermark": {"raw_event_count": 0, "as_of": None},
    "overview": {},
    "payments": {},
    "risk": {},
    "providers": {},
    "accounts": {},
    "timeseries": [],
}


@runtime_checkable
class AnalyticsStore(Protocol):
    def record_event(self, event: RawEvent) -> bool:
        """Persist a raw event idempotently. Returns True if it was newly
        recorded, False if this event_id was already present."""
        ...

    def raw_count(self) -> int:
        """How many distinct raw events are retained."""
        ...

    def raw_events(self) -> list[RawEvent]:
        """The retained raw history, the source of every projection."""
        ...

    def refresh(self) -> None:
        """Recompute the materialized projections from the raw history."""
        ...

    def reset(self) -> None:
        """Drop the materialized projections. The raw history is untouched."""
        ...

    def rebuild(self) -> None:
        """Reset then refresh: regenerate every projection from raw history."""
        ...

    def projection(self, name: str) -> object:
        """Read one materialized projection by name."""
        ...

    def watermark(self) -> dict:
        """The raw-event boundary the current projections were built from
        (`raw_event_count`, `as_of`), so eventual consistency is visible."""
        ...

    def account(self, account_id: str) -> dict:
        """Read one account's materialized activity."""
        ...


class InMemoryStore:
    """Reference store for local runs and the full test suite. Holds the raw
    history in a dict keyed on event_id (so ingestion is idempotent) and the
    materialized projections separately (so they are plainly disposable)."""

    def __init__(self) -> None:
        self._raw: dict[str, RawEvent] = {}
        self._projections: dict[str, object] = dict(_EMPTY)
        # Wall-clock time of the last refresh. Deliberately NOT part of the
        # projection content (which must be deterministic for the rebuild proof);
        # it is operational metadata the API can surface as freshness/lag.
        self.refreshed_at: datetime | None = None

    def record_event(self, event: RawEvent) -> bool:
        if event.event_id in self._raw:
            return False
        self._raw[event.event_id] = event
        return True

    def raw_count(self) -> int:
        return len(self._raw)

    def raw_events(self) -> list[RawEvent]:
        return list(self._raw.values())

    def refresh(self) -> None:
        self._projections = projections.build_projections(self._raw.values())
        self.refreshed_at = datetime.now(tz=timezone.utc)

    def reset(self) -> None:
        self._projections = dict(_EMPTY)
        self.refreshed_at = None

    def rebuild(self) -> None:
        self.reset()
        self.refresh()

    def projection(self, name: str) -> object:
        return self._projections.get(name, _EMPTY.get(name, {}))

    def watermark(self) -> dict:
        return self._projections.get("watermark", dict(_EMPTY["watermark"]))

    def account(self, account_id: str) -> dict:
        accounts = self._projections.get("accounts", {})
        if isinstance(accounts, dict) and account_id in accounts:
            return accounts[account_id]
        return projections.account([], account_id)


def get_store(backend: str = "memory") -> AnalyticsStore:
    """Select a store backend. The BigQuery adapter arrives in Milestone 2 and
    satisfies the same contract."""
    if backend == "memory":
        return InMemoryStore()
    if backend == "bigquery":
        from app.bigquery_store import BigQueryStore

        return BigQueryStore()
    raise ValueError(f"unknown store backend: {backend}")
