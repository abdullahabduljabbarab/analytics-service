"""The ABS event envelope, and the normalized raw event analytics retains.

Every service in the ecosystem publishes events in one envelope shape. Analytics
consumes them from three topics and keeps each as a `RawEvent` in its durable
history (`raw_events`). That history is canonical *within analytics*: it is the
durable analytical copy the projections are rebuilt from. It is not the
ecosystem's authoritative event store, the upstream services remain authoritative
for the facts they committed; analytics never writes back to them.

The envelope carries a `payload` whose shape depends on the event type.
`payment_id` and `account_id` are lifted into nullable convenience columns where
the event contract defines an unambiguous single value (payment and risk events),
so projections can see them uniformly. They are only conveniences: the payload
stays authoritative for analytical interpretation, and events whose contract has
no single such value leave them null. Transaction events, for instance, carry no
account field, a double-entry transaction concerns more than one account, so
per-account analytics derives from payment events, and transactions contribute at
the platform level only. The full payload is always retained for anything else.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pydantic import BaseModel, Field


class AbsEvent(BaseModel):
    """The wire envelope as published by an ABS service."""

    event_id: str
    event_type: str
    event_version: int = 1
    occurred_at: datetime | None = None
    producer: str | None = None
    correlation_id: str | None = None
    causation_id: str | None = None
    aggregate_id: str | None = None
    payload: dict = Field(default_factory=dict)


@dataclass(frozen=True)
class RawEvent:
    """A validated event as analytics retains it. Immutable: once ingested, a
    raw event is a fact in the analytical history and is never mutated, only
    read by projections."""

    event_id: str
    event_type: str
    event_version: int
    occurred_at: datetime
    producer: str | None
    correlation_id: str | None
    causation_id: str | None
    aggregate_id: str | None
    account_id: str | None
    payment_id: str | None
    payload: dict
    ingested_at: datetime

    @property
    def occurred_date(self) -> str:
        """The UTC calendar date the event occurred, for daily rollups."""
        return self.occurred_at.astimezone(timezone.utc).date().isoformat()


def _parse_dt(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def to_raw_event(event: AbsEvent, ingested_at: datetime | None = None) -> RawEvent:
    """Normalize a validated envelope into the retained raw event, lifting the
    account and payment ids out of the payload. `occurred_at` falls back to
    ingest time only if the producer omitted it, so daily rollups always have a
    date to bucket on."""
    payload = event.payload or {}
    now = datetime.now(tz=timezone.utc)
    occurred = event.occurred_at or now
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=timezone.utc)
    return RawEvent(
        event_id=event.event_id,
        event_type=event.event_type,
        event_version=event.event_version,
        occurred_at=occurred,
        producer=event.producer,
        correlation_id=event.correlation_id,
        causation_id=event.causation_id,
        aggregate_id=event.aggregate_id,
        account_id=payload.get("account_id"),
        payment_id=payload.get("payment_id") or event.aggregate_id,
        payload=payload,
        ingested_at=ingested_at or now,
    )
