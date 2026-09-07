import base64
import json
import logging

from fastapi import Depends, FastAPI, Header, HTTPException
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as google_id_token
from pydantic import ValidationError

from app import config
from app.envelope import AbsEvent, to_raw_event
from app.schemas import PubSubPush
from app.store import AnalyticsStore, get_store

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("analytics.api")

DESCRIPTION = """
Downstream analytics for the ABS platform.

The service consumes committed events from the ledger, the orchestrator and the
risk engine, keeps a durable analytical copy in **BigQuery `raw_events`**, and
serves read-optimized projections derived from it. It owns derived analytical
truth, never financial truth, emits no events, and can be stopped or hours behind
while payments settle normally (extends ABS-REQ-006).

**Correctness properties**

- Ingestion is idempotent on **event_id**; at-least-once redelivery never
  double-counts
- Projections are a deterministic function of the raw history and are
  **rebuildable**: destroy them and regenerate from `raw_events`, identical result
- Ingestion is separate from projection: the consumer persists the raw event and
  acks; projections are refreshed off that path
- Every projection carries a **watermark** (`raw_event_count`, `as_of`), so its
  eventual consistency is visible

Analytical answers are not authoritative current balances; those belong to the
ledger.
"""

TAGS = [
    {"name": "System", "description": "Health and service status."},
    {"name": "Analytics", "description": "Read-optimized aggregate projections."},
    {"name": "Event Delivery", "description": "Authenticated Pub/Sub push consumer."},
]

app = FastAPI(
    title="Analytics Service",
    version="0.1.0",
    description=DESCRIPTION,
    openapi_tags=TAGS,
    license_info={"name": "MIT", "url": "https://opensource.org/licenses/MIT"},
)

# Process-level store. In production this is the BigQuery adapter, shared durable
# state that a separate refresh job also writes to; in tests the dependency is
# overridden with an in-memory store.
_store = get_store(config.STORE_BACKEND)


def get_analytics_store() -> AnalyticsStore:
    return _store


@app.get("/health", tags=["System"], summary="Liveness probe")
def health():
    return {"status": "ok", "backend": config.STORE_BACKEND}


def _with_watermark(store: AnalyticsStore, name: str) -> dict:
    return {"watermark": store.watermark(), name: store.projection(name)}


@app.get("/analytics/overview", tags=["Analytics"], summary="Headline platform metrics")
def analytics_overview(store: AnalyticsStore = Depends(get_analytics_store)):
    return _with_watermark(store, "overview")


@app.get("/analytics/payments", tags=["Analytics"], summary="Payment volumes and outcomes")
def analytics_payments(store: AnalyticsStore = Depends(get_analytics_store)):
    return _with_watermark(store, "payments")


@app.get("/analytics/risk", tags=["Analytics"], summary="Risk decision distribution")
def analytics_risk(store: AnalyticsStore = Depends(get_analytics_store)):
    return _with_watermark(store, "risk")


@app.get("/analytics/providers", tags=["Analytics"], summary="Per-provider performance")
def analytics_providers(store: AnalyticsStore = Depends(get_analytics_store)):
    return _with_watermark(store, "providers")


@app.get("/analytics/timeseries", tags=["Analytics"], summary="Daily platform metrics")
def analytics_timeseries(store: AnalyticsStore = Depends(get_analytics_store)):
    return _with_watermark(store, "timeseries")


@app.get(
    "/analytics/accounts/{account_id}",
    tags=["Analytics"],
    summary="Activity for one account",
)
def analytics_account(
    account_id: str, store: AnalyticsStore = Depends(get_analytics_store)
):
    return {"watermark": store.watermark(), "account": store.account(account_id)}


@app.get(
    "/analytics/events",
    tags=["Analytics"],
    summary="Trace-safe event metadata for one correlation id",
)
def analytics_events(
    correlation_id: str, store: AnalyticsStore = Depends(get_analytics_store)
):
    """Every event analytics ingested under one correlation id, as trace metadata
    (identity, type, timing, lineage) with no payload. This closes the cross-service
    trace, letting a correlation id be followed into analytics without exposing its
    contents, and backs the portal's payment trace. It reads raw history directly, so
    it does not wait on a projection refresh."""
    return {
        "correlation_id": correlation_id,
        "events": store.events_by_correlation(correlation_id),
    }


_google_request = google_requests.Request()


def _verify_push_identity(authorization: str | None) -> None:
    """Require a Google OIDC token minted for the configured push service
    account, so only an authenticated Pub/Sub push can feed the raw history.
    Skipped when PUBSUB_PUSH_SA is unset (local and tests)."""
    if not config.PUBSUB_PUSH_SA:
        return
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing push authentication")
    token = authorization.split(" ", 1)[1]
    try:
        claims = google_id_token.verify_oauth2_token(token, _google_request)
    except Exception as e:
        raise HTTPException(status_code=403, detail="invalid push token") from e
    if not claims.get("email_verified") or claims.get("email") != config.PUBSUB_PUSH_SA:
        raise HTTPException(status_code=403, detail="unauthorized push identity")


_ENVELOPE_ATTRS = (
    "event_version",
    "occurred_at",
    "producer",
    "correlation_id",
    "causation_id",
    "aggregate_id",
)


def _envelope_from(decoded: object, attributes: dict) -> dict | None:
    """The ecosystem emits events in two wire shapes, and analytics consumes
    both. The orchestrator and risk engine put the full ABS envelope in the
    message data; the ledger puts the payload in the data and carries event_id
    and event_type in Pub/Sub message attributes. Normalize either into one
    envelope."""
    if isinstance(decoded, dict) and "event_id" in decoded and "event_type" in decoded:
        return decoded
    if "event_id" in attributes and "event_type" in attributes:
        envelope = {
            "event_id": attributes["event_id"],
            "event_type": attributes["event_type"],
            "payload": decoded if isinstance(decoded, dict) else {},
        }
        for key in _ENVELOPE_ATTRS:
            if attributes.get(key) is not None:
                envelope[key] = attributes[key]
        return envelope
    return None


@app.post(
    "/events/pubsub",
    tags=["Event Delivery"],
    summary="Authenticated Pub/Sub push consumer",
)
def pubsub_push(
    body: PubSubPush,
    authorization: str | None = Header(default=None),
    store: AnalyticsStore = Depends(get_analytics_store),
):
    _verify_push_identity(authorization)
    message = body.message
    data = message.get("data")
    attributes = message.get("attributes") or {}
    if not data and not attributes:
        raise HTTPException(status_code=400, detail="missing message data")

    decoded: object = {}
    if data:
        try:
            decoded = json.loads(base64.b64decode(data).decode("utf-8"))
        except Exception as e:
            raise HTTPException(status_code=400, detail="invalid message data") from e

    envelope = _envelope_from(decoded, attributes)
    if envelope is None:
        raise HTTPException(status_code=400, detail="invalid envelope")
    try:
        event = AbsEvent(**envelope)
    except ValidationError as e:
        raise HTTPException(status_code=400, detail="invalid envelope") from e

    # The durable, acked action is persisting the raw event. Projections are
    # refreshed off this path (ADR-003), so the consumer never rescans history.
    created = store.record_event(to_raw_event(event))
    return {"status": "recorded" if created else "duplicate", "event_id": event.event_id}
