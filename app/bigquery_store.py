"""The BigQuery analytics store: the production `AnalyticsStore` backend.

Two tables in the analytics dataset. `raw_events` is the durable analytical
history. Ingestion is an append-only **streaming insert**, not per-event DML:
BigQuery caps concurrent DML at ~20 statements per table, so a MERGE per pushed
event collapses under load, whereas streaming inserts are the high-throughput
ingestion path. "One row per `event_id`" is therefore a *logical* invariant
enforced on read, not on write, the refresh reads `raw_events` deduplicated by
`event_id`, and `raw_count` counts distinct `event_id`. This is the standard
append-then-dedup-on-read analytics pattern, and it matches what the projections
already do (canonicalize by event_id). `projections` holds the materialized read
model, one row per projection name with its JSON.

A refresh streams the deduplicated `raw_events` in canonical order through the
single-pass `StreamingProjectionBuilder`, so a rebuild never loads the whole
history into memory, and writes the results with DML (low volume, strongly
consistent, so a read right after a refresh sees the new values). Because
ingestion is a streaming insert, a just-ingested event is queryable within a few
seconds, not instantly, which is exactly the eventual consistency the watermark
makes visible.

This module imports the BigQuery client and is only loaded when the store backend
is "bigquery"; the deterministic core and the in-memory store never touch it, so
CI needs no cloud. It satisfies the same store contract as the in-memory store,
smoke-verified against a temporary dataset during deployment.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from datetime import datetime, timezone

from google.cloud import bigquery

from app import config, projections
from app.envelope import RawEvent
from app.store import _EMPTY

RAW_TABLE = "raw_events"
PROJECTIONS_TABLE = "projections"

_RAW_SCHEMA = [
    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("event_version", "INT64"),
    bigquery.SchemaField("occurred_at", "TIMESTAMP", mode="REQUIRED"),
    bigquery.SchemaField("producer", "STRING"),
    bigquery.SchemaField("correlation_id", "STRING"),
    bigquery.SchemaField("causation_id", "STRING"),
    bigquery.SchemaField("aggregate_id", "STRING"),
    bigquery.SchemaField("account_id", "STRING"),
    bigquery.SchemaField("payment_id", "STRING"),
    bigquery.SchemaField("payload", "STRING"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

_PROJECTIONS_SCHEMA = [
    bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("data", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("raw_event_count", "INT64"),
    bigquery.SchemaField("as_of", "TIMESTAMP"),
    bigquery.SchemaField("refreshed_at", "TIMESTAMP"),
]

_RAW_COLUMNS = [f.name for f in _RAW_SCHEMA]


class BigQueryStore:
    def __init__(
        self,
        project: str | None = None,
        dataset: str | None = None,
        location: str | None = None,
    ) -> None:
        self._project = project or config.BIGQUERY_PROJECT
        self._dataset = dataset or config.BIGQUERY_DATASET
        self._location = location or config.BIGQUERY_LOCATION
        self._client = bigquery.Client(project=self._project, location=self._location)
        self.refreshed_at: datetime | None = None

    # --- table names -----------------------------------------------------

    def _table_id(self, name: str) -> str:
        return f"{self._project}.{self._dataset}.{name}"

    def _table(self, name: str) -> str:
        return f"`{self._table_id(name)}`"

    def _query(self, sql: str, params: list | None = None):
        job_config = bigquery.QueryJobConfig(query_parameters=params or [])
        return self._client.query(sql, job_config=job_config).result()

    # --- schema as code --------------------------------------------------

    def ensure_tables(self) -> None:
        """Create the dataset and tables if they do not exist. Idempotent, run at
        startup and by the smoke test."""
        dataset = bigquery.Dataset(f"{self._project}.{self._dataset}")
        dataset.location = self._location
        self._client.create_dataset(dataset, exists_ok=True)
        self._client.create_table(
            bigquery.Table(self._table_id(RAW_TABLE), schema=_RAW_SCHEMA), exists_ok=True
        )
        self._client.create_table(
            bigquery.Table(self._table_id(PROJECTIONS_TABLE), schema=_PROJECTIONS_SCHEMA),
            exists_ok=True,
        )

    # --- ingestion -------------------------------------------------------

    def record_event(self, event: RawEvent) -> bool:
        # Append-only streaming insert: no DML, so no per-table concurrent-DML
        # limit under load. Duplicates are tolerated here and removed on read
        # (the event_id logical invariant is enforced by the dedup in _iter_raw).
        row = {
            "event_id": event.event_id,
            "event_type": event.event_type,
            "event_version": event.event_version,
            "occurred_at": event.occurred_at.isoformat(),
            "producer": event.producer,
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id,
            "aggregate_id": event.aggregate_id,
            "account_id": event.account_id,
            "payment_id": event.payment_id,
            "payload": json.dumps(event.payload),
            "ingested_at": event.ingested_at.isoformat(),
        }
        errors = self._client.insert_rows_json(self._table_id(RAW_TABLE), [row])
        if errors:
            raise RuntimeError(f"raw_events streaming insert failed: {errors}")
        return True

    # --- raw history -----------------------------------------------------

    def raw_count(self) -> int:
        # Distinct event_id: the physical table may hold duplicate rows from
        # at-least-once delivery, but the logical history is one per event_id.
        rows = self._query(
            f"SELECT COUNT(DISTINCT event_id) AS c FROM {self._table(RAW_TABLE)}"
        )
        return next(iter(rows)).c

    def _iter_raw(self) -> Iterator[RawEvent]:
        # Deduplicate by event_id on read, keeping the earliest ingest, then
        # order canonically. This is where the one-per-event_id invariant is
        # enforced, so ingestion can stay a cheap append.
        sql = f"""
        SELECT * EXCEPT(_rn) FROM (
          SELECT *, ROW_NUMBER() OVER (
            PARTITION BY event_id ORDER BY ingested_at
          ) AS _rn
          FROM {self._table(RAW_TABLE)}
        )
        WHERE _rn = 1
        ORDER BY occurred_at, event_id
        """
        for row in self._client.query(sql).result(page_size=1000):
            yield self._row_to_event(row)

    def raw_events(self) -> list[RawEvent]:
        return list(self._iter_raw())

    @staticmethod
    def _row_to_event(row) -> RawEvent:
        occurred = row.occurred_at
        ingested = row.ingested_at
        return RawEvent(
            event_id=row.event_id,
            event_type=row.event_type,
            event_version=row.event_version or 1,
            occurred_at=occurred if occurred.tzinfo else occurred.replace(tzinfo=timezone.utc),
            producer=row.producer,
            correlation_id=row.correlation_id,
            causation_id=row.causation_id,
            aggregate_id=row.aggregate_id,
            account_id=row.account_id,
            payment_id=row.payment_id,
            payload=json.loads(row.payload) if row.payload else {},
            ingested_at=ingested if ingested.tzinfo else ingested.replace(tzinfo=timezone.utc),
        )

    # --- projections -----------------------------------------------------

    def refresh(self) -> None:
        builder = projections.StreamingProjectionBuilder()
        for event in self._iter_raw():
            builder.add(event)
        self._write_projections(builder.result())
        self.refreshed_at = datetime.now(tz=timezone.utc)

    def _write_projections(self, result: dict) -> None:
        self.reset()
        watermark = result["watermark"]
        refreshed = datetime.now(tz=timezone.utc)
        for name, data in result.items():
            sql = f"""
            INSERT INTO {self._table(PROJECTIONS_TABLE)}
              (name, data, raw_event_count, as_of, refreshed_at)
            VALUES (@name, @data, @raw_event_count, @as_of, @refreshed_at)
            """
            as_of = watermark["as_of"]
            params = [
                bigquery.ScalarQueryParameter("name", "STRING", name),
                bigquery.ScalarQueryParameter("data", "STRING", json.dumps(data)),
                bigquery.ScalarQueryParameter("raw_event_count", "INT64", watermark["raw_event_count"]),
                bigquery.ScalarQueryParameter("as_of", "TIMESTAMP", as_of),
                bigquery.ScalarQueryParameter("refreshed_at", "TIMESTAMP", refreshed),
            ]
            self._query(sql, params)

    def reset(self) -> None:
        self._query(f"DELETE FROM {self._table(PROJECTIONS_TABLE)} WHERE TRUE")
        self.refreshed_at = None

    def rebuild(self) -> None:
        self.reset()
        self.refresh()

    def projection(self, name: str) -> object:
        sql = f"SELECT data FROM {self._table(PROJECTIONS_TABLE)} WHERE name = @name"
        params = [bigquery.ScalarQueryParameter("name", "STRING", name)]
        rows = list(self._query(sql, params))
        if not rows:
            return _EMPTY.get(name, {})
        return json.loads(rows[0].data)

    def watermark(self) -> dict:
        value = self.projection("watermark")
        return value if isinstance(value, dict) and value else dict(_EMPTY["watermark"])

    def account(self, account_id: str) -> dict:
        accounts = self.projection("accounts")
        if isinstance(accounts, dict) and account_id in accounts:
            return accounts[account_id]
        return projections.account([], account_id)
