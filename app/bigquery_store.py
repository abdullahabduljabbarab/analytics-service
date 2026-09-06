"""The BigQuery analytics store: the production `AnalyticsStore` backend.

Two tables in the analytics dataset. `raw_events` is the durable analytical
history, one logical row per `event_id`; ingestion upholds that with a `MERGE`,
not a check-then-insert, so at-least-once redelivery cannot race. `projections`
holds the materialized read model, one row per projection name with its JSON.

A refresh streams `raw_events` in canonical order through the single-pass
`StreamingProjectionBuilder`, so a rebuild never loads the whole history into
memory, and writes the results with DML (strongly consistent, so a read right
after a refresh sees the new values, unlike a streaming insert's buffer).

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
        sql = f"""
        MERGE {self._table(RAW_TABLE)} T
        USING (SELECT @event_id AS event_id) S
        ON T.event_id = S.event_id
        WHEN NOT MATCHED THEN
          INSERT ({", ".join(_RAW_COLUMNS)})
          VALUES (@event_id, @event_type, @event_version, @occurred_at, @producer,
                  @correlation_id, @causation_id, @aggregate_id, @account_id,
                  @payment_id, @payload, @ingested_at)
        """
        params = [
            bigquery.ScalarQueryParameter("event_id", "STRING", event.event_id),
            bigquery.ScalarQueryParameter("event_type", "STRING", event.event_type),
            bigquery.ScalarQueryParameter("event_version", "INT64", event.event_version),
            bigquery.ScalarQueryParameter("occurred_at", "TIMESTAMP", event.occurred_at),
            bigquery.ScalarQueryParameter("producer", "STRING", event.producer),
            bigquery.ScalarQueryParameter("correlation_id", "STRING", event.correlation_id),
            bigquery.ScalarQueryParameter("causation_id", "STRING", event.causation_id),
            bigquery.ScalarQueryParameter("aggregate_id", "STRING", event.aggregate_id),
            bigquery.ScalarQueryParameter("account_id", "STRING", event.account_id),
            bigquery.ScalarQueryParameter("payment_id", "STRING", event.payment_id),
            bigquery.ScalarQueryParameter("payload", "STRING", json.dumps(event.payload)),
            bigquery.ScalarQueryParameter("ingested_at", "TIMESTAMP", event.ingested_at),
        ]
        job_config = bigquery.QueryJobConfig(query_parameters=params)
        job = self._client.query(sql, job_config=job_config)
        job.result()
        return job.num_dml_affected_rows == 1

    # --- raw history -----------------------------------------------------

    def raw_count(self) -> int:
        rows = self._query(f"SELECT COUNT(*) AS c FROM {self._table(RAW_TABLE)}")
        return next(iter(rows)).c

    def _iter_raw(self) -> Iterator[RawEvent]:
        sql = f"SELECT * FROM {self._table(RAW_TABLE)} ORDER BY occurred_at, event_id"
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
