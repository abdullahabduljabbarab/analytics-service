import os

# The analytical store is BigQuery, reached with the runtime's own credentials
# (Application Default Credentials on Cloud Run), so there is no connection
# string and no database password. These name the dataset the raw event history
# and the projection tables live in.
BIGQUERY_PROJECT = os.getenv("BIGQUERY_PROJECT", "ledger-api-507618")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "analytics")
BIGQUERY_LOCATION = os.getenv("BIGQUERY_LOCATION", "europe-west2")

# The service account Pub/Sub uses to sign push OIDC tokens. When set, the
# ingest endpoint requires a verified token from this identity; unset locally and
# in tests, where the endpoint is open. Mirrors the risk engine and notification
# service ingress.
PUBSUB_PUSH_SA = os.getenv("PUBSUB_PUSH_SA", "")

# "memory" selects the in-memory store (local, tests); "bigquery" selects the
# BigQuery adapter (production). The two satisfy the same store contract.
STORE_BACKEND = os.getenv("STORE_BACKEND", "memory")

ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
