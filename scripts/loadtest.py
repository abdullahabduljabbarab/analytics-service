"""
Load test harness for the Analytics Service.

Run against the live GCP deployment:
    locust -f scripts/loadtest.py --host https://analytics-service-eppidgbmxa-nw.a.run.app

Then open http://localhost:8089, or run headless with -u / -r / -t. Set CA_BUNDLE
to a certificate bundle if a local TLS-inspecting proxy breaks verification.

The ingest endpoint is authenticated (a Pub/Sub OIDC token), so this exercises the
public read path: the aggregate endpoints, each of which reads a materialized
projection from BigQuery. Set ACCOUNT_ID to an account with activity to exercise
the per-account endpoint against real data.
"""

import os
import uuid

from locust import HttpUser, between, task

CA_BUNDLE = os.getenv("CA_BUNDLE")
ACCOUNT_ID = os.getenv("ACCOUNT_ID", str(uuid.uuid4()))


class AnalyticsUser(HttpUser):
    wait_time = between(0.1, 0.5)

    def on_start(self):
        if CA_BUNDLE:
            self.client.verify = CA_BUNDLE

    @task(5)
    def overview(self):
        self.client.get("/analytics/overview")

    @task(2)
    def payments(self):
        self.client.get("/analytics/payments")

    @task(2)
    def risk(self):
        self.client.get("/analytics/risk")

    @task(1)
    def providers(self):
        self.client.get("/analytics/providers")

    @task(1)
    def account(self):
        self.client.get(f"/analytics/accounts/{ACCOUNT_ID}", name="/analytics/accounts/[id]")

    @task(1)
    def health(self):
        self.client.get("/health")
