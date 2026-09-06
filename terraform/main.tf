terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

data "google_project" "current" {}

# The three topics analytics consumes, all owned elsewhere: the ledger's
# transaction events, the orchestrator's payment events, and the risk engine's
# decisions. Referenced here, never created here. Analytics publishes nothing.
data "google_pubsub_topic" "transaction_events" {
  name = var.transaction_events_topic
}

data "google_pubsub_topic" "payment_events" {
  name = var.payment_events_topic
}

data "google_pubsub_topic" "risk_events" {
  name = var.risk_events_topic
}

# The analytical store: a BigQuery dataset holding raw_events and the projection
# tables. No Cloud SQL, no relational schema, no secret.
resource "google_bigquery_dataset" "analytics" {
  dataset_id  = var.dataset
  location    = var.region
  description = "ABS analytics: durable raw event history and derived projections"
}

# A dedicated runtime identity for the Cloud Run service and the refresh Job,
# rather than the broad default compute service account, so BigQuery access is
# scoped to exactly this workload. It needs to read and write the dataset and to
# run query jobs, and nothing else.
resource "google_service_account" "runtime" {
  account_id   = "analytics-service-runtime"
  display_name = "Analytics Service Runtime"
}

resource "google_bigquery_dataset_iam_member" "runtime_editor" {
  dataset_id = google_bigquery_dataset.analytics.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_project_iam_member" "runtime_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_artifact_registry_repository" "analytics_service" {
  location      = var.region
  repository_id = "analytics-service"
  format        = "DOCKER"
}

# A dedicated identity for the push subscriptions. Pub/Sub mints an OIDC token as
# this account; the consumer endpoint verifies it before applying any event.
resource "google_service_account" "pubsub_push" {
  account_id   = "analytics-pubsub-push"
  display_name = "Analytics Pub/Sub Push"
}

resource "google_service_account_iam_member" "pubsub_token_creator" {
  service_account_id = google_service_account.pubsub_push.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Dead-letter for events the consumer cannot process at all.
resource "google_pubsub_topic" "dead_letter" {
  name = "analytics-events-deadletter"
}

resource "google_cloud_run_v2_service" "analytics_service" {
  name     = "analytics-service"
  location = var.region

  template {
    service_account = google_service_account.runtime.email

    containers {
      image = "${var.region}-docker.pkg.dev/${var.project_id}/analytics-service/analytics-service:latest"

      ports {
        container_port = 8080
      }

      env {
        name  = "ENVIRONMENT"
        value = "production"
      }
      env {
        name  = "STORE_BACKEND"
        value = "bigquery"
      }
      env {
        name  = "BIGQUERY_PROJECT"
        value = var.project_id
      }
      env {
        name  = "BIGQUERY_DATASET"
        value = google_bigquery_dataset.analytics.dataset_id
      }
      env {
        name  = "PUBSUB_PUSH_SA"
        value = google_service_account.pubsub_push.email
      }

      resources {
        limits = {
          cpu    = "1"
          memory = "512Mi"
        }
      }
    }

    scaling {
      min_instance_count = 0
      max_instance_count = 3
    }
  }
}

# The read and health endpoints are public; the ingest endpoint is protected at
# the application layer by the push OIDC token, so public invoke is safe.
resource "google_cloud_run_v2_service_iam_member" "public" {
  name     = google_cloud_run_v2_service.analytics_service.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "allUsers"
}

# The projection refresh, the same image run as a job. A scheduled execution
# materializes projections from raw_events off the ingest path.
resource "google_cloud_run_v2_job" "analytics_refresh" {
  name     = "analytics-refresh"
  location = var.region

  template {
    template {
      service_account = google_service_account.runtime.email

      containers {
        image   = "${var.region}-docker.pkg.dev/${var.project_id}/analytics-service/analytics-service:latest"
        command = ["python"]
        args    = ["-m", "app.admin", "refresh"]

        env {
          name  = "STORE_BACKEND"
          value = "bigquery"
        }
        env {
          name  = "BIGQUERY_PROJECT"
          value = var.project_id
        }
        env {
          name  = "BIGQUERY_DATASET"
          value = google_bigquery_dataset.analytics.dataset_id
        }
      }
    }
  }
}

# A push subscription on each of the three upstream topics, all delivering to the
# single consumer endpoint, which routes on event type.
locals {
  push_endpoint = "${google_cloud_run_v2_service.analytics_service.uri}/events/pubsub"
  source_topics = {
    transaction = data.google_pubsub_topic.transaction_events.id
    payment     = data.google_pubsub_topic.payment_events.id
    risk        = data.google_pubsub_topic.risk_events.id
  }
}

resource "google_pubsub_subscription" "to_analytics" {
  for_each = local.source_topics

  name  = "${each.key}-events-to-analytics"
  topic = each.value

  push_config {
    push_endpoint = local.push_endpoint

    oidc_token {
      service_account_email = google_service_account.pubsub_push.email
    }
  }

  ack_deadline_seconds = 20

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dead_letter.id
    max_delivery_attempts = 5
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }
}

# Dead-lettering requires the Pub/Sub service agent to publish to the dead-letter
# topic and to acknowledge on each subscription.
resource "google_pubsub_topic_iam_member" "dead_letter_publish" {
  topic  = google_pubsub_topic.dead_letter.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription_iam_member" "dead_letter_subscribe" {
  for_each = google_pubsub_subscription.to_analytics

  subscription = each.value.name
  role         = "roles/pubsub.subscriber"
  member       = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# Cloud Scheduler runs the refresh job on a schedule, authenticating as its own
# identity with permission to execute the job.
resource "google_service_account" "scheduler" {
  account_id   = "analytics-scheduler"
  display_name = "Analytics Refresh Scheduler"
}

resource "google_cloud_run_v2_job_iam_member" "scheduler_runs_job" {
  name     = google_cloud_run_v2_job.analytics_refresh.name
  location = var.region
  role     = "roles/run.invoker"
  member   = "serviceAccount:${google_service_account.scheduler.email}"
}

resource "google_cloud_scheduler_job" "refresh" {
  name     = "analytics-refresh-schedule"
  region   = var.region
  schedule = var.refresh_schedule

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/${google_cloud_run_v2_job.analytics_refresh.name}:run"

    oauth_token {
      service_account_email = google_service_account.scheduler.email
    }
  }
}

# Keyless CI deploy via Workload Identity Federation. The pool and provider are
# shared across the ABS services and referenced here, not recreated; the pool has
# no data source, so its resource name is composed from the project number.
locals {
  wif_pool_name = "projects/${data.google_project.current.number}/locations/global/workloadIdentityPools/${var.wif_pool_id}"
}

resource "google_service_account" "deploy" {
  account_id   = "analytics-service-deploy"
  display_name = "Analytics Service Deploy"
}

resource "google_project_iam_member" "deploy_roles" {
  for_each = toset([
    "roles/run.admin",
    "roles/artifactregistry.writer",
    "roles/iam.serviceAccountUser",
  ])
  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.deploy.email}"
}

# Only the analytics-service repository may impersonate the deploy account.
resource "google_service_account_iam_member" "deploy_wif" {
  service_account_id = google_service_account.deploy.name
  role               = "roles/iam.workloadIdentityUser"
  member             = "principalSet://iam.googleapis.com/${local.wif_pool_name}/attribute.repository/${var.github_owner}/${var.github_repo}"
}
