output "cloud_run_url" {
  description = "Live analytics service URL"
  value       = google_cloud_run_v2_service.analytics_service.uri
}

output "artifact_registry" {
  description = "Docker image registry path"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/analytics-service"
}

output "bigquery_dataset" {
  description = "The analytical dataset holding raw_events and projections"
  value       = google_bigquery_dataset.analytics.dataset_id
}

output "subscriptions" {
  description = "Push subscriptions feeding the three event streams into analytics"
  value       = [for s in google_pubsub_subscription.to_analytics : s.name]
}

output "refresh_job" {
  description = "The Cloud Run job that materializes projections"
  value       = google_cloud_run_v2_job.analytics_refresh.name
}

output "dead_letter_topic" {
  description = "Transport dead-letter topic for unprocessable events"
  value       = google_pubsub_topic.dead_letter.id
}
