variable "project_id" {
  description = "GCP project ID. Analytics shares the ledger's project."
  type        = string
  default     = "ledger-api-507618"
}

variable "region" {
  description = "GCP region for all resources"
  type        = string
  default     = "europe-west2"
}

variable "dataset" {
  description = "BigQuery dataset holding raw_events and the projection tables"
  type        = string
  default     = "analytics"
}

variable "transaction_events_topic" {
  description = "The ledger's topic analytics consumes transaction events from"
  type        = string
  default     = "transaction-events"
}

variable "payment_events_topic" {
  description = "The orchestrator's topic analytics consumes payment events from"
  type        = string
  default     = "payment-events"
}

variable "risk_events_topic" {
  description = "The risk engine's topic analytics consumes risk decisions from"
  type        = string
  default     = "risk-events"
}

variable "refresh_schedule" {
  description = "Cron schedule for the projection refresh job"
  type        = string
  default     = "*/10 * * * *"
}

variable "wif_pool_id" {
  description = "The shared GitHub Actions Workload Identity pool, owned by platform-infrastructure and referenced here"
  type        = string
  default     = "github-actions"
}

variable "github_owner" {
  description = "GitHub owner allowed to deploy via Workload Identity Federation"
  type        = string
  default     = "abdullahabduljabbarab"
}

variable "github_repo" {
  description = "GitHub repository allowed to impersonate the deploy service account"
  type        = string
  default     = "analytics-service"
}
