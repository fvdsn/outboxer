output "project_id" {
  value = var.project_id
}

output "region" {
  value = var.region
}

output "service_url" {
  value = google_cloud_run_v2_service.relay.uri
}

output "cloudsql_connection_name" {
  value = google_sql_database_instance.outboxer.connection_name
}

output "db_name" {
  value = google_sql_database.outboxer.name
}

output "db_user" {
  value = google_sql_user.outboxer.name
}

output "db_password" {
  value     = random_password.db.result
  sensitive = true
}

output "topic" {
  value = google_pubsub_topic.events.name
}

output "subscription" {
  value = google_pubsub_subscription.events.name
}

output "dlq_table" {
  value = local.relay_env.DLQ_TABLE
}

output "init_job" {
  value = google_cloud_run_v2_job.init.name
}

output "artifact_repository" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.outboxer.repository_id}"
}
