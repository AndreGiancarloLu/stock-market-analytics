output "bucket_name" {
  value = google_storage_bucket.data_lake.name
}

output "bigquery_dataset_raw" {
  value = google_bigquery_dataset.raw.dataset_id
}

output "bigquery_dataset_mart" {
  value = google_bigquery_dataset.mart.dataset_id
}

output "service_account_email" {
  value = google_service_account.pipeline_sa.email
}