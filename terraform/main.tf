terraform {
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

# GCS bucket data lake
resource "google_storage_bucket" "data_lake" {
  name          = "${var.project_id}-data-lake"
  location      = var.region
  force_destroy = true
}

# BigQuery datasets
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw_stock_data"
  location   = var.region
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = "mart_stock_data"
  location   = var.region
}

# Service account for pipeline scripts
resource "google_service_account" "pipeline_sa" {
  account_id   = "stock-pipeline-sa"
  display_name = "Stock Pipeline Service Account"
}

resource "google_project_iam_member" "sa_gcs_access" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

resource "google_project_iam_member" "sa_bq_access" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}