provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "raw_bucket" {
  name     = "${var.project_id}-raw-bucket"
  location = var.region
}

resource "google_bigquery_dataset" "bronze" {
  dataset_id = "bronze"
  location   = var.region
}

resource "google_bigquery_dataset" "silver" {
  dataset_id = "silver"
  location   = var.region
}

resource "google_bigquery_dataset" "gold" {
  dataset_id = "gold"
  location   = var.region
}
