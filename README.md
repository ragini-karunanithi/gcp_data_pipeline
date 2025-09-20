# Medallion GCP Pipeline

End-to-end CSV ingestion pipeline into BigQuery using Cloud Composer.
- Bronze (raw)
- Silver (curated)
- Gold (aggregated insights)

This project demonstrates an **end-to-end data engineering pipeline** on **Google Cloud Platform (GCP)** for consumer analytics.  
It ingests **CSV/JSON files** from **Google Cloud Storage (GCS)**, loads them into **BigQuery raw layer**, transforms the data into curated datasets, and finally prepares aggregated insights for reporting.

The workflow is orchestrated using **Cloud Composer (Airflow DAG)**.
