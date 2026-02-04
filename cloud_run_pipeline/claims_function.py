import logging

import functions_framework
from google.cloud import bigquery

from cloud_run_pipeline.bq_operations.transform import claims_transform

client = bigquery.Client()


@functions_framework.cloud_event
def process_claims_matching(cloud_event):
    # 1. Get GCS file info
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]

    # Mocking the API payload values (typically these would come from a database or request)
    encounter_dos = "2024-01-15"
    encounter_npi = "1000000001"
    # payload = data.get("metadata", {})
    # encounter_dos = payload.get("encounter_dos")
    # encounter_npi = payload.get("encounter_npi")

    # 2. Access the file from Storage
    claims_transform(client, encounter_dos, encounter_npi)
    logging.info(f"Pipeline finished successfully for {file_name}.")
