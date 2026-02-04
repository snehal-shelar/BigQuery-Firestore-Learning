import logging
import os

import functions_framework
from google.cloud import bigquery

from .bq_operations.transform import run_transform

client = bigquery.Client()


@functions_framework.cloud_event
def gcs_to_bigquery_pipeline(cloud_event):
    """
    Triggers when a file is uploaded to the GCS bucket.
    - Loads CSV data into a BigQuery staging table.
    - Executes a transformation logic to a final table.
    """
    # 1. Capture metadata from the GCS upload event
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    uri = f"gs://{bucket_name}/{file_name}"

    logging.info(f"Starting pipeline for: {uri}")

    # Configuration for tables
    project = os.environ.get("GCP_PROJECT_ID", "dataflow-pipeline-485105")
    staging_table = f"{project}.organization.employees_advanced"

    try:
        # 1: THE LOAD (Extract/Load)
        load_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )

        logging.info(f"Loading {file_name} into staging table: {staging_table}")
        load_job = client.load_table_from_uri(uri, staging_table, job_config=load_config)
        load_job.result()
        logging.info("Load to staging successful.")

        # 2: THE TRANSFORM (Transform/Load)
        run_transform(client)
        logging.info(f"Pipeline finished successfully for {file_name}.")

    except Exception as e:
        logging.error(f"Pipeline failed for {file_name}: {str(e)}")
        raise e
