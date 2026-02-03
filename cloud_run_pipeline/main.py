import functions_framework
from google.cloud import bigquery

from .bq_operations.transform import run_transform

client = bigquery.Client()


@functions_framework.cloud_event
def gcs_to_bigquery_pipeline(cloud_event):
    # 1. CAPTURE metadata from the GCS upload event. (Extract)
    data = cloud_event.data
    bucket_name = data["bucket"]
    file_name = data["name"]
    uri = f"gs://{bucket_name}/{file_name}"

    print(f"Starting pipeline for: {uri}")

    # Configuration for tables
    project = "dataflow-pipeline-485105"  # os.environ.get("GCP_PROJECT_ID")
    staging_table = f"{project}.organization.employees_advanced"
    final_table = f"{project}.organization.employees_advanced"

    # --- STEP 1: THE LOAD (Extract/Load) ---
    # We use WRITE_TRUNCATE for staging so it always holds only the newest data
    load_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    print(f"Loading {file_name} into staging.")
    load_job = client.load_table_from_uri(uri, staging_table, job_config=load_config)
    load_job.result()  # Wait for the load to complete
    print("Load to staging successful.")

    # --- STEP 2: THE TRANSFORM (Transform/Load) ---
    # Using your practiced QUALIFY and ROW_NUMBER logic to ensure deduplication
    # Transform data using SQL or panda.
    run_transform(client, final_table, staging_table)
    print(f"Pipeline finished. Data from {file_name} is now in {final_table}.")
