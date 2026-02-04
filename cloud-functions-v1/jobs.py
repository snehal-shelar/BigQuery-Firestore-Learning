from google.cloud import bigquery


def load_to_staging(bucket_name, file_name):
    client = bigquery.Client()
    table_id = "dataflow-pipeline-485105.organization.staging_table"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",  # Refresh staging every time
    )

    uri = f"gs://{bucket_name}/{file_name}"
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    load_job.result()  # Wait for the load to finish
    print(f"Loaded {file_name} to staging.")


def transform_data():
    """Loads data from staging table and transforms it into BigQuery table"""
    client = bigquery.Client()

    # Example: Moving data using the logic from your window_function_queries.sql
    sql = """
          INSERT INTO `your_dataset.final_employees`
          SELECT * \
          FROM `your_dataset.staging_table` QUALIFY ROW_NUMBER() OVER(PARTITION BY emp_id ORDER BY join_date DESC) = 1 \
          """

    query_job = client.query(sql)
    query_job.result()
    print("Transformation complete.")
