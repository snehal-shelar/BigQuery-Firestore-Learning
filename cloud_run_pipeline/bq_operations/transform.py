import os

from google.cloud import bigquery


def run_transform(client, final_table, staging_table):
    # Define the path to your SQL file
    # This works whether running locally or on Cloud Functions
    base_path = os.path.dirname(__file__)
    sql_path = os.path.join(base_path, 'sql', 'employee_transform.sql')

    # 1. READ: Open and read the SQL file as a single string
    with open(sql_path, 'r') as file:
        query_string = file.read()

    # Define the parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id_to_find", "INTEGER", 1),
        ]
    )
    # 2. EXECUTE: Pass the string directly to BigQuery
    print(f"Executing query from {sql_path}...")
    query_job = client.query(query_string, job_config=job_config)

    # 3. WAIT: Blocks until the query is finished
    results = query_job.result()
    print(f"Query completed successfully. Job ID: {query_job.job_id}")
