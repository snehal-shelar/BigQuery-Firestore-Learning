import os

from google.cloud import bigquery


def run_transform(client):
    base_path = os.path.dirname(__file__)
    sql_path = os.path.join(base_path, 'sql', 'employee_transform.sql')

    # Open and read the SQL file as a single string
    with open(sql_path, 'r') as file:
        query_string = file.read()

    # Define the parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("id_to_find", "INTEGER", 1),
        ]
    )
    # Pass the string directly to BigQuery
    print(f"Executing query from {sql_path}...")
    query_job = client.query(query_string, job_config=job_config)

    results = query_job.result()
    print(f"Query completed successfully. Job ID: {query_job.job_id}")
