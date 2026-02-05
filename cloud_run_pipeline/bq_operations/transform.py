import os

from google.cloud import bigquery


def run_transform(client):
    base_path = os.path.dirname(__file__)
    sql_path = os.path.join(base_path, 'sql', 'employee_transform.sql')

    # Open and read the SQL file as a single string
    with open(sql_path, 'r') as file:
        query_string = file.read()
        
    # Pass the string directly to BigQuery
    print(f"Executing query from {sql_path}")
    query_job = client.query(query_string)

    results = query_job.result()
    print(f"Query completed. Job ID: {query_job.job_id}")
