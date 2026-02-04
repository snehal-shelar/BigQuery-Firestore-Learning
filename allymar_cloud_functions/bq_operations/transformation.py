import os

from google.cloud import bigquery


def claims_transform(client, encounter_dos, encounter_npi, provider_name):
    base_path = os.path.dirname(__file__)
    sql_path = os.path.join(base_path, 'sql', 'patient_claims_with_fuzzy_match.sql')

    # Open and read the SQL file as a single string
    with open(sql_path, 'r') as file:
        query_string = file.read()

    # Define the parameters
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("encounter_dos", "DATE", encounter_dos),
            bigquery.ScalarQueryParameter("encounter_npi", "INTEGER", encounter_npi),
            bigquery.ScalarQueryParameter("providerName", "STRING", provider_name),
        ]
    )
    # Pass the SQL string directly to BigQuery
    print(f"Executing query from {sql_path}")
    query_job = client.query(query_string, job_config=job_config)
    results = query_job.result()

    print(f"Query completed with Job ID: {query_job.job_id}")
    return results
