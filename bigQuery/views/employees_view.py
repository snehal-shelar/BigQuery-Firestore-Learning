from google.cloud import bigquery
import pandas as pd


def get_salary_trends(dept_name: str):
    """
    Connects to BigQuery and retrieves salary trend data
    from the employee_view view.
    """
    # Initialize the client
    client = bigquery.Client()

    query = """
            SELECT * \
            FROM `organization.employee_view`
            WHERE dept_name = @dept \
            """


    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("dept", "STRING", dept_name)
        ]
    )

    # Execute the query and convert to a Pandas DataFrame
    query_job = client.query(query, job_config=job_config)
    df = query_job.to_dataframe()

    return df

if __name__ == '__main__':
    """
        This will run the main function.
    """
    df_devops = get_salary_trends("Engineer")
    print(df_devops.head())
