import logging

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

# GCP Project Configurations
PROJECT_ID = 'dataflow-pipeline-485105'
BUCKET = 'dataflow-apache-pipeline'
DATASET_ID = 'student_data'
DATASET_TABLE = 'passed_students'


def parse_csv(line):
    # Define columns based on student data CSV
    cols = ['school', 'sex', 'age', 'address', 'famsize', 'Pstatus', 'Medu', 'Fedu',
            'Mjob', 'Fjob', 'reason', 'guardian', 'traveltime', 'studytime',
            'failures', 'schoolsup', 'famsup', 'paid', 'activities', 'nursery',
            'higher', 'internet', 'romantic', 'famrel', 'freetime', 'goout', 'Dalc',
            'Walc', 'health', 'absences', 'passed']

    values = line.split(',')
    if len(values) < len(cols):
        return None

    row = dict(zip(cols, values))

    # Type conversion
    try:
        row['studytime'] = int(row['studytime'])
        row['failures'] = int(row['failures'])
        row['age'] = int(row['age'])
    except ValueError:
        return None
    return row


def filter_passed(row):
    if row is None: return False
    # Check if 'passed' column is 'yes' (case-insensitive)
    return row.get('passed', '').strip().lower() == 'yes'


def format_for_bq(row):
    # Transform into BigQuery Schema format
    return {
        'student_age': row['age'],
        'sex': row['sex'],
        'study_time': row['studytime'],
        'grade_status': 'PASS'
    }


def run():
    """
        This function will create a job on google cloud.
        This will read file from csv, format/transform the data and load into BigQuery.
    """
    clean_bucket = BUCKET
    gcs_temp_location = f"gs://{clean_bucket}/temp"

    pipeline_args = [
        f'--project={PROJECT_ID}',
        f'--temp_location={gcs_temp_location}',
        f'--region=us-central1',

        # THIS TURNS IT INTO A REAL CLOUD JOB:
        '--runner=DataflowRunner',

        # REQUIRED: Where to stage the code packages
        f'--staging_location=gs://{clean_bucket}/staging',

        # Name of the job in the console of Dataflow
        '--job_name=my-first-cloud-job-v1'
    ]

    # Apply options to pipelines
    options = PipelineOptions(flags=pipeline_args)

    # Define Table Reference: PROJECT:DATASET.TABLE
    table_spec = f"{PROJECT_ID}:{DATASET_ID}.{DATASET_TABLE}"

    # Define Table Schema
    table_schema = 'student_age:INTEGER, sex:STRING, study_time:INTEGER, grade_status:STRING'

    print(f"Pipeline Configured.")
    print(f"Project: {PROJECT_ID}")
    print(f"Temp GCS: {gcs_temp_location}")

    with beam.Pipeline(options=options) as p:
        (
                p
                | 'ReadLocalCSV' >> ReadFromText('student-data.csv', skip_header_lines=1)
                | 'ParseCSV' >> beam.Map(parse_csv)
                | 'FilterPassed' >> beam.Filter(filter_passed)
                | 'FormatBQ' >> beam.Map(format_for_bq)
                | 'WriteToBQ' >> WriteToBigQuery(
            table_spec,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location=gcs_temp_location
        )
        )
    print("Pipeline Finished! Go check BigQuery.")


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.ERROR)
    run()
