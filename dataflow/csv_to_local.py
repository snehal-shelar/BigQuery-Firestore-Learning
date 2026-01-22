import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import logging

# Define the column names based on your CSV structure, This helps us map the comma-separated values to keys
COLUMNS = [
    'school', 'sex', 'age', 'address', 'famsize', 'Pstatus', 'Medu', 'Fedu',
    'Mjob', 'Fjob', 'reason', 'guardian', 'traveltime', 'studytime',
    'failures', 'schoolsup', 'famsup', 'paid', 'activities', 'nursery',
    'higher', 'internet', 'romantic', 'famrel', 'freetime', 'goout', 'Dalc',
    'Walc', 'health', 'absences', 'passed'
]


def parse_csv(line):
    """
        - Split the raw line by comma
        - Create a dictionary by zipping column names with values
        - Convert numeric fields for calculation
    """
    values = line.split(',')

    row = dict(zip(COLUMNS, values))

    try:
        row['studytime'] = int(row['studytime'])
        row['failures'] = int(row['failures'])
        row['age'] = int(row['age'])
    except ValueError:
        pass

    return row


def filter_passed_students(row):
    """
        Only keep students where 'passed' is 'yes'
        .strip() removes any accidental whitespace/newlines
    """

    return row.get('passed', '').strip().lower() == 'yes'


def enrich_data(row):
    """
        Return a simplified string for the final report
    """
    effort_score = (row['studytime'] * 10) - (row['failures'] * 5)

    return f"Student ({row['sex']}, {row['age']}y) | StudyTime: {row['studytime']} | Effort Score: {effort_score}"


def run():
    options = PipelineOptions()

    with beam.Pipeline(options=options) as p:
        (
                p
                # 1. READ: Read the file, skipping the header row
                | 'Read CSV' >> ReadFromText('student-data.csv', skip_header_lines=1)

                # 2. PARSE: Convert string lines to dictionaries
                | 'Parse CSV' >> beam.Map(parse_csv)

                # 3. FILTER: Keep only students who passed
                | 'Filter Passed' >> beam.Filter(filter_passed_students)

                # 4. TRANSFORM: Calculate scores and format output
                | 'Enrich Data' >> beam.Map(enrich_data)

                # 5. WRITE: Save to output file
                | 'Write Report' >> WriteToText('passed_students_report')
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()