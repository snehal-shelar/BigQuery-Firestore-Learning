# @functions_framework.http
# def hello_world(request):
#     """HTTP Cloud Function.
#     Args:
#         request (flask.Request): The response object.
#     Returns:
#         The response text.
#     """
#     request_json = request.get_json(silent=True)
#     request_args = request.args
#
#     if request_json and 'name' in request_json:
#         name = request_json['name']
#     elif request_args and 'name' in request_args:
#         name = request_args['name']
#     else:
#         name = 'World'
#
#     return f'Hello {name}! Your first Cloud Function is workings.'

# import googleapiclient.discovery
# from google.oauth2 import service_account
#
#
# @functions_framework.http
# def trigger_dataflow(request):
#     """Simple HTTP version to test deployment first, basically this is the cloud function."""
#     project = 'dataflow-pipeline-485105'
#     region = 'us-central1'
#
#     return f"Status: Function is alive for project {project} in {region}!"
#
#
# # This is for the Background Trigger version later
# def trigger_dataflow_event(event, context):
#     print(f"Event received: {event}")

import functions_framework
import googleapiclient.discovery


@functions_framework.cloud_event
def trigger_dataflow_automation(cloud_event):
    # 1. Get file metadata from the Cloud Event
    data = cloud_event.data
    bucket = data["bucket"]
    file_name = data["name"]
    print(f"File detected: gs://{bucket}/{file_name}")

    # 2. Only process CSV files
    if not file_name.endswith('.csv'):
        print("Not a CSV. Skipping.")
        return

    project = 'dataflow-pipeline-485105'
    region = 'us-central1'

    # 3. Launch the Dataflow Job
    # Note: We use the 'launch' method for Flex Templates
    dataflow = googleapiclient.discovery.build('dataflow', 'v1b3')

    request = dataflow.projects().locations().flexTemplates().launch(
        projectId=project,
        location=region,
        body={
            "launchParameter": {
                "jobName": f"auto-job-{file_name.replace('.', '-')}",
                "containerSpecGcsPath": f"gs://{bucket}/templates/student_template.json",
                "parameters": {
                    "input": f"gs://{bucket}/{file_name}"
                }
            }
        }
    )

    response = request.execute()
    print(f"Success! Dataflow Job ID: {response['job']['id']}")
