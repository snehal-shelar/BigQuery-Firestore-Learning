import functions_framework
import json
from google.cloud import bigquery

from bq_operations.transformation import claims_transform

client = bigquery.Client()


@functions_framework.http
def process_claims_matching_http(request):
    # Get JSON from the POST request body
    request_json = request.get_json(silent=True)

    if request_json:
        encounter_dos = request_json.get("encounter_dos")
        encounter_npi = request_json.get("encounter_npi")
        provider_name = request_json.get("provider_name")
    else:
        return "Error: No JSON payload provided", 400

    # transformation function will execute the SQL queries
    results = claims_transform(client, encounter_dos, encounter_npi, provider_name)

    output = []
    for row in results:
        item = dict(row)
        # Convert any Date objects to strings
        for key, value in item.items():
            if hasattr(value, 'isoformat'):
                item[key] = value.isoformat()
        output.append(item)

    return json.dumps({
        "status": "success",
        "data": output
    })
