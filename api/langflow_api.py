import requests
import os
import uuid

api_key = '<api key>'
url = "http://localhost:3000/api/v1/run/<flow_id>"  # The complete API endpoint URL for this flow

# Request payload configuration
payload = {
    "output_type": "chat",
    "input_type": "chat",
    "input_value": "What is this document about?",
    "tweaks": {
        "AstraDB-2F3UL": {
            "advanced_search_filter": "{'filename': FAQ_LATAM_Airlines (1).pdf}"
        }
    }
}
payload["session_id"] = str(uuid.uuid4())

headers = {"x-api-key": api_key}

try:
    # Send API request
    response = requests.request("POST", url, json=payload, headers=headers)
    response.raise_for_status()  # Raise exception for bad status codes

    # Print response
    print(response.text)

except requests.exceptions.RequestException as e:
    print(f"Error making API request: {e}")
except ValueError as e:
    print(f"Error parsing response: {e}")