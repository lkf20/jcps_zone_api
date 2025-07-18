import requests
import json

api_url = "http://localhost:5001/school-details-by-address"
# payload = {"address": "7005 Shallow Lake Road Prospect, KY"}
# Or try another address:
payload = {"address": "4425 Preston Hwy, Louisville, KY"}


headers = {"Content-Type": "application/json"}

try:
    response = requests.post(api_url, data=json.dumps(payload), headers=headers, timeout=30) # Increased timeout just in case
    response.raise_for_status()  # Raises an HTTPError for bad responses (4XX or 5XX)
    print("Status Code:", response.status_code)

    # --- THIS IS THE UPDATED PART ---
    # Get the JSON object from the response
    json_response = response.json()
    
    # Use json.dumps to format it with an indent of 2 spaces for pretty-printing
    formatted_json = json.dumps(json_response, indent=2)
    
    print("Response JSON:")
    print(formatted_json)
    # --- END OF UPDATE ---

except requests.exceptions.HTTPError as errh:
    print(f"Http Error: {errh}")
    print("Response content:", response.content.decode())
except requests.exceptions.ConnectionError as errc:
    print(f"Error Connecting: {errc}")
except requests.exceptions.Timeout as errt:
    print(f"Timeout Error: {errt}")
except requests.exceptions.RequestException as err:
    print(f"Oops: Something Else: {err}")
except json.JSONDecodeError:
    print("Could not decode JSON response. Raw response:")
    print(response.text)