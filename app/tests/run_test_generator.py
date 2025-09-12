# app/tests/run_test_generator.py

import requests
import json

API_ENDPOINT = "http://localhost:5001/generate-test-case"

# --- YOUR LIST OF 14 ADDRESSES ---
# Fill this list out with one valid address from each of your 14 zones.
# I have added a few examples.
ADDRESSES_TO_TEST = [
    {"zone_name": "Waggener Zone", "address": "330 S Hubbards Ln, Louisville, KY 40207"},
    {"zone_name": "Ballard Zone", "address": "7005 Shallow Lake Rd, Prospect, KY 40059"},
    {"zone_name": "Atherton Zone", "address": "2901 Falmouth Dr, Louisville, KY 40205"},
    {"zone_name": "Seneca Zone", "address": "4425 Preston Hwy, Louisville, KY 40213"},
    {"zone_name": "Eastern Zone", "address": "12400 Old Shelbyville Rd, Louisville, KY"},
    {"zone_name": "Iroquois Zone", "address": "4615 Taylor Blvd, Louisville, KY"},
    {"zone_name": "Doss Zone", "address": "7601 St Andrews Church Rd, Louisville, KY 40214"},
    {"zone_name": "Fairdale Zone", "address": "9906 Callie Dr, Louisville, KY"},
    {"zone_name": "Fern Creek Zone", "address": "9115 Fern Creek Rd, Louisville, KY 40291"},
    {"zone_name": "Jeffersontown Zone", "address": "2209 Patti Ln, Jeffersontown, KY 40299"},
    {"zone_name": "Moore Zone", "address": "6415 Outer Loop, Louisville, KY 40228"},
    {"zone_name": "Pleasure Ridge Park Zone", "address": "5901 Greenwood Rd, Louisville, KY 40258"},
    {"zone_name": "Southern Zone", "address": "8620 Preston Hwy, Louisville, KY 40219"},
    {"zone_name": "Valley Zone", "address": "10200 Dixie Hwy, Louisville, KY 40258"},
]

def run_generator():
    print("Starting test case generation...")
    print("Ensure your local API server (app/api.py) is running.")
    
    for item in ADDRESSES_TO_TEST:
        try:
            print(f"\nRequesting test case for: {item['zone_name']}")
            response = requests.post(API_ENDPOINT, json=item)
            if response.status_code == 200:
                print(f"-> Success! Check your API terminal for the JSON output.")
            else:
                print(f"-> ERROR for {item['zone_name']}: {response.text}")
        except requests.exceptions.ConnectionError as e:
            print("\nFATAL ERROR: Could not connect to the API.")
            print("Please make sure your local API server is running at http://localhost:5001")
            break
            
if __name__ == "__main__":
    run_generator()