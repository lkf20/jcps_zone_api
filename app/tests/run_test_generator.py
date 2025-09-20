# app/tests/run_test_generator.py
import requests
import json

API_ENDPOINT = "http://localhost:5001/generate-test-case"
# Define the output file name
OUTPUT_FILE = "generated_test_cases.json" 

# ADDRESSES_TO_TEST = [
#      {"zone_name": "Doss Zone", "address": "7601 St Andrews Church Rd, Louisville, KY 40214"},
# ]

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
    {"zone_name": "Jeffersontown Zone", "address": "9619 Glenawyn Circle, Louisville, KY 40299"},
    {"zone_name": "Moore Zone", "address": "6415 Outer Loop, Louisville, KY 40228"},
    {"zone_name": "Pleasure Ridge Park Zone", "address": "5901 Greenwood Rd, Louisville, KY 40258"},
    {"zone_name": "Southern Zone", "address": "8620 Preston Hwy, Louisville, KY 40219"},
    {"zone_name": "Valley Zone", "address": "10200 Dixie Hwy, Louisville, KY 40258"},
    # Below are choice zone schools
    {"zone_name": "Jeffersontown Zone", "address": "828 S 17th St, Louisville, KY 40210, USA"},
    {"zone_name": "Jeffersontown Zone", "address": "1421 Magazine St, Louisville, KY 40203, USA"},
    {"zone_name": "Fairdale Zone", "address": "2323 Millers Ln, Louisville, KY 40216, USA"},
    {"zone_name": "Ballard Zone", "address": "3922 Garfield Ave, Louisville, KY 40212, USA"},
    {"zone_name": "Waggener Zone", "address": "1686 Letterle Ave, Louisville, KY 40206, USA"},
    {"zone_name": "Doss Zone", "address": "1131 S 32nd St, Louisville, KY 40211, USA"},
    {"zone_name": "Eastern Zone", "address": "2219 Owen St, Louisville, KY 40212, USA"},
    {"zone_name": "Moore Zone", "address": "1779 Bolling Ave, Louisville, KY 40210, USA"},
    {"zone_name": "Seneca Zone", "address": "500 W Gaulbert Ave, Louisville, KY 40208, USA"},
    {"zone_name": "Valley Zone", "address": "1611 Lyman Johnson Dr, Louisville, KY 40211, USA"},
    {"zone_name": "Pleasure Ridge Park Zone", "address": "608 S 40th St, Louisville, KY 40211, USA"},
    {"zone_name": "Atherton Zone", "address": "717 Gwendolyn St, Louisville, KY 40203, USA"},
    {"zone_name": "Atherton Zone", "address": "1418 Morton AVE, Louisville, KY 40204, USA"},
    # Other middle schools to test
    {"zone_name": "Eastern Zone", "address": "16005 Shelbyville Rd, Louisville, KY 40245, USA"},
    {"zone_name": "Jeffersontown Zone", "address": "4320 Billtown Rd, Jeffersontown, KY 40299, USA"},
    {"zone_name": "Fern Creek Zone", "address": "5313 Sprigwood Ln, Louisville, KY 40291, USA"},
    {"zone_name": "Fern Creek Zone", "address": "5250 Bardstown Rd, Louisville, KY 40291, USA"},
    {"zone_name": "Southern Zone", "address": "5044 Poplar Level Rd, Louisville, KY 40219, USA"},
    {"zone_name": "Iroquois Zone", "address": "2118 S Preston St, Louisville, KY 40217, USA"},
    {"zone_name": "Pleasure Ridge Park Zone", "address": "1313 Southwestern Pkwy, Louisville, KY 40211"},
]

def run_generator():
    print("--- Starting Test Case Generation ---")
    print("Ensure your local API server (app/api.py) is running.")
    
    all_test_cases = []
    
    for item in ADDRESSES_TO_TEST:
        try:
            print(f"Requesting test case for: {item['zone_name']}...")
            response = requests.post(API_ENDPOINT, json=item)
            
            if response.status_code == 200:
                test_case_data = response.json()
                all_test_cases.append(test_case_data)
                print(f"  -> Success! Generated test case for {item['zone_name']}.")
            else:
                print(f"  -> ERROR for {item['zone_name']}: {response.status_code} - {response.text}")

        except requests.exceptions.ConnectionError:
            print("\n❌ FATAL ERROR: Could not connect to the API.")
            print("   Please make sure your local API server is running at http://localhost:5001")
            return
            
    # After the loop, write the final output directly to a file
    if all_test_cases:
        try:
            with open(OUTPUT_FILE, 'w') as f:
                json.dump(all_test_cases, f, indent=2)
            
            print("\n\n" + "="*50)
            print("--- ✅ Generation Complete ---")
            print(f"--- 📋 The new test cases have been saved to the file: {OUTPUT_FILE} ---")
            print("---    Open that file, copy its contents, and replace the content of test_cases.json. ---")
            print("="*50 + "\n")

        except IOError as e:
            print(f"\n\n--- ❌ ERROR: Could not write to file {OUTPUT_FILE}. Error: {e} ---")
    else:
        print("\n\n--- ⚠️ No test cases were generated. ---")


if __name__ == "__main__":
    run_generator()