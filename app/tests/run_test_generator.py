# app/tests/run_test_generator.py

import requests
import json

API_ENDPOINT = "http://localhost:5001/generate-test-case"


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
    #Below are choice zone schools
    {"zone_name": "Valley Zone", "address": "2015 S 39th St, Louisville, KY 40211, USA"},
    {"zone_name": "Jeffersontown Zone", "address": "828 S 17th St, Louisville, KY 40210, USA"},
    {"zone_name": "Fairdale Zone", "address": "2323 Millers Ln, Louisville, KY 40216, USA"},
    {"zone_name": "Ballard Zone", "address": "3922 Garfield Ave, Louisville, KY 40212, USA"},
    {"zone_name": "Waggener Zone", "address": "1686 Letterle Ave, Louisville, KY 40206, USA"},
    {"zone_name": "Doss Zone", "address": "1131 S 32nd St, Louisville, KY 40211, USA"},
    {"zone_name": "Eastern Zone", "address": "2219 Owen St, Louisville, KY 40212, USA"},
    {"zone_name": "Moore Zone", "address": "1779 W Hill St, Louisville, KY 40210, USA"},
    {"zone_name": "Seneca Zone", "address": "500 W Gaulbert Ave, Louisville, KY 40208, USA"},
    {"zone_name": "Valley Zone", "address": "1611 Lyman Johnson Dr, Louisville, KY 40211, USA"},
    {"zone_name": "Fairdale Zone", "address": "2305 Ratcliffe Ave, Louisville, KY 40210, USA"},
    {"zone_name": "Pleasure Ridge Park Zone", "address": "608 S 40th St, Louisville, KY 40211, USA"},
    {"zone_name": "Atherton Zone", "address": "717 Gwendolyn St, Louisville, KY 40203, USA"},
    {"zone_name": "Waggener Zone", "address": "612 E Market St, Louisville, KY 40202, USA"},
    {"zone_name": "Atherton Zone", "address": "1418 Morton AVE, Louisville, KY 40204, USA"},
]

def run_generator():
    print("--- Starting Test Case Generation ---")
    print("Ensure your local API server (app/api.py) is running.")
    
    # <<< START: MODIFIED CODE >>>
    # Create a list to hold all the generated test cases
    all_test_cases = []
    
    for item in ADDRESSES_TO_TEST:
        try:
            print(f"Requesting test case for: {item['zone_name']}...")
            response = requests.post(API_ENDPOINT, json=item)
            
            if response.status_code == 200:
                # Get the JSON data from the response
                test_case_data = response.json()
                
                # Add the data to our list
                all_test_cases.append(test_case_data)
                print(f"  -> Success! Generated test case for {item['zone_name']}.")
            else:
                print(f"  -> ERROR for {item['zone_name']}: {response.status_code} - {response.text}")

        except requests.exceptions.ConnectionError:
            print("\n❌ FATAL ERROR: Could not connect to the API.")
            print("   Please make sure your local API server is running at http://localhost:5001")
            return # Stop the script if the server isn't running
            
    # After the loop, print the final, clean output
    if all_test_cases:
        print("\n\n" + "="*50)
        print("--- ✅ Generation Complete ---")
        print("--- 📋 COPY THE JSON ARRAY BELOW and append it to your test_cases.json file ---")
        print("="*50 + "\n")
        
        # Print the entire list as a properly formatted JSON array string
        print(json.dumps(all_test_cases, indent=2))
        
        print("\n" + "="*50)
    else:
        print("\n\n--- ⚠️ No test cases were generated. ---")
    # <<< END: MODIFIED CODE >>>

if __name__ == "__main__":
    run_generator()
