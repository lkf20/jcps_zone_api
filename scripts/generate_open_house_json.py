# jcps_gis_api/scripts/generate_open_house_json.py

import pandas as pd
import json
import os
from collections import defaultdict

# --- Configuration ---
CSV_SOURCE_FILE = os.path.join('data', 'open_house_dates.csv')
JSON_OUTPUT_FILE = os.path.join('data', 'open_house_dates.json')

def process_and_generate_json():
    print("--- Starting Open House JSON Generation ---")

    if not os.path.exists(CSV_SOURCE_FILE):
        print(f"❌ ERROR: Source CSV file not found at '{CSV_SOURCE_FILE}'")
        return

    try:
        df = pd.read_csv(CSV_SOURCE_FILE, dtype={'School Code Adjusted': str})
        df['School Code Adjusted'].fillna(method='ffill', inplace=True)
        print(f"✅ Successfully loaded {len(df)} rows from CSV.")
    except Exception as e:
        print(f"❌ ERROR: Could not read or process the CSV file. Error: {e}")
        return

    schools_data = defaultdict(lambda: {
        "phone": None,
        "notes": None,
        # <<< START: ADDED CODE >>>
        "registration_link": None,
        # <<< END: ADDED CODE >>>
        "events": {
            "Open House": [],
            "School Tour": [],
            "Other": []
        }
    })

    for _, row in df.iterrows():
        sca = row.get('School Code Adjusted')
        if not sca or pd.isna(sca):
            continue
        sca = str(sca).strip()

        # <<< START: MODIFIED CODE >>>
        if pd.notna(row.get('Phone')):
            schools_data[sca]['phone'] = str(row['Phone']).strip()
        if pd.notna(row.get('Notes')):
            schools_data[sca]['notes'] = str(row['Notes']).strip()
        # This will capture the new registration link column
        if pd.notna(row.get('Registration link')):
            schools_data[sca]['registration_link'] = str(row['Registration link']).strip()
        # <<< END: MODIFIED CODE >>>

        if pd.notna(row.get('Start Time')):
            try:
                start_time = pd.to_datetime(row['Start Time']).isoformat()
                end_time = pd.to_datetime(row['End Time']).isoformat() if pd.notna(row.get('End Time')) else None
                
                event = {
                    "start": start_time,
                    "end": end_time,
                }
                
                event_type = str(row.get('Type', 'Other')).strip()
                if event_type in schools_data[sca]['events']:
                    schools_data[sca]['events'][event_type].append(event)
                else:
                    schools_data[sca]['events']['Other'].append(event)
            except Exception as e:
                print(f"⚠️ WARNING: Could not parse date for SCA {sca}. Skipping event. Error: {e}")

    for sca in schools_data:
        schools_data[sca]['events'] = {k: v for k, v in schools_data[sca]['events'].items() if v}

    try:
        with open(JSON_OUTPUT_FILE, 'w') as f:
            json.dump(schools_data, f, indent=2)
        print(f"\n--- ✅ Success! ---")
        print(f"Generated '{JSON_OUTPUT_FILE}' with data for {len(schools_data)} unique schools.")
    except Exception as e:
        print(f"❌ ERROR: Could not write the final JSON file. Error: {e}")

if __name__ == "__main__":
    process_and_generate_json()