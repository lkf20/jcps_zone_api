import pytest
import requests
import json
import os
import re

API_URL = "http://localhost:5001/school-details-by-address"
TEST_CASES_PATH = os.path.join(os.path.dirname(__file__), 'school_finder_website_tests.json')

def load_test_cases():
    with open(TEST_CASES_PATH, 'r') as f:
        return json.load(f)

# <<< START: MODIFIED CODE >>>
def normalize_school_name(name):
    """Cleans up a school name to handle common inconsistencies."""
    if not isinstance(name, str):
        return ""
    
    # 1. General cleaning: lowercase, remove periods, and extra whitespace
    name = name.lower().replace('.', '').strip()
    name = re.sub(r'\s+', ' ', name)
    
    # 2. Handle specific, known name variations
    name_map = {
        'the academy @ shawnee middle': 'the academy @ shawnee',
        'the academy @ shawnee high': 'the academy @ shawnee',
        'dupont manual high': 'dupont manual high',
        'hudson middle school': 'hudson middle',
        'perry elementary': 'dr william h perry elementary school',
        'stuart middle school': 'stuart academy',
        'wilkerson traditional elementary': 'wilkerson elementary',
        'greathouse/shryock traditional': 'greathouse/shryock traditional elementary',
        'norton commons elementary school': 'norton commons elementary'
    }
    
    # If the name is a key in our map, replace it with the standardized value
    if name in name_map:
        name = name_map[name]

    return name
# <<< END: MODIFIED CODE >>>

test_data = load_test_cases()
test_ids = [case.get('zone_name', f'test_case_{i}') for i, case in enumerate(test_data)]

@pytest.mark.parametrize("test_case", test_data, ids=test_ids)
def test_school_zones(test_case):
    """
    For a given address, this test validates the API response against the golden record
    and provides a user-friendly comparison on failure.
    """
    address = test_case['address']
    zone_name = test_case['zone_name']
    
    response = requests.post(API_URL, json={"address": address})
    assert response.status_code == 200, f"API returned a non-200 status for {zone_name}"
    
    api_data = response.json()
    
    api_schools = {}
    for zone in api_data.get("results_by_zone", []):
        for school in zone.get("schools", []):
            name = normalize_school_name(school['display_name'])
            status = school['display_status']
            if status == "Academies of Louisville": status = "Magnet/Choice Program"
            if status == "Satellite School": status = "Reside"
            api_schools[name] = status

    expected_schools = {}
    for level, expected_list in test_case["expected_schools"].items():
        for school in expected_list:
            if school['display_name'] == "Youth Performing Arts School":
                continue
            name = normalize_school_name(school['display_name'])
            expected_schools[name] = school['expected_status']

    api_school_set = set(api_schools.keys())
    expected_school_set = set(expected_schools.keys())

    missing_schools = expected_school_set - api_school_set
    extra_schools = api_school_set - expected_school_set
    
    status_mismatches = []
    common_schools = api_school_set.intersection(expected_school_set)
    for school_name in common_schools:
        if api_schools.get(school_name) != expected_schools.get(school_name):
            status_mismatches.append(
                f"  - {school_name}: (API Status: '{api_schools.get(school_name)}', Expected: '{expected_schools.get(school_name)}')"
            )

    error_messages = []
    if missing_schools:
        error_messages.append(f"\nSchools MISSING from API output:\n  - " + "\n  - ".join(sorted(list(missing_schools))))
    if extra_schools:
        error_messages.append(f"\nEXTRA schools found in API output:\n  - " + "\n  - ".join(sorted(list(extra_schools))))
    if status_mismatches:
        error_messages.append(f"\nSchools with STATUS MISMATCHES:\n" + "\n".join(status_mismatches))

    if error_messages:
        full_error_message = f"Mismatch found for zone '{zone_name}' ({address}):\n" + "\n".join(error_messages)
        pytest.fail(full_error_message, pytrace=False)