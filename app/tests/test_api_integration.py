# app/tests/test_api_integration.py

import pytest
import requests
import json
import os

# --- Configuration ---
API_URL = "http://localhost:5001/school-details-by-address"
TEST_CASES_PATH = os.path.join(os.path.dirname(__file__), 'test_cases.json')

# --- Helper to load test data ---
def load_test_cases():
    with open(TEST_CASES_PATH, 'r') as f:
        return json.load(f)

# --- NEW: Generate a list of clean IDs for each test case ---
# This reads your test data and creates a simple list of names for Pytest to use.
test_data = load_test_cases()
test_ids = [case.get('zone_name', f'test_case_{i}') for i, case in enumerate(test_data)]
# --- END NEW ---


# --- The Test Function ---
# MODIFIED: We pass the data and the new IDs to the decorator.
@pytest.mark.parametrize("test_case", test_data, ids=test_ids)
def test_school_zones_for_address(test_case):
    """
    For a given address, this test validates the API response against a known good list.
    """
    address = test_case['address']
    zone_name = test_case['zone_name']
    
    # 1. Make the request to the local API
    response = requests.post(API_URL, json={"address": address})
    assert response.status_code == 200, f"API returned a non-200 status for {zone_name}"
    
    api_data = response.json()
    
    # 2. Flatten the API response for easier comparison
    api_schools = {}
    for zone in api_data.get("results_by_zone", []):
        for school in zone.get("schools", []):
            api_schools[school['display_name']] = school['display_status']

    # 3. Validate against the expected schools
    for level, expected_list in test_case["expected_schools"].items():
        for expected_school in expected_list:
            name = expected_school['display_name']
            status = expected_school['expected_status']
            
            assert name in api_schools, f"'{name}' was missing for {zone_name} ({level})"
            
            assert api_schools[name] == status, f"'{name}' had wrong status. Got '{api_schools[name]}', expected '{status}' for {zone_name}"