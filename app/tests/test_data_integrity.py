# app/tests/test_data_integrity.py

import pytest
import sqlite3
import json
import os

# --- Configuration ---
DATABASE_PATH = os.path.join(os.path.dirname(__file__), '..', 'jcps_school_data.db')
TEST_DATA_PATH = os.path.join(os.path.dirname(__file__), 'school_data_validation_tests.json')
DB_SCHOOLS_TABLE = 'schools'

# --- Helper Functions ---

def load_test_cases():
    """Loads the golden record data from the JSON file."""
    with open(TEST_DATA_PATH, 'r') as f:
        return json.load(f)

def get_school_data_from_db(school_code, fields):
    """Fetches specific fields for a single school from the database."""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Create a string of fields safe for SQL, e.g., '"field1", "field2"'
    select_fields = ", ".join(f'"{field}"' for field in fields)
    
    query = f'SELECT {select_fields} FROM "{DB_SCHOOLS_TABLE}" WHERE school_code_adjusted = ?'
    cursor.execute(query, (school_code,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return dict(result)
    return None

# --- Pytest Setup ---

# Load test cases and create IDs for clearer test reports
test_cases = load_test_cases()
test_ids = [case.get('display_name', f"SCA-{case.get('school_code_adjusted')}") for case in test_cases]

# --- The Test Function ---

@pytest.mark.parametrize("test_case", test_cases, ids=test_ids)
def test_school_data_validation(test_case):
    """
    Validates specific data points for a school against the golden record.
    """
    school_code = test_case['school_code_adjusted']
    expected_values = test_case['expected_values']
    
    # Get the actual data from the database for the fields we want to test
    actual_data = get_school_data_from_db(school_code, expected_values.keys())
    
    assert actual_data is not None, f"School with code '{school_code}' not found in the database."
    
    # Compare each expected value with the actual value from the database
    for field, expected_value in expected_values.items():
        actual_value = actual_data.get(field)
        
        # Create a helpful error message for failures
        error_msg = (
            f"Mismatch for '{test_case['display_name']}' in field '{field}'. "
            f"Expected: '{expected_value}' (type: {type(expected_value).__name__}), "
            f"Got: '{actual_value}' (type: {type(actual_value).__name__})"
        )
        
        # Use pytest.approx for numbers to handle floating point differences
        if isinstance(expected_value, (int, float)):
            assert actual_value == pytest.approx(expected_value), error_msg
        else:
            # For strings, we can strip whitespace for a more robust comparison
            if isinstance(actual_value, str):
                actual_value = actual_value.strip()
            if isinstance(expected_value, str):
                expected_value = expected_value.strip()
            
            assert actual_value == expected_value, error_msg