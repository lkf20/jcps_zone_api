# app/check_school_flags.py (New version to inspect flags for a specific school)

import sqlite3
import os

# --- Configuration ---
DB_FILE = 'jcps_school_data.db'
TABLE_NAME = 'schools'

# --- List of schools we want to investigate ---
# This is the school that is showing up incorrectly.
SCHOOLS_TO_CHECK = [
    'Meyzeek Middle'
]

# --- List of all the flags our "Decision Engine" uses for Middle Schools ---
# We will check the exact value of each of these columns.
FLAGS_TO_CHECK = [
    'universal_magnet_traditional_school',
    'universal_magnet_traditional_program',
    'universal_academies_or_other',
    'magnet_programs' # We'll check this one too, just in case
]

def inspect_database():
    """
    Connects to the database and prints the exact flag values for our target schools.
    """
    db_path = os.path.join(os.path.dirname(__file__), DB_FILE)
    if not os.path.exists(db_path):
        print(f"--- FATAL ERROR: Database not found at '{db_path}' ---")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("--- Database Flag Inspector ---")
    print(f"Querying for {len(SCHOOLS_TO_CHECK)} school(s)...")

    # Construct the query
    where_clause = ' OR '.join(['"display_name" = ?'] * len(SCHOOLS_TO_CHECK))
    select_cols = ", ".join(f'"{col}"' for col in ['display_name', 'school_level'] + FLAGS_TO_CHECK)
    
    sql = f'SELECT {select_cols} FROM "{TABLE_NAME}" WHERE {where_clause}'

    try:
        cursor.execute(sql, tuple(SCHOOLS_TO_CHECK))
        results = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"\n--- ❌ DATABASE ERROR ---")
        print(f"An error occurred while querying the database: {e}")
        print("This likely means one of the column names in FLAGS_TO_CHECK is misspelled or does not exist.")
        conn.close()
        return

    if not results:
        print("\n--- ❌ No matching schools found in the database. ---")
        print("This could be due to a misspelling in the SCHOOLS_TO_CHECK list.")
        
    for school in results:
        print("\n------------------------------------")
        print(f"🔍 Checking: {school['display_name']} (Level: {school['school_level']})")
        print("------------------------------------")
        for flag in FLAGS_TO_CHECK:
            value = school[flag]
            print(f"  - Flag: '{flag}'")
            print(f"    Value in DB: [{value}] (Type: {type(value).__name__})")
            if value == 'Yes':
                print("    ❗️ POTENTIAL ISSUE: This flag is 'Yes'. This is likely why the school is being included when it shouldn't be.")

    conn.close()
    print("\n--- Inspection Complete ---")

if __name__ == '__main__':
    inspect_database()