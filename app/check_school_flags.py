# app/check_school_flags.py (New version for feeder school diagnostics)

import sqlite3
import os

# --- Configuration ---
DB_FILE = 'jcps_school_data.db'
TABLE_NAME = 'schools'

# --- The High School Feeder pattern we want to investigate ---
HIGH_SCHOOL_TO_CHECK = 'Seneca High'

def inspect_feeder_pattern():
    """
    Connects to the database and lists all elementary schools that
    feed into the specified high school.
    """
    db_path = os.path.join(os.path.dirname(__file__), DB_FILE)
    if not os.path.exists(db_path):
        print(f"--- FATAL ERROR: Database not found at '{db_path}' ---")
        return

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print(f"--- Feeder School Inspector ---")
    print(f"Querying for all Elementary Schools that feed into '{HIGH_SCHOOL_TO_CHECK}'...")

    sql = (f'SELECT display_name, school_code_adjusted FROM "{TABLE_NAME}" '
           f'WHERE feeder_to_high_school = ? AND school_level = ? '
           f'ORDER BY display_name ASC')
    
    try:
        cursor.execute(sql, (HIGH_SCHOOL_TO_CHECK, 'Elementary School'))
        results = cursor.fetchall()
    except sqlite3.Error as e:
        print(f"\n--- ❌ DATABASE ERROR ---")
        print(f"An error occurred while querying the database: {e}")
        conn.close()
        return

    if not results:
        print(f"\n--- ❌ No elementary feeder schools found for '{HIGH_SCHOOL_TO_CHECK}'. ---")
        print("This could be due to a misspelling of the high school name or a data issue.")
    else:
        print("\n--- Feeder Schools Found ---")
        for school in results:
            print(f"  - {school['display_name']} (SCA: {school['school_code_adjusted']})")

    conn.close()
    print("\n--- Inspection Complete ---")

if __name__ == '__main__':
    inspect_feeder_pattern()