# jcps_gis_api/scripts/update_greatschools_data.py

import pandas as pd
import sqlite3
import os

# --- Configuration ---
# Assumes this script is run from the root of the `jcps_gis_api` project.
CSV_SOURCE_FILE = os.path.join('data', 'great_schools_update.csv')
DATABASE_FILE = os.path.join('app', 'jcps_school_data.db')
TABLE_NAME = 'schools'

# --- Column Mapping ---
# Maps the Excel column names to the database column names
COLUMN_MAPPING = {
    'School Code Adjusted': 'school_code_adjusted',
    'Great Schools Rating': 'great_schools_rating',
    'Great Schools URL': 'great_schools_url'
}

def main():
    """Reads the update CSV and applies the changes to the SQLite database."""
    print("--- Starting Great Schools Data Update Script ---")

    # 1. Validate that the required files exist
    if not os.path.exists(CSV_SOURCE_FILE):
        print(f"❌ ERROR: Source CSV file not found at '{CSV_SOURCE_FILE}'")
        return
    if not os.path.exists(DATABASE_FILE):
        print(f"❌ ERROR: Database not found at '{DATABASE_FILE}'")
        return

    # 2. Load the update data from the CSV using pandas
    try:
        df_updates = pd.read_csv(CSV_SOURCE_FILE, dtype={'School Code Adjusted': str})
        
        # Remove any unnamed columns (trailing commas in CSV)
        unnamed_cols = [col for col in df_updates.columns if col.startswith('Unnamed:')]
        if unnamed_cols:
            df_updates = df_updates.drop(columns=unnamed_cols)
            print(f"⚠️ Removed {len(unnamed_cols)} unnamed column(s) from CSV")
        
        # Rename columns to match database schema for easier processing
        df_updates.rename(columns=COLUMN_MAPPING, inplace=True)
        print(f"✅ Successfully loaded {len(df_updates)} records from '{CSV_SOURCE_FILE}'.")
    except Exception as e:
        print(f"❌ ERROR: Could not read or process the CSV file. Error: {e}")
        return

    # 3. Connect to the database and perform the updates
    updated_count = 0
    skipped_count = 0
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        cursor = conn.cursor()

        print("\n--- Applying updates to the database... ---")
        
        # Iterate through each row in our update data
        for index, row in df_updates.iterrows():
            school_code = row['school_code_adjusted']
            new_rating = row['great_schools_rating']
            new_url = row['great_schools_url']

            # Make sure we have a valid school code to work with
            if not school_code or pd.isna(school_code):
                print(f"⚠️ WARNING: Skipping row {index + 2} due to missing School Code Adjusted.")
                skipped_count += 1
                continue

            # Construct the SQL UPDATE statement
            sql = f'''
                UPDATE "{TABLE_NAME}"
                SET 
                    great_schools_rating = ?,
                    great_schools_url = ?
                WHERE 
                    school_code_adjusted = ?
            '''
            
            # Execute the update for the current school
            cursor.execute(sql, (new_rating, new_url, school_code))

            if cursor.rowcount > 0:
                updated_count += 1
            else:
                print(f"  -> INFO: School code '{school_code}' not found in the database. Skipping.")
                skipped_count += 1

        # Commit all the changes at once
        conn.commit()
        print("\n--- ✅ Database Update Complete ---")

    except sqlite3.Error as e:
        print(f"❌ DATABASE ERROR: An error occurred during the update. Rolling back changes. Error: {e}")
        if 'conn' in locals() and conn:
            conn.rollback()
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("Database connection closed.")

    print("\n--- Summary ---")
    print(f"Rows processed: {len(df_updates)}")
    print(f"  - ✅ Rows successfully updated: {updated_count}")
    print(f"  - ⚠️ Rows skipped (e.g., code not found): {skipped_count}")


if __name__ == "__main__":
    main()