import sqlite3
import csv
import os

# --- Configuration ---
# Point to the EXISTING database file
DATABASE_FILE = 'jcps_school_data.db'
TABLE_NAME = 'schools'

# --- New Data Configuration ---
NEW_DATA_CSV = 'school_report_links.csv' # Path to the new CSV
NEW_COLUMN_NAME = 'ky_reportcard_URL'   # Name for the new DB column
NEW_COLUMN_TYPE = 'TEXT'             # Data type for the new column

# --- CSV Headers for New Data ---
# *** Adjust these to match your principal_emails.csv headers ***
CSV_PK_COL = 'School Code Adjusted' # Column in CSV matching the DB Primary Key
CSV_NEW_DATA_COL = 'KYReportCard URL' # Column in CSV with the new data

# --- File Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')
NEW_DATA_CSV_PATH = os.path.join(DATA_DIR, NEW_DATA_CSV)
DB_FILE_PATH = os.path.join(SCRIPT_DIR, DATABASE_FILE) # Assuming DB is next to script

# --- Script Logic ---

# 1. Check if required files exist
if not os.path.exists(DB_FILE_PATH):
    print(f"ERROR: Database file not found at '{DB_FILE_PATH}'. Cannot update.")
    exit()
if not os.path.exists(NEW_DATA_CSV_PATH):
    print(f"ERROR: New data CSV file not found at '{NEW_DATA_CSV_PATH}'.")
    exit()

# 2. Connect to the EXISTING database
conn = sqlite3.connect(DB_FILE_PATH)
cursor = conn.cursor()
print(f"Connected to existing database '{DATABASE_FILE}'.")

# 3. Add the new column if it doesn't exist
try:
    print(f"Attempting to add column '{NEW_COLUMN_NAME}'...")
    cursor.execute(f'ALTER TABLE "{TABLE_NAME}" ADD COLUMN "{NEW_COLUMN_NAME}" {NEW_COLUMN_TYPE}')
    print(f"  Successfully added column '{NEW_COLUMN_NAME}'.")
    conn.commit() # Commit the schema change
except sqlite3.OperationalError as e:
    # This error usually means the column already exists, which is often okay
    if "duplicate column name" in str(e).lower():
        print(f"  Info: Column '{NEW_COLUMN_NAME}' already exists.")
    else:
        print(f"  ERROR during ALTER TABLE: {e}")
        conn.close()
        exit() # Exit on other ALTER errors

# 4. Load the new data from CSV into memory
new_data_dict = {}
print(f"\nLoading new data from '{NEW_DATA_CSV}'...")
try:
    with open(NEW_DATA_CSV_PATH, 'r', encoding='utf-8-sig') as file:
        reader = csv.DictReader(file)
        if CSV_PK_COL not in reader.fieldnames or CSV_NEW_DATA_COL not in reader.fieldnames:
             raise ValueError(f"Missing required headers '{CSV_PK_COL}' or '{CSV_NEW_DATA_COL}' in {NEW_DATA_CSV}")

        loaded_count = 0
        for row in reader:
            pk = row.get(CSV_PK_COL, '').strip()
            new_value = row.get(CSV_NEW_DATA_COL, '').strip()
            if pk: # Only store if primary key exists
                new_data_dict[pk] = new_value if new_value else None # Store None for empty emails
                loaded_count += 1
        print(f"  Loaded {loaded_count} records from CSV.")

except FileNotFoundError:
    print(f"  ERROR: Could not find the new data CSV file '{NEW_DATA_CSV_PATH}'")
    conn.close(); exit()
except ValueError as ve:
    print(f"  ERROR: Issue with CSV headers: {ve}")
    conn.close(); exit()
except Exception as e:
    print(f"  ERROR reading new data CSV: {e}")
    conn.close(); exit()

# 5. Update the database table
print(f"\nUpdating '{TABLE_NAME}' table...")
updated_count = 0
skipped_count = 0
primary_key_db_col = 'school_code_adjusted' # The actual PK column name in the DB

try:
    for pk_value, new_data_value in new_data_dict.items():
        # Use parameterized query for safety
        update_sql = f'UPDATE "{TABLE_NAME}" SET "{NEW_COLUMN_NAME}" = ? WHERE "{primary_key_db_col}" = ?'
        cursor.execute(update_sql, (new_data_value, pk_value))
        if cursor.rowcount > 0:
            updated_count += 1
        else:
            # This means no row with that primary key was found in the DB
            print(f"  Warning: Primary key '{pk_value}' from CSV not found in database table. Skipping update.")
            skipped_count += 1

    # Commit the updates
    conn.commit()
    print(f"Database update complete. Rows updated: {updated_count}, Updates skipped (PK not found): {skipped_count}")

except sqlite3.Error as e:
    print(f"  ERROR during database UPDATE: {e}")
    conn.rollback() # Rollback changes if error occurs during update loop
except Exception as e:
     print(f" An unexpected error occurred during update: {e}")
     conn.rollback()
finally:
    conn.close()
    print("Database connection closed.")