import sqlite3
import csv
import os
import re

# --- Configuration ---
DATABASE_FILE = 'jcps_school_data.db'  # Database file name
TABLE_NAME = 'schools'

# --- Calculate the correct path to the CSV file ---
# Get the directory where the script setup_database.py is located
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Go up one level to the project root directory (parent of 'app')
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
# Construct the full path to the CSV file inside the 'data' folder
CSV_FILE = os.path.join(PROJECT_ROOT, 'data', 'JCPS_Merged_Data.csv')
# --- End path calculation ---

# --- Column Mapping (Original CSV Header -> Sanitized DB Column) ---
# *** CRITICAL: Verify these CSV headers exactly match your downloaded file ***
COLUMN_MAPPING = {
    'School Code': 'school_code',
    'School Code Adjusted': 'school_code_adjusted', # PRIMARY KEY
    'School Name': 'school_name',
    'School Name Alternate': 'school_name_alternate',
    # 'Level' removed as requested
    'Type': 'type',
    'Zone': 'zone',
    'Feeder to High School': 'feeder_to_high_school',
    'Network': 'network',
    'Great Schools Rating': 'great_schools_rating', # Changed to INTEGER
    'Great Schools URL': 'great_schools_url',
    'School Level': 'school_level',
    'Enrollment': 'enrollment',
    'All Grades Enrollment': 'all_grades_enrollment',
    'Preschool Enrollment': 'preschool_enrollment',
    'All Grades Except Preschool Enrollment': 'all_grades_except_preschool_enrollment', # Changed to INTEGER
    'Elementary School Enrollment': 'elementary_school_enrollment',
    'Middle School Enrollment': 'middle_school_enrollment',
    'High School Enrollment': 'high_school_enrollment',
    'Student Teacher Ratio': 'student_teacher_ratio', # Original TEXT format (e.g., '15:01')
    # A new column 'student_teacher_ratio_value' (REAL) will be calculated
    'Grade': 'grade',
    'Mathematics - All Students Proficient or Distinguished': 'math_all_proficient_distinguished',
    'Mathematics - Economically Disadvantaged Proficient or Distinguished': 'math_econ_disadv_proficient_distinguished',
    'Mathematics - Non Economically Disadvantaged Proficient or Distinguished': 'math_non_econ_disadv_proficient_distinguished',
    'Reading - All Students Proficient or Distinguished': 'reading_all_proficient_distinguished',
    'Reading - Economically Disadvantaged Proficient or Distinguished': 'reading_econ_disadv_proficient_distinguished',
    'Reading - Non Economically Disadvantaged Proficient or Distinguished': 'reading_non_econ_disadv_proficient_distinguished', # Corrected typo
    'Total Ready': 'total_ready',
    'Teacher Average Years of Experience': 'teacher_avg_years_experience',
    'Percent of Teachers With 1-3 Years Experience': 'percent_teachers_1_3_years_experience',
    'Percent of Teachers With Less Than 1 Year Experience': 'percent_teachers_less_than_1_year_experience',
    'Percent of Teachers with 3 Years or Less of Experience': 'percent_teachers_3_years_or_less_experience',
    'Total Behavior Events': 'total_behavior_events',
    'Alcohol': 'behavior_events_alcohol',
    'Assault, 1st Degree': 'behavior_events_assault_1st_degree',
    'Drugs': 'behavior_events_drugs',
    'Harassment (Includes Bullying)': 'behavior_events_harassment',
    'Other Assault or Violence': 'behavior_events_other_assault_violence',
    'Other Events Resulting in State Resolutions': 'behavior_events_other_state_resolution',
    'Tobacco': 'behavior_events_tobacco',
    'Weapons': 'behavior_events_weapons',
    'Attendance Rate': 'attendance_rate',
    'Dropout Rate': 'dropout_rate',
    'Students Whose Parents Attended Teacher Conferences': 'parents_attended_conferences',
    'Parents Voted in SBDM Elections': 'parents_voted_sbdm',
    'Parents Who Served on SBDM or Committees': 'parents_served_sbdm_committee',
    'Volunteer Hours Contributed by Parents': 'parent_volunteer_hours',
    'School Website Link': 'school_website_link',
    'School Type': 'school_type_code',
    'Low Grade': 'low_grade',
    'High Grade': 'high_grade',
    'Title I Status': 'title_i_status',
    'Contact Name': 'contact_name',
    'Address': 'address',
    'Address2': 'address2',
    'PO Box': 'po_box',
    'City': 'city',
    'State': 'state',
    'Zipcode': 'zipcode',
    'Phone': 'phone',
    'Fax': 'fax',
    'Latitude': 'latitude',
    'Longitude': 'longitude'
}

# Define data types for cleaning/conversion (INTEGER or REAL)
# Use the SANITIZED database column names here
INTEGER_COLUMNS = {
    'great_schools_rating', # Moved to INTEGER
    'enrollment', 'all_grades_enrollment', 'preschool_enrollment',
    'all_grades_except_preschool_enrollment', # Moved to INTEGER
    'elementary_school_enrollment', 'middle_school_enrollment',
    'high_school_enrollment', 'total_ready', 'total_behavior_events',
    'behavior_events_alcohol', 'behavior_events_assault_1st_degree',
    'behavior_events_drugs', 'behavior_events_harassment',
    'behavior_events_other_assault_violence', 'behavior_events_other_state_resolution',
    'behavior_events_tobacco', 'behavior_events_weapons',
    'parents_attended_conferences', 'parents_voted_sbdm',
    'parents_served_sbdm_committee', 'parent_volunteer_hours'
}
REAL_COLUMNS = {
    'math_all_proficient_distinguished',
    'math_econ_disadv_proficient_distinguished', 'math_non_econ_disadv_proficient_distinguished',
    'reading_all_proficient_distinguished', 'reading_econ_disadv_proficient_distinguished',
    'reading_non_econ_disadv_proficient_distinguished', 'teacher_avg_years_experience',
    'percent_teachers_1_3_years_experience', 'percent_teachers_less_than_1_year_experience',
    'percent_teachers_3_years_or_less_experience', 'attendance_rate', 'dropout_rate',
    'latitude', 'longitude',
    'student_teacher_ratio_value' # Add the new calculated column here
}

# --- Helper Function for Cleaning ---
def clean_value(value, target_type):
    """Cleans and converts string value to target type (INTEGER or REAL), returns None on failure."""
    if value is None:
        return None
    # Standardize common non-numeric/placeholder values like '*' to empty string first
    value = str(value).strip()
    if value in ['*', 'N/A', 'n/a', '#VALUE!']:
        value = ''

    if not value: # Handle empty strings after cleaning placeholders
        return None

    try:
        if target_type == 'INTEGER':
            value = value.replace(',', '')
            return int(float(value))
        elif target_type == 'REAL':
            value = value.replace(',', '').replace('%', '')
            return float(value)
        else: # Assume TEXT
             return value
    except (ValueError, TypeError):
        # print(f"Warning: Could not convert '{value}' to {target_type}. Setting to NULL.")
        return None

# --- Helper Function for Ratio Calculation ---
def calculate_ratio_value(ratio_str):
    """Calculates a decimal value from a ratio string like '15:01'."""
    if not ratio_str or ':' not in ratio_str:
        return None
    try:
        students_str, teachers_str = ratio_str.split(':', 1)
        students = float(students_str.strip())
        teachers = float(teachers_str.strip())
        if teachers == 0:
            return None # Avoid division by zero
        return students / teachers
    except (ValueError, TypeError, IndexError):
        # print(f"Warning: Could not parse ratio '{ratio_str}'. Setting value to NULL.")
        return None

# --- Script Logic ---

# Ensure the CSV file exists
if not os.path.exists(CSV_FILE):
    print(f"Error: CSV file not found at '{CSV_FILE}'")
    exit()

# Delete existing database file if it exists
if os.path.exists(DATABASE_FILE):
    print(f"Removing existing database file: {DATABASE_FILE}")
    os.remove(DATABASE_FILE)

# Connect to the SQLite database
conn = sqlite3.connect(DATABASE_FILE)
cursor = conn.cursor()
print(f"Database file '{DATABASE_FILE}' created/connected.")

# Build the CREATE TABLE statement dynamically
columns_sql = []
primary_key_db_col = 'school_code_adjusted' # Define the primary key column name

# Add mapped columns first
for db_col in COLUMN_MAPPING.values():
    if db_col == primary_key_db_col:
        col_type = 'TEXT PRIMARY KEY NOT NULL'
    elif db_col in INTEGER_COLUMNS:
        col_type = 'INTEGER'
    elif db_col in REAL_COLUMNS and db_col != 'student_teacher_ratio_value': # Exclude calculated column for now
        col_type = 'REAL'
    else: # Includes TEXT columns like the original student_teacher_ratio
        col_type = 'TEXT'
    columns_sql.append(f'"{db_col}" {col_type}')

# Manually add the calculated ratio column definition
columns_sql.append(f'"student_teacher_ratio_value" REAL')

create_table_sql = f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" ({", ".join(columns_sql)})'

# Execute CREATE TABLE
try:
    cursor.execute(create_table_sql)
    print(f"Table '{TABLE_NAME}' created successfully with '{primary_key_db_col}' as PRIMARY KEY.")
    # print(create_table_sql) # Uncomment to see the exact SQL used
except sqlite3.Error as e:
    print(f"Error creating table: {e}")
    conn.close()
    exit()

# Read data from CSV and insert
inserted_count = 0
skipped_count = 0
print(f"Starting data import from '{CSV_FILE}'...")
try:
    with open(CSV_FILE, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)

        # Verify all mapped CSV headers exist in the actual file headers
        missing_headers = [csv_h for csv_h in COLUMN_MAPPING.keys() if csv_h not in csv_reader.fieldnames]
        if missing_headers:
             print("\n--- WARNING: Headers in COLUMN_MAPPING not found in CSV file ---")
             for h in missing_headers:
                 print(f" - '{h}'")
             print("--- These columns will be skipped or cause errors. Please check spelling/case in COLUMN_MAPPING or CSV file. ---\n")

        # Prepare for insertion - include the calculated column
        db_columns_mapped = list(COLUMN_MAPPING.values())
        db_columns_all = db_columns_mapped + ['student_teacher_ratio_value'] # Add the calculated column name
        placeholders = ', '.join(['?'] * len(db_columns_all))
        insert_sql = f'INSERT OR IGNORE INTO "{TABLE_NAME}" ({", ".join(f"{col}" for col in db_columns_all)}) VALUES ({placeholders})'
        # print(insert_sql) # Uncomment to verify INSERT statement

        # Find the CSV header for the primary key
        primary_key_csv_header = None
        for csv_h, db_c in COLUMN_MAPPING.items():
             if db_c == primary_key_db_col:
                  primary_key_csv_header = csv_h
                  break
        if not primary_key_csv_header:
             print(f"FATAL ERROR: Could not find CSV header for primary key '{primary_key_db_col}'.")
             conn.close()
             exit()

        # Find the CSV header for the ratio string
        ratio_csv_header = None
        for csv_h, db_c in COLUMN_MAPPING.items():
             if db_c == 'student_teacher_ratio':
                  ratio_csv_header = csv_h
                  break
        # No fatal error if ratio header not found, calculation will just be None

        for row_num, row in enumerate(csv_reader, start=1):
            values_to_insert = []
            primary_key_value_raw = row.get(primary_key_csv_header)
            primary_key_value_cleaned = str(primary_key_value_raw).strip() if primary_key_value_raw else None

            # Basic check: Primary key cannot be NULL/empty
            if not primary_key_value_cleaned:
                 print(f"Skipping row {row_num} due to missing or empty primary key ('{primary_key_csv_header}'). Raw row data: {row}")
                 skipped_count += 1
                 continue

            # Process values for mapped columns
            process_error = False
            ratio_str_raw = None # Store the raw ratio string for calculation later
            for db_col in db_columns_mapped:
                # Find corresponding CSV header
                csv_header = None
                for csv_h, db_c in COLUMN_MAPPING.items():
                    if db_c == db_col:
                        csv_header = csv_h
                        break
                if csv_header is None: continue # Should not happen if mapping is complete

                raw_value = row.get(csv_header)
                if db_col == 'student_teacher_ratio':
                    ratio_str_raw = raw_value # Save the raw string

                # Determine target type and clean
                target_type = 'TEXT'
                if db_col in INTEGER_COLUMNS: target_type = 'INTEGER'
                elif db_col in REAL_COLUMNS: target_type = 'REAL'
                cleaned = clean_value(raw_value, target_type)
                values_to_insert.append(cleaned)

            # Calculate and append the ratio value
            ratio_value_calculated = calculate_ratio_value(ratio_str_raw)
            values_to_insert.append(ratio_value_calculated)

            # Execute insert
            try:
                cursor.execute(insert_sql, tuple(values_to_insert))
                if cursor.rowcount > 0:
                     inserted_count += 1
                else:
                    print(f"Ignoring row {row_num} due to duplicate primary key ('{primary_key_csv_header}'): '{primary_key_value_cleaned}'")
                    skipped_count += 1
            except Exception as insert_error:
                print(f"--- ERROR inserting row {row_num} (PK: {primary_key_value_cleaned}) ---")
                print(f"Error: {insert_error}")
                # print(f"Values attempted: {values_to_insert}") # Uncomment if needed
                print("----------------------------------------------------------------")
                skipped_count += 1

    # Commit the changes
    conn.commit()
    print(f"\nData insertion complete. Inserted: {inserted_count}, Skipped/Ignored: {skipped_count}")

except FileNotFoundError:
    print(f"Error: Could not find the CSV file '{CSV_FILE}'")
except Exception as e:
    print(f"An error occurred during CSV processing: {e}")
finally:
    # Close the database connection
    conn.close()
    print("Database connection closed.")