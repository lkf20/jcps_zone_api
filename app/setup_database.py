import sqlite3
import csv
import os
import re

# --- Configuration ---
DATABASE_FILE = 'jcps_school_data.db'
TABLE_NAME = 'schools'

# --- File Paths ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
DATA_DIR = os.path.join(PROJECT_ROOT, 'data')

MAIN_CSV_FILE = os.path.join(DATA_DIR, 'JCPS_Merged_Data.csv')
# LINK_CSV_FILE = os.path.join(DATA_DIR, 'school_report_links.csv') # REMOVED

# --- Column Mapping (Main CSV Header -> Sanitized DB Column) ---
# *** Verify these CSV headers exactly match JCPS_Merged_Data.csv ***
# *** Includes display_name, gis_name, universal_magnet, KYReportCard URL ***
COLUMN_MAPPING = {
    'School Code': 'school_code',
    'School Code Adjusted': 'school_code_adjusted', # PRIMARY KEY
    'School Name': 'school_name',
    'School Name Alternate': 'school_name_alternate',
    'Display Name': 'display_name', # Assumed header in CSV
    'GIS Name': 'gis_name',         # Assumed header in CSV
    'Universal Magnet Traditional': 'universal_magnet', # Assumed header in CSV
    'Type': 'type',
    'Zone': 'zone',
    'Feeder to High School': 'feeder_to_high_school',
    'Network': 'network',
    'Great Schools Rating': 'great_schools_rating', # INTEGER
    'Great Schools URL': 'great_schools_url',
    'KYReportCard URL': 'ky_reportcard_URL', # <<< ADDED MAPPING FOR LINK
    'School Level': 'school_level',
    'Enrollment': 'enrollment',
    'All Grades Enrollment': 'all_grades_enrollment',
    'Preschool Enrollment': 'preschool_enrollment',
    'All Grades Except Preschool Enrollment': 'all_grades_except_preschool_enrollment', # INTEGER
    'Elementary School Enrollment': 'elementary_school_enrollment',
    'Middle School Enrollment': 'middle_school_enrollment',
    'High School Enrollment': 'high_school_enrollment',
    'Student Teacher Ratio': 'student_teacher_ratio', # TEXT format '15:01'
    # 'student_teacher_ratio_value' (REAL) calculated below
    'Grade': 'grade',
    'Mathematics - All Students Proficient or Distinguished': 'math_all_proficient_distinguished',
    'Mathematics - Economically Disadvantaged Proficient or Distinguished': 'math_econ_disadv_proficient_distinguished',
    'Mathematics - Non Economically Disadvantaged Proficient or Distinguished': 'math_non_econ_disadv_proficient_distinguished',
    'Reading - All Students Proficient or Distinguished': 'reading_all_proficient_distinguished',
    'Reading - Economically Disadvantaged Proficient or Distinguished': 'reading_econ_disadv_proficient_distinguished',
    'Reading - Non Economically Disadvantaged Proficient or Distinguished': 'reading_non_econ_disadv_proficient_distinguished',
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
    # 'student_teacher_ratio_value' added dynamically
    # 'ky_reportcard_URL' is now included via mapping above
}

# --- Column Types for Cleaning ---
# Use SANITIZED db column names
INTEGER_COLUMNS = {
    'great_schools_rating', # INTEGER
    'all_grades_except_preschool_enrollment', # INTEGER
    'enrollment', 'all_grades_enrollment', 'preschool_enrollment',
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
    # 'student_teacher_ratio_value' - Defined as REAL later
}
# TEXT_COLUMNS = Everything else in COLUMN_MAPPING + student_teacher_ratio + calculated columns

# --- Helper Functions ---
def clean_value(value, target_type):
    if value is None: return None
    value = str(value).strip()
    if value in ['*', 'N/A', 'n/a', '#VALUE!']: value = ''
    if not value: return None
    try:
        if target_type == 'INTEGER': return int(float(value.replace(',', '')))
        elif target_type == 'REAL': return float(value.replace(',', '').replace('%', ''))
        else: return value # TEXT
    except (ValueError, TypeError): return None

def calculate_ratio_value(ratio_str):
    if not ratio_str or ':' not in ratio_str: return None
    try:
        s, t = map(str.strip, ratio_str.split(':', 1))
        return float(s) / float(t) if float(t) != 0 else None
    except (ValueError, TypeError, IndexError): return None

# --- Pre-load Report Links REMOVED ---
# report_links_dict = {} # REMOVED

# --- Database Setup ---
print("\nSetting up database...")
if not os.path.exists(MAIN_CSV_FILE): print(f"Error: Main CSV not found at '{MAIN_CSV_FILE}'"); exit()
DB_FILE_PATH = os.path.join(SCRIPT_DIR, DATABASE_FILE) # DB next to script

if os.path.exists(DB_FILE_PATH): print(f"Removing existing database file: {DB_FILE_PATH}"); os.remove(DB_FILE_PATH)

conn = sqlite3.connect(DB_FILE_PATH); cursor = conn.cursor()
print(f"Database file '{DB_FILE_PATH}' created/connected.")

# --- Build CREATE TABLE statement ---
db_columns_final_set = set(COLUMN_MAPPING.values())
db_columns_final_set.add('student_teacher_ratio_value') # Add calculated column
# report_link_url is now included via COLUMN_MAPPING ('ky_reportcard_URL')

columns_sql_defs = []
primary_key_db_col = 'school_code_adjusted'

for db_col in sorted(list(db_columns_final_set)): # Sort for consistent order
    col_type = 'TEXT' # Default
    if db_col == primary_key_db_col: col_type = 'TEXT PRIMARY KEY NOT NULL'
    elif db_col == 'student_teacher_ratio_value': col_type = 'REAL'
    # Removed specific check for report_link_url, handled by mapping
    elif db_col in INTEGER_COLUMNS: col_type = 'INTEGER'
    elif db_col in REAL_COLUMNS: col_type = 'REAL'
    columns_sql_defs.append(f'"{db_col}" {col_type}')

create_table_sql = f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" ({", ".join(columns_sql_defs)})'

try: cursor.execute(create_table_sql); print(f"Table '{TABLE_NAME}' created successfully.")
except sqlite3.Error as e: print(f"Error creating table: {e}"); conn.close(); exit()

# --- Create Indexes ---
print("Creating database indexes...")
index_commands = [
    f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_gis_name ON "{TABLE_NAME}" (gis_name);',
    f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_display_name ON "{TABLE_NAME}" (display_name);',
    f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_feeder_hs ON "{TABLE_NAME}" (feeder_to_high_school);',
    f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_school_level ON "{TABLE_NAME}" (school_level);',
    f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_universal ON "{TABLE_NAME}" (universal_magnet);',
    f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_feeder_level ON "{TABLE_NAME}" (feeder_to_high_school, school_level);'
]
try: [cursor.execute(cmd) for cmd in index_commands]; conn.commit(); print("Database indexes created.")
except sqlite3.Error as e: print(f"Error creating indexes: {e}")

# --- Import Main Data ---
inserted_count = 0; skipped_count = 0
print(f"\nStarting data import from '{MAIN_CSV_FILE}'...")
try:
    with open(MAIN_CSV_FILE, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        missing_headers = [h for h in COLUMN_MAPPING.keys() if h not in csv_reader.fieldnames]
        if missing_headers: print(f"WARNING: Headers missing in main CSV: {missing_headers}")

        # Get final list of DB columns in a defined order (mapped + calculated)
        db_columns_ordered = sorted(list(db_columns_final_set))
        placeholders = ', '.join(['?'] * len(db_columns_ordered))
        insert_sql = f'INSERT OR IGNORE INTO "{TABLE_NAME}" ({", ".join(f"{col}" for col in db_columns_ordered)}) VALUES ({placeholders})'

        # Find CSV header for PK
        primary_key_csv_header = next((csv_h for csv_h, db_c in COLUMN_MAPPING.items() if db_c == primary_key_db_col), None)
        if not primary_key_csv_header: raise ValueError(f"Cannot find CSV header for PK '{primary_key_db_col}'")

        # Find CSV header for ratio string
        ratio_csv_header = next((csv_h for csv_h, db_c in COLUMN_MAPPING.items() if db_c == 'student_teacher_ratio'), None)

        for row_num, row in enumerate(csv_reader, start=1):
            values_map = {}
            pk_value_raw = row.get(primary_key_csv_header)
            pk_value_cleaned = str(pk_value_raw).strip() if pk_value_raw else None

            if not pk_value_cleaned: print(f"Skipping row {row_num} due to missing PK"); skipped_count += 1; continue

            # Process mapped columns from CSV
            ratio_str_for_calc = None
            for csv_header, db_col in COLUMN_MAPPING.items():
                raw_value = row.get(csv_header) # Use get() for safety
                if db_col == 'student_teacher_ratio': ratio_str_for_calc = raw_value
                target_type = 'TEXT'
                if db_col in INTEGER_COLUMNS: target_type = 'INTEGER'
                elif db_col in REAL_COLUMNS: target_type = 'REAL'
                values_map[db_col] = clean_value(raw_value, target_type)

            # Add calculated value
            values_map['student_teacher_ratio_value'] = calculate_ratio_value(ratio_str_for_calc)

            # Prepare tuple in the correct order for INSERT
            values_to_insert = [values_map.get(db_col) for db_col in db_columns_ordered]

            # Execute insert
            try:
                cursor.execute(insert_sql, tuple(values_to_insert))
                if cursor.rowcount > 0: inserted_count += 1
                else: skipped_count += 1 # Ignored duplicate PK
            except Exception as insert_error: print(f"ERROR inserting row {row_num} (PK: {pk_value_cleaned}): {insert_error}"); skipped_count += 1

    conn.commit()
    print(f"\nData insertion complete. Inserted: {inserted_count}, Skipped/Ignored: {skipped_count}")

except FileNotFoundError: print(f"Error: Could not find the main CSV file '{MAIN_CSV_FILE}'")
except ValueError as ve: print(f"ValueError during setup: {ve}")
except Exception as e: print(f"An error occurred during main CSV processing: {e}")
finally: conn.close(); print("Database connection closed.")