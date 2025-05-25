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

# --- Column Mapping (Main CSV Header -> Sanitized DB Column) ---
# *** Verify these CSV headers exactly match JCPS_Merged_Data.csv ***
COLUMN_MAPPING = {
    # Core Identifiers
    'School Code': 'school_code',
    'School Code Adjusted': 'school_code_adjusted', # PRIMARY KEY
    'JCPS School Code': 'jcps_school_code',
    'School Name': 'school_name',
    'School Name Alternate': 'school_name_alternate',
    'Display Name': 'display_name',
    'GIS Name': 'gis_name',

    # Type & Structure
    'Level': 'level',
    'Type': 'type',
    'Zone': 'zone',
    'Feeder to High School': 'feeder_to_high_school',
    'Network': 'network',
    'School Level': 'school_level',
    'Grade': 'grade',
    'Low Grade': 'low_grade',
    'High Grade': 'high_grade',
    'School Type': 'school_type_code',
    'Universal Magnet Traditional': 'universal_magnet',

    # Ratings & Links
    'Great Schools Rating': 'great_schools_rating', # INTEGER
    'Great Schools URL': 'great_schools_url',
    'KYReportCard URL': 'ky_reportcard_URL',
    'School Website Link': 'school_website_link',

    # Enrollment & Ratio
    'Enrollment': 'enrollment',
    'Membership': 'membership', 
    'All Grades Enrollment': 'all_grades_enrollment',
    'Preschool Membership': 'preschool_membership',
    'All Grades Except Preschool Enrollment': 'all_grades_except_preschool_enrollment', # INTEGER
    'Elementary School Membership': 'elementary_school_membership',
    'Middle School Membership': 'middle_school_membership',
    'High School Membership': 'high_school_membership',
    'Student Teacher Ratio': 'student_teacher_ratio', # TEXT format '15:01'
    # 'student_teacher_ratio_value' (REAL) calculated
    'All Grades With Preschool Membership': 'all_grades_with_preschool_membership',
    'All Grades Membership':'all_grades_membership',

    # <<< ADDED START/END TIME MAPPING >>>
    'Start Time': 'start_time', # TEXT
    'End Time': 'end_time',     # TEXT

    # Diversity & Demographics (%)
    'White %': 'white_percent',
    'African American %': 'african_american_percent',
    'Hispanic %': 'hispanic_percent',
    'Asian %': 'asian_percent',
    'Two or More Races %': 'two_or_more_races_percent',
    'Economically Disadvantaged %': 'economically_disadvantaged_percent',

    # Academic Proficiency (%)
    'Mathematics - All Students Proficient or Distinguished': 'math_all_proficient_distinguished',
    'Mathematics - Economically Disadvantaged Proficient or Distinguished': 'math_econ_disadv_proficient_distinguished',
    'Mathematics - Non Economically Disadvantaged Proficient or Distinguished': 'math_non_econ_disadv_proficient_distinguished',
    'Reading - All Students Proficient or Distinguished': 'reading_all_proficient_distinguished',
    'Reading - Economically Disadvantaged Proficient or Distinguished': 'reading_econ_disadv_proficient_distinguished',
    'Reading - Non Economically Disadvantaged Proficient or Distinguished': 'reading_non_econ_disadv_proficient_distinguished',

    # Other Metrics
    'Total Ready': 'total_ready',
    'Gifted Talented': 'gifted_talented_percent', # Assuming % value -> REAL
    'Attendance Rate': 'attendance_rate',
    'Dropout Rate': 'dropout_rate',
    'Parent Satisfaction': 'parent_satisfaction', # Assuming % value -> REAL

    # Teacher Experience
    'Teacher Average Years of Experience': 'teacher_avg_years_experience',
    'Percent of Teachers With 1-3 Years Experience': 'percent_teachers_1_3_years_experience',
    'Percent of Teachers With Less Than 1 Year Experience': 'percent_teachers_less_than_1_year_experience',
    'Percent of Teachers with 3 Years or Less of Experience': 'percent_teachers_3_years_or_less_experience',

    # Behavior/Discipline (Counts)
    'Total Behavior Events': 'total_behavior_events',
    'Alcohol': 'behavior_events_alcohol',
    'Assault, 1st Degree': 'behavior_events_assault_1st_degree',
    'Drugs': 'behavior_events_drugs',
    'Harassment (Includes Bullying)': 'behavior_events_harassment',
    'Other Assault or Violence': 'behavior_events_other_assault_violence',
    'Other Events Resulting in State Resolutions': 'behavior_events_other_state_resolution',
    'Tobacco': 'behavior_events_tobacco',
    'Weapons': 'behavior_events_weapons',
    'Total Assault Or Weapons': 'total_assault_weapons',
    'Total Discipline Resolutions': 'total_discipline_resolutions',
    'Percent Disciplinary Resolutions': 'percent_disciplinary_resolutions', 
    'Corporal Punishment (SSP5)': 'corporal_punishment_ssp5',
    'Restraint (SSP7)': 'restraint_ssp7',
    'Seclusion (SSP8)': 'seclusion_ssp8',
    'Expelled, Not Receiving Services (SSP2)': 'expelled_not_receiving_ssp2',
    'Expelled, Receiving Services (SSP1)': 'expelled_receiving_ssp1',
    'In-School Removal (INSR) Or In-District Removal (INDR) >=.5': 'in_school_removal_indr',
    'Out-Of-School Suspensions (SSP3)': 'out_of_school_suspensions_ssp3',
    'Removal By Hearing Officer (IAES2)': 'removal_hearing_officer_iaes2',
    'Unilateral Removal By School Personnel (IAES1)': 'unilateral_removal_iaes1',

    # Behavior/Discipline (%)
    'Percent_Drugs': 'percent_drugs',
    'Percent_Assault': 'percent_assault',
    'Percent_Total_Behavior': 'percent_total_behavior',

    # Parent Involvement
    'Students Whose Parents Attended Teacher Conferences': 'parents_attended_conferences',
    'Parents Voted in SBDM Elections': 'parents_voted_sbdm',
    'Parents Who Served on SBDM or Committees': 'parents_served_sbdm_committee',
    'Volunteer Hours Contributed by Parents': 'parent_volunteer_hours',
    'PTA Membership Percent': 'pta_membership_percent',

    # Contact & Address
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

# --- Column Types for Cleaning ---
# Use SANITIZED db column names
INTEGER_COLUMNS = {
    'great_schools_rating', 'all_grades_except_preschool_enrollment',
    'enrollment', 'all_grades_enrollment', 'membership', 'preschool_membership',
    'elementary_school_membership', 'middle_school_membership',
    'high_school_membership', 'total_ready', 'total_behavior_events',
    'behavior_events_alcohol', 'behavior_events_assault_1st_degree',
    'behavior_events_drugs', 'behavior_events_harassment',
    'behavior_events_other_assault_violence', 'behavior_events_other_state_resolution',
    'behavior_events_tobacco', 'behavior_events_weapons',
    'total_assault_weapons', 'total_discipline_resolutions',
    'corporal_punishment_ssp5', 'restraint_ssp7', 'seclusion_ssp8',
    'expelled_not_receiving_ssp2', 'expelled_receiving_ssp1',
    'in_school_removal_indr', 'out_of_school_suspensions_ssp3',
    'removal_hearing_officer_iaes2', 'unilateral_removal_iaes1',
    'parents_attended_conferences', 'parents_voted_sbdm',
    'parents_served_sbdm_committee', 'parent_volunteer_hours','all_grades_membership', 'all_grades_with_preschool_membership'
}
REAL_COLUMNS = {
    'math_all_proficient_distinguished', 'math_econ_disadv_proficient_distinguished',
    'math_non_econ_disadv_proficient_distinguished', 'reading_all_proficient_distinguished',
    'reading_econ_disadv_proficient_distinguished', 'reading_non_econ_disadv_proficient_distinguished',
    'teacher_avg_years_experience', 'percent_teachers_1_3_years_experience',
    'percent_teachers_less_than_1_year_experience', 'percent_teachers_3_years_or_less_experience',
    'attendance_rate', 'dropout_rate', 'latitude', 'longitude',
    'white_percent', 'african_american_percent', 'hispanic_percent', 'asian_percent',
    'two_or_more_races_percent', 'economically_disadvantaged_percent',
    'percent_drugs', 'percent_assault', 'percent_total_behavior',
    'gifted_talented_percent', 'pta_membership_percent',
    'parent_satisfaction', 'percent_disciplinary_resolutions'# Assuming REAL
    # 'student_teacher_ratio_value' - Added later
}
# TEXT_COLUMNS include start_time, end_time, student_teacher_ratio, etc.

# --- Helper Functions ---
def clean_value(value, target_type):
    # ... (Keep clean_value function exactly as before) ...
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
    # ... (Keep calculate_ratio_value function exactly as before) ...
    if not ratio_str or ':' not in ratio_str: return None
    try: s, t = map(str.strip, ratio_str.split(':', 1)); return float(s) / float(t) if float(t) != 0 else None
    except (ValueError, TypeError, IndexError): return None

# --- Database Setup ---
print("\nSetting up database...")
if not os.path.exists(MAIN_CSV_FILE): print(f"Error: Main CSV not found at '{MAIN_CSV_FILE}'"); exit()
DB_FILE_PATH = os.path.join(SCRIPT_DIR, DATABASE_FILE)

if os.path.exists(DB_FILE_PATH): print(f"Removing existing database file: {DB_FILE_PATH}"); os.remove(DB_FILE_PATH)

conn = sqlite3.connect(DB_FILE_PATH); cursor = conn.cursor()
print(f"Database file '{DB_FILE_PATH}' created/connected.")

# --- Build CREATE TABLE statement ---
db_columns_final_set = set(COLUMN_MAPPING.values())
db_columns_final_set.add('student_teacher_ratio_value') # Add calculated column
# ky_reportcard_URL is now included via COLUMN_MAPPING

columns_sql_defs = []
primary_key_db_col = 'school_code_adjusted'

for db_col in sorted(list(db_columns_final_set)):
    col_type = 'TEXT' # Default
    if db_col == primary_key_db_col: col_type = 'TEXT PRIMARY KEY NOT NULL'
    elif db_col == 'display_name': col_type = 'TEXT NOT NULL' # Assume essential
    elif db_col == 'student_teacher_ratio_value': col_type = 'REAL'
    elif db_col in INTEGER_COLUMNS: col_type = 'INTEGER'
    elif db_col in REAL_COLUMNS: col_type = 'REAL'
    # All others (including start_time, end_time) default to TEXT
    columns_sql_defs.append(f'"{db_col}" {col_type}')

create_table_sql = f'CREATE TABLE IF NOT EXISTS "{TABLE_NAME}" ({", ".join(columns_sql_defs)})'

try: cursor.execute(create_table_sql); print(f"Table '{TABLE_NAME}' created successfully.")
except sqlite3.Error as e: print(f"Error creating table: {e}"); conn.close(); exit()

# --- Create Indexes ---
print("Creating database indexes...")
# Define columns to index based on lookups and priorities
index_cols = [
    'gis_name', 'display_name', 'feeder_to_high_school', 'school_level',
    'universal_magnet', 'membership', 'great_schools_rating', 'parent_satisfaction',
    'math_all_proficient_distinguished', 'reading_all_proficient_distinguished',
    'white_percent', 'african_american_percent', 'hispanic_percent', 'asian_percent',
    'two_or_more_races_percent', 'economically_disadvantaged_percent',
    'gifted_talented_percent', 'percent_teachers_3_years_or_less_experience',
    'teacher_avg_years_experience', 'pta_membership_percent',
    'percent_total_behavior', 'total_assault_weapons',
    'start_time', 'end_time' # <<< Added start/end time indexes
]
# Add composite index
index_commands = [f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_feeder_level ON "{TABLE_NAME}" (feeder_to_high_school, school_level);']
# Add single column indexes
for col in index_cols:
    index_commands.append(f'CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_{col} ON "{TABLE_NAME}" ("{col}");')

try: [cursor.execute(cmd) for cmd in index_commands]; conn.commit(); print("Database indexes created.")
except sqlite3.Error as e: print(f"Error creating indexes: {e}")

# --- Import Main Data ---
inserted_count = 0; skipped_count = 0
print(f"\nStarting data import from '{MAIN_CSV_FILE}'...")
try:
    with open(MAIN_CSV_FILE, 'r', encoding='utf-8-sig') as file:
        csv_reader = csv.DictReader(file)
        # Verify essential headers exist
        required_headers = list(COLUMN_MAPPING.keys())
        missing_headers = [h for h in required_headers if h not in csv_reader.fieldnames]
        if missing_headers:
            print(f"--- FATAL ERROR: Required headers missing in '{MAIN_CSV_FILE}' ---")
            for h in missing_headers: print(f" - '{h}'")
            print("--- Please check the CSV file headers against COLUMN_MAPPING. ---")
            conn.close(); exit()

        # Get final list of DB columns in a defined order
        db_columns_ordered = sorted(list(db_columns_final_set))
        placeholders = ', '.join(['?'] * len(db_columns_ordered))
        insert_sql = f'INSERT OR IGNORE INTO "{TABLE_NAME}" ({", ".join(f"{col}" for col in db_columns_ordered)}) VALUES ({placeholders})'

        primary_key_csv_header = 'School Code Adjusted'
        ratio_csv_header = 'Student Teacher Ratio'

        row_count_in_csv = 0
        for row_num, row in enumerate(csv_reader, start=1):
            row_count_in_csv += 1
            values_map = {}
            pk_value_raw = row.get(primary_key_csv_header)
            pk_value_cleaned = str(pk_value_raw).strip() if pk_value_raw else None

            if not pk_value_cleaned: print(f"Skipping CSV row {row_num} due to missing PK"); skipped_count += 1; continue

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

            # Ensure all DB columns have a value (even if None) before creating tuple
            for db_col_name in db_columns_ordered:
                if db_col_name not in values_map:
                    values_map[db_col_name] = None

            # Prepare tuple in the correct order
            try: values_to_insert = [values_map[db_col] for db_col in db_columns_ordered]
            except KeyError as ke: print(f"ERROR preparing insert tuple row {row_num} (PK: {pk_value_cleaned}): Missing key {ke}"); skipped_count += 1; continue

            # Execute insert
            try:
                cursor.execute(insert_sql, tuple(values_to_insert))
                if cursor.rowcount > 0: inserted_count += 1
                else: skipped_count += 1 # Ignored duplicate PK
            except sqlite3.Error as insert_error: print(f"DB ERROR inserting row {row_num} (PK: {pk_value_cleaned}): {insert_error}"); skipped_count += 1
            except Exception as general_error: print(f"GENERAL ERROR inserting row {row_num} (PK: {pk_value_cleaned}): {general_error}"); skipped_count += 1

    conn.commit()
    print(f"\nData insertion complete. Processed CSV Rows: {row_count_in_csv}, Inserted New: {inserted_count}, Skipped/Ignored: {skipped_count}")
    if inserted_count + skipped_count != row_count_in_csv: print("WARNING: Row count mismatch.")

except FileNotFoundError: print(f"Error: Could not find the main CSV file '{MAIN_CSV_FILE}'")
except ValueError as ve: print(f"ValueError during setup: {ve}")
except Exception as e: print(f"An error occurred during main CSV processing: {e}")
finally:
    if conn: conn.close(); print("Database connection closed.")