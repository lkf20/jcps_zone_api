# -----------------------------------------------------------------------------
# JCPS School Data Ingestion Script (Corrected Version)
# -----------------------------------------------------------------------------
# This script automates the process of downloading school report card data,
# filtering it for Jefferson County, transforming it into a usable format,
# and loading it into a SQLite database for use by an API.
# -----------------------------------------------------------------------------

import requests
import pandas as pd
import sqlite3
import os
import numpy as np # Used for safe division when calculating percentages

# --- 1. PATH CONFIGURATION ---
SCRIPT_ABS_DIR = os.path.dirname(os.path.abspath(__file__))
API_ROOT_DIR = os.path.join(SCRIPT_ABS_DIR, os.pardir)
DATA_DIR = os.path.join(API_ROOT_DIR, "data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "school_data_downloads")
DATABASE_NAME = os.path.join(DATA_DIR, "jcps_schools.db")

# --- 2. DATA SOURCE CONFIGURATION ---
CSV_URLS = [
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Student_Membership.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Economically_Disadvantaged.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Average_Years_School_Experience.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_ASMT_Kentucky_Summative_Assessment.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Inexperienced_Teachers.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_EDOP_Gifted_Participation_by_Grade_Level.csv",
]

# --- 3. COLUMN SELECTION & SCRIPT LOGIC CONFIGURATION ---
COLUMNS_TO_KEEP = {
    'ovw_student_membership': [
        'District Number', 'School Code', 'School Name','School Type',
        'Preschool', 'K', 'Grade 1', 'Grade 2', 'Grade 3', 'Grade 4', 'Grade 5',
        'Grade 6', 'Grade 7', 'Grade 8', 'Grade 9', 'Grade 10', 'Grade 11', 'Grade 12'
    ],
    'ovw_economically_disadvantaged': [
        'District Number', 'School Code', 'TOTAL COUNT ECON DISADVANTAGED','TOTAL MEMBERSHIP'
    ],
    'ovw_average_years_school_experience': [
        'District Number', 'School Code', 'Average Years of Experience',
    ],
    'asmt_kentucky_summative_assessment': [
        'District Number', 'School Code', 'School Type', 'Grade', 'Subject', 'Demographic','Proficient / Distinguished'
    ],
    'ovw_inexperienced_teachers': [
        'District Number', 'School Code', 'Percent Of Teachers With 1 3 Years Experience',
    ],
    'edop_gifted_participation_by_grade_level': [
        'District Number', 'School Code', 'Demographic','All Grades'
    ],
}

COUNTY_FILTER_COLUMN = 'District Number'
COUNTY_FILTER_VALUE = 275
SCHOOL_CODE_COLUMN_NAME = 'School Code'

INDIVIDUAL_GRADE_COLUMNS = {
    'Preschool': ['Preschool'],
    'Elementary': ['K', 'Grade 1', 'Grade 2', 'Grade 3', 'Grade 4', 'Grade 5'],
    'Middle': ['Grade 6', 'Grade 7', 'Grade 8'],
    'High': ['Grade 9', 'Grade 10', 'Grade 11', 'Grade 12']
}

LEVEL_SUFFIXES = { 'Preschool': '3', 'Elementary': '0', 'Middle': '1', 'High': '2' }

# --- 4. HELPER FUNCTIONS & GLOBAL VARIABLES ---
school_level_mapping = {}
school_membership_mapping = {}

def ensure_data_directories():
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)
    if not os.path.exists(DOWNLOAD_DIR): os.makedirs(DOWNLOAD_DIR)

def download_file(url):
    local_filename = os.path.join(DOWNLOAD_DIR, url.split('/')[-1])
    try:
        print(f"Downloading {url.split('/')[-1]}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
        print(f" -> Downloaded successfully.")
        return local_filename
    except requests.exceptions.RequestException as e:
        print(f" -> ERROR downloading {url}: {e}")
        return None

def build_school_level_mapping(df_membership):
    global school_level_mapping
    school_level_mapping = {}
    all_grade_cols = [col for sublist in INDIVIDUAL_GRADE_COLUMNS.values() for col in sublist]
    for col in all_grade_cols:
        if col in df_membership.columns:
            df_membership.loc[:, col] = pd.to_numeric(df_membership[col], errors='coerce').fillna(0).astype(int)
    for _, row in df_membership.iterrows():
        school_code = str(row[SCHOOL_CODE_COLUMN_NAME])
        levels_served = []
        # Skip Preschool level - only include Elementary, Middle, and High
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['Elementary']) > 0: levels_served.append('Elementary')
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['Middle']) > 0: levels_served.append('Middle')
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['High']) > 0: levels_served.append('High')
        school_level_mapping[school_code] = levels_served
    print("Built school level mapping from membership data.")

def process_and_load_data(file_path):
    print(f"\nProcessing {os.path.basename(file_path)}...")
    try:
        df = pd.read_csv(file_path, low_memory=False)
        file_name = os.path.basename(file_path)
        file_key = file_name.replace(".csv", "").replace("KYRC24_","").lower()

        if file_key in COLUMNS_TO_KEEP:
            columns_list = COLUMNS_TO_KEEP[file_key]
            essential_cols = [SCHOOL_CODE_COLUMN_NAME, COUNTY_FILTER_COLUMN]
            for col in essential_cols:
                if col not in columns_list: columns_list.append(col)
            existing_columns_to_keep = [col for col in columns_list if col in df.columns]
            print(f" -> Selecting {len(existing_columns_to_keep)} of {len(df.columns)} columns.")
            df = df[existing_columns_to_keep]

        if COUNTY_FILTER_COLUMN in df.columns:
            df.loc[:, COUNTY_FILTER_COLUMN] = pd.to_numeric(df[COUNTY_FILTER_COLUMN], errors='coerce')
            df_jcps = df[df[COUNTY_FILTER_COLUMN] == COUNTY_FILTER_VALUE].copy()
            print(f" -> Filtered for District {COUNTY_FILTER_VALUE}. Rows remaining: {len(df_jcps)}")
        else:
            df_jcps = df.copy()
        
        if df_jcps.empty:
            print(" -> No data remaining after filtering. Skipping file.")
            return

        if file_key == 'ovw_student_membership':
            print(" -> Transforming: Calculating Membership columns.")
            global school_membership_mapping
            grade_cols_for_sum = [col for sublist in INDIVIDUAL_GRADE_COLUMNS.values() for col in sublist]
            for col in grade_cols_for_sum:
                 if col in df_jcps.columns: df_jcps.loc[:, col] = pd.to_numeric(df_jcps[col], errors='coerce').fillna(0)
            k_12_cols = [col for col in grade_cols_for_sum if col != 'Preschool' and col in df_jcps.columns]
            prek_12_cols = [col for col in grade_cols_for_sum if col in df_jcps.columns]
            df_jcps['Membership'] = df_jcps[k_12_cols].sum(axis=1)
            df_jcps['Membership - All Grades With Preschool'] = df_jcps[prek_12_cols].sum(axis=1)
            school_membership_mapping = df_jcps.set_index(SCHOOL_CODE_COLUMN_NAME)['Membership'].to_dict()
            print(" -> Created school membership mapping for other files.")

        elif file_key == 'ovw_economically_disadvantaged':
            print(" -> Transforming: Calculating Percent Economically Disadvantaged.")
            df_jcps.loc[:, 'TOTAL COUNT ECON DISADVANTAGED'] = pd.to_numeric(df_jcps['TOTAL COUNT ECON DISADVANTAGED'], errors='coerce')
            df_jcps.loc[:, 'TOTAL MEMBERSHIP'] = pd.to_numeric(df_jcps['TOTAL MEMBERSHIP'], errors='coerce')
            
            # --- FIX 1: Robust Percentage Calculation ---
            percent = (df_jcps['TOTAL COUNT ECON DISADVANTAGED'] / df_jcps['TOTAL MEMBERSHIP']) * 100
            percent = percent.replace([np.inf, -np.inf], np.nan).fillna(0)
            df_jcps['Percent Economically Disadvantaged'] = percent.round(2)
            # --- END FIX 1 ---

            df_jcps = df_jcps.drop(columns=['TOTAL COUNT ECON DISADVANTAGED', 'TOTAL MEMBERSHIP'])
        
        elif file_key == 'asmt_kentucky_summative_assessment':
            print(" -> Transforming: Processing Grade 5, Grade 8, and Grade 10 assessment data for specific columns.")
            # Filter for Grades 3-12 (to allow fallback options), exclude Preschool, and only include School Type A1 and A5
            df_jcps = df_jcps[df_jcps['Grade'].isin(['Grade 3', 'Grade 4', 'Grade 5', 'Grade 6', 'Grade 7', 'Grade 8', 'Grade 9', 'Grade 10', 'Grade 11', 'Grade 12'])].copy()
            df_jcps = df_jcps[df_jcps['School Type'].isin(['A1', 'A5'])].copy()
            df_jcps.loc[:, 'Proficient / Distinguished'] = pd.to_numeric(df_jcps['Proficient / Distinguished'], errors='coerce').fillna(0)
            
            # Debug: Check if school 275013 survives the filtering (try both string and numeric)
            school_275013_data = df_jcps[df_jcps[SCHOOL_CODE_COLUMN_NAME] == '275013']
            if len(school_275013_data) == 0:
                school_275013_data = df_jcps[df_jcps[SCHOOL_CODE_COLUMN_NAME] == 275013]
            
            if len(school_275013_data) > 0:
                print(f"DEBUG: School 275013 survived filtering with {len(school_275013_data)} records")
                print(f"DEBUG: Available grades: {school_275013_data['Grade'].unique()}")
                print(f"DEBUG: Available subjects: {school_275013_data['Subject'].unique()}")
                print(f"DEBUG: School code type: {type(school_275013_data[SCHOOL_CODE_COLUMN_NAME].iloc[0])}")
            else:
                print("DEBUG: School 275013 was filtered out - checking original data...")
                original_data = pd.read_csv(file_path, low_memory=False)
                original_275013 = original_data[original_data['School Code'] == 275013]
                if len(original_275013) > 0:
                    print(f"DEBUG: School 275013 exists in original data with {len(original_275013)} records")
                    print(f"DEBUG: Original School Type: {original_275013['School Type'].unique()}")
                    print(f"DEBUG: Original Grades: {original_275013['Grade'].unique()}")
                    print(f"DEBUG: Original School Code type: {type(original_275013['School Code'].iloc[0])}")
                else:
                    print("DEBUG: School 275013 not found in original data")
            
            # Create the specific columns requested with fallback logic
            result_data = []
            for school_code in df_jcps[SCHOOL_CODE_COLUMN_NAME].unique():
                # Debug: Check if school 275013 is in the data
                if str(school_code) == '275013':
                    print(f"DEBUG: Found school 275013 in assessment data, total records: {len(df_jcps[df_jcps[SCHOOL_CODE_COLUMN_NAME] == school_code])}")
                school_data = df_jcps[df_jcps[SCHOOL_CODE_COLUMN_NAME] == school_code].copy()
                
                # Get the levels this school actually serves from the membership mapping
                levels_served = school_level_mapping.get(str(school_code), [])
                
                # Define grade fallback options and their corresponding levels
                grade_targets = {
                    'Grade 5': {
                        'level': 'Elementary',
                        'fallback_grades': ['Grade 5', 'Grade 4', 'Grade 3']
                    },
                    'Grade 8': {
                        'level': 'Middle',
                        'fallback_grades': ['Grade 8', 'Grade 6', 'Grade 7']
                    },
                    'Grade 10': {
                        'level': 'High',
                        'fallback_grades': ['Grade 10', 'Grade 11', 'Grade 12', 'Grade 9']
                    }
                }
                
                # Process each target grade for this school, but only if the school serves that level
                for target_grade, config in grade_targets.items():
                    required_level = config['level']
                    if required_level not in levels_served:
                        continue  # Skip if school doesn't serve this level
                    
                    # Try to find data using fallback grades
                    selected_grade = None
                    selected_grade_data = None
                    
                    # Debug: Track processing for school 275013
                    if str(school_code) == '275013':
                        print(f"DEBUG: Processing school {school_code}, target grade {target_grade}")
                    
                    # First pass: Look for grades with BOTH math AND reading data
                    for fallback_grade in config['fallback_grades']:
                        grade_data = school_data[school_data['Grade'] == fallback_grade].copy()
                        if not grade_data.empty:
                            # Check if this grade has meaningful data (not all zeros)
                            math_all_data = grade_data[(grade_data['Subject'] == 'Mathematics') & 
                                                      (grade_data['Demographic'] == 'All Students')]['Proficient / Distinguished']
                            reading_all_data = grade_data[(grade_data['Subject'] == 'Reading') & 
                                                         (grade_data['Demographic'] == 'All Students')]['Proficient / Distinguished']
                            
                            math_val = math_all_data.iloc[0] if len(math_all_data) > 0 else 0
                            reading_val = reading_all_data.iloc[0] if len(reading_all_data) > 0 else 0
                            
                            # Debug: Show what we found for school 275013
                            if str(school_code) == '275013':
                                print(f"DEBUG: Grade {fallback_grade} - Math: {math_val}, Reading: {reading_val}")
                                if len(math_all_data) > 0:
                                    print(f"DEBUG: Math data found: {math_all_data.iloc[0]}")
                                if len(reading_all_data) > 0:
                                    print(f"DEBUG: Reading data found: {reading_all_data.iloc[0]}")
                            
                            # If we have BOTH math AND reading data, use this grade (preferred)
                            if math_val > 0 and reading_val > 0:
                                selected_grade = fallback_grade
                                selected_grade_data = grade_data
                                if str(school_code) == '275013':
                                    print(f"DEBUG: Selected grade {fallback_grade} for target {target_grade} (BOTH math and reading)")
                                break
                    
                    # Second pass: If no grade has both math and reading, look for any grade with either
                    if selected_grade is None:
                        for fallback_grade in config['fallback_grades']:
                            grade_data = school_data[school_data['Grade'] == fallback_grade].copy()
                            if not grade_data.empty:
                                # Check if this grade has meaningful data (not all zeros)
                                math_all_data = grade_data[(grade_data['Subject'] == 'Mathematics') & 
                                                          (grade_data['Demographic'] == 'All Students')]['Proficient / Distinguished']
                                reading_all_data = grade_data[(grade_data['Subject'] == 'Reading') & 
                                                             (grade_data['Demographic'] == 'All Students')]['Proficient / Distinguished']
                                
                                math_val = math_all_data.iloc[0] if len(math_all_data) > 0 else 0
                                reading_val = reading_all_data.iloc[0] if len(reading_all_data) > 0 else 0
                                
                                # If we have non-zero data for either math or reading, use this grade
                                if math_val > 0 or reading_val > 0:
                                    selected_grade = fallback_grade
                                    selected_grade_data = grade_data
                                    if str(school_code) == '275013':
                                        print(f"DEBUG: Selected grade {fallback_grade} for target {target_grade} (partial data)")
                                    break
                    
                    # If we found data, create the record
                    if selected_grade and selected_grade_data is not None:
                        result_row = {SCHOOL_CODE_COLUMN_NAME: school_code, 'Grade': target_grade}
                        
                        # Debug: Show what we're extracting for school 275013
                        if str(school_code) == '275013':
                            print(f"DEBUG: Extracting data from selected grade {selected_grade}")
                            print(f"DEBUG: Selected grade data shape: {selected_grade_data.shape}")
                            print(f"DEBUG: Available subjects: {selected_grade_data['Subject'].unique()}")
                            print(f"DEBUG: Available demographics: {selected_grade_data['Demographic'].unique()}")
                        
                        # Mathematics columns
                        math_all = selected_grade_data[(selected_grade_data['Subject'] == 'Mathematics') & 
                                                      (selected_grade_data['Demographic'] == 'All Students')]['Proficient / Distinguished']
                        math_econ_dis = selected_grade_data[(selected_grade_data['Subject'] == 'Mathematics') & 
                                                           (selected_grade_data['Demographic'] == 'Economically Disadvantaged')]['Proficient / Distinguished']
                        math_non_econ_dis = selected_grade_data[(selected_grade_data['Subject'] == 'Mathematics') & 
                                                               (selected_grade_data['Demographic'] == 'Non Economically Disadvantaged')]['Proficient / Distinguished']
                        
                        # Debug: Show math data extraction results for school 275013
                        if str(school_code) == '275013':
                            print(f"DEBUG: Math All Students found: {len(math_all)} records")
                            if len(math_all) > 0:
                                print(f"DEBUG: Math All Students value: {math_all.iloc[0]}")
                            print(f"DEBUG: Math Econ Dis found: {len(math_econ_dis)} records")
                            if len(math_econ_dis) > 0:
                                print(f"DEBUG: Math Econ Dis value: {math_econ_dis.iloc[0]}")
                            print(f"DEBUG: Math Non-Econ Dis found: {len(math_non_econ_dis)} records")
                            if len(math_non_econ_dis) > 0:
                                print(f"DEBUG: Math Non-Econ Dis value: {math_non_econ_dis.iloc[0]}")
                        
                        result_row['Mathematics - All Students Proficient or Distinguished'] = math_all.iloc[0] if len(math_all) > 0 else 0
                        result_row['Mathematics - Economically Disadvantaged Proficient or Distinguished'] = math_econ_dis.iloc[0] if len(math_econ_dis) > 0 else 0
                        result_row['Mathematics - Non Economically Disadvantaged Proficient or Distinguished'] = math_non_econ_dis.iloc[0] if len(math_non_econ_dis) > 0 else 0
                        
                        # Reading columns
                        reading_all = selected_grade_data[(selected_grade_data['Subject'] == 'Reading') & 
                                                         (selected_grade_data['Demographic'] == 'All Students')]['Proficient / Distinguished']
                        reading_econ_dis = selected_grade_data[(selected_grade_data['Subject'] == 'Reading') & 
                                                              (selected_grade_data['Demographic'] == 'Economically Disadvantaged')]['Proficient / Distinguished']
                        reading_non_econ_dis = selected_grade_data[(selected_grade_data['Subject'] == 'Reading') & 
                                                                  (selected_grade_data['Demographic'] == 'Non Economically Disadvantaged')]['Proficient / Distinguished']
                        
                        result_row['Reading - All Students Proficient or Distinguished'] = reading_all.iloc[0] if len(reading_all) > 0 else 0
                        result_row['Reading - Economically Disadvantaged Proficient or Distinguished'] = reading_econ_dis.iloc[0] if len(reading_econ_dis) > 0 else 0
                        result_row['Reading - Non Economically Disadvantaged Proficient or Distinguished'] = reading_non_econ_dis.iloc[0] if len(reading_non_econ_dis) > 0 else 0
                        
                        result_data.append(result_row)
            
            df_jcps = pd.DataFrame(result_data)

        elif file_key == 'edop_gifted_participation_by_grade_level':
            print(" -> Transforming: Calculating Percent Gifted and Talented.")
            df_jcps = df_jcps[df_jcps['Demographic'] == 'All Students'].copy()
            df_jcps.loc[:, 'All Grades'] = pd.to_numeric(df_jcps['All Grades'], errors='coerce').fillna(0)
            def calculate_gifted_percent(row):
                membership = school_membership_mapping.get(str(row[SCHOOL_CODE_COLUMN_NAME]), 0)
                return round((row['All Grades'] / membership) * 100, 2) if membership > 0 else 0.0
            df_jcps['Percent Gifted and Talented'] = df_jcps.apply(calculate_gifted_percent, axis=1)
            df_jcps = df_jcps.drop(columns=['Demographic', 'All Grades'])

        if SCHOOL_CODE_COLUMN_NAME not in df_jcps.columns:
            print(f" -> ERROR: School Code column not found after transformations. Skipping.")
            return
            
        if "KYRC24_OVW_Student_Membership.csv" in file_name:
            build_school_level_mapping(df_jcps)

        processed_rows = []
        for _, row in df_jcps.iterrows():
            school_code = str(row[SCHOOL_CODE_COLUMN_NAME])
            levels = school_level_mapping.get(school_code, [])
            if not levels:
                row_copy = row.copy(); row_copy['School Code Adjusted'] = school_code; processed_rows.append(row_copy)
            else:
                for level in levels:
                    row_copy = row.copy(); row_copy['School Code Adjusted'] = school_code + LEVEL_SUFFIXES.get(level, ''); row_copy['School Level'] = level; processed_rows.append(row_copy)
        
        # --- FIX 2: Correct DataFrame Assembly ---
        df_to_load = pd.DataFrame(processed_rows)
        # --- END FIX 2 ---
        
        print(f" -> Generated {len(df_to_load)} final records for this file.")
        if df_to_load.empty:
            print(" -> No data to load into database. Skipping.")
            return

        table_name = "student_membership_adjusted" if "KYRC24_OVW_Student_Membership.csv" in file_name else file_key
        print(f" -> Loading data into table: '{table_name}'")
        conn = sqlite3.connect(DATABASE_NAME)
        df_to_load.to_sql(table_name, conn, if_exists='replace', index=False)
        conn.close()
        print(f" -> Successfully loaded data into '{table_name}'.")
    except Exception as e:
        print(f" -> FATAL ERROR while processing {os.path.basename(file_path)}: {e}")

# --- 5. MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    print("--- Starting JCPS Data Ingestion Process ---")
    ensure_data_directories()
    
    downloaded_filepaths = [fp for url in CSV_URLS if (fp := download_file(url)) is not None]
    if not downloaded_filepaths: exit("ABORT: No files were downloaded.")

    membership_filename = "KYRC24_OVW_Student_Membership.csv"
    membership_file_path = None
    other_files_paths = []
    for fp in downloaded_filepaths:
        if os.path.basename(fp) == membership_filename: membership_file_path = fp
        else: other_files_paths.append(fp)

    if membership_file_path:
        process_and_load_data(membership_file_path)
    else:
        exit(f"ABORT: Membership file ('{membership_filename}') was not found. Cannot continue.")
    
    if school_level_mapping and school_membership_mapping:
        print("\n--- Processing Remaining Files ---")
        for file_path in other_files_paths:
            process_and_load_data(file_path)
    else:
        print("\nWARNING: A required mapping failed. Skipping processing of other files.")

    print("\n--- Data Ingestion Process Complete! ---")
    print(f"Database located at: '{os.path.normpath(DATABASE_NAME)}'")