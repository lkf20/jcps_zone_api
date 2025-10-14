# -----------------------------------------------------------------------------
# JCPS School Data Ingestion Script
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
# Sets up the directory structure based on this script's location.
# Assumes 'data' and 'scripts' folders are at the same level (e.g., in an 'app' or project root folder).
SCRIPT_ABS_DIR = os.path.dirname(os.path.abspath(__file__))
API_ROOT_DIR = os.path.join(SCRIPT_ABS_DIR, os.pardir)
DATA_DIR = os.path.join(API_ROOT_DIR, "data")
DOWNLOAD_DIR = os.path.join(DATA_DIR, "school_data_downloads")
DATABASE_NAME = os.path.join(DATA_DIR, "jcps_schools.db")

# --- 2. DATA SOURCE CONFIGURATION ---
# List of URLs for the CSV files to download
CSV_URLS = [
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Student_Membership.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Economically_Disadvantaged.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_OVW_Average_Years_School_Experience.csv",
    "https://kdeschoolreportcard.blob.core.windows.net/datasets/KYRC24_ASMT_Kentucky_Summative_Assessment.csv",
]

# --- 3. COLUMN SELECTION & SCRIPT LOGIC CONFIGURATION ---
# This dictionary defines which columns to keep from each file.
# This reduces database size and memory usage.
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
        'District Number', 'School Code', 'Grade', 'Subject', 'Demographic','Proficient / Distinguished'
    ],
    'ovw_inexperienced_teachers': [
        'District Number', 'School Code', 'Percent Of Teachers With 1 3 Years Experience',
    ],
    'edop_gifted_participation_by_grade_level': [
        'District Number', 'School Code', 'Demographic','All Grades'
    ],

}

# --- Filtering and Level Detection Configuration ---
COUNTY_FILTER_COLUMN = 'District Number'
COUNTY_FILTER_VALUE = 275
SCHOOL_CODE_COLUMN_NAME = 'School Code'

INDIVIDUAL_GRADE_COLUMNS = {
    'Preschool': ['Preschool'],
    'Elementary': ['K', 'Grade 1', 'Grade 2', 'Grade 3', 'Grade 4', 'Grade 5'],
    'Middle': ['Grade 6', 'Grade 7', 'Grade 8'],
    'High': ['Grade 9', 'Grade 10', 'Grade 11', 'Grade 12']
}

LEVEL_SUFFIXES = {
    'Preschool': '3',
    'Elementary': '0',
    'Middle': '1',
    'High': '2'
}

# --- 4. HELPER FUNCTIONS ---

# Global variable to store school level mapping (populated by membership file)
school_level_mapping = {}

def ensure_data_directories():
    """Ensures the data and download directories exist before starting."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"Created data directory: {DATA_DIR}")
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Created download directory: {DOWNLOAD_DIR}")

def download_file(url):
    """Downloads a file from a given URL to the DOWNLOAD_DIR."""
    local_filename = os.path.join(DOWNLOAD_DIR, url.split('/')[-1])
    try:
        print(f"Downloading {url.split('/')[-1]}...")
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_filename, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f" -> Downloaded successfully.")
        return local_filename
    except requests.exceptions.RequestException as e:
        print(f" -> ERROR downloading {url}: {e}")
        return None

def build_school_level_mapping(df_membership):
    """
    Builds a mapping of School Code to a list of levels it serves
    by summing individual grade columns from the membership file.
    """
    global school_level_mapping
    school_level_mapping = {}

    all_grade_cols = [col for sublist in INDIVIDUAL_GRADE_COLUMNS.values() for col in sublist]
    for col in all_grade_cols:
        if col in df_membership.columns:
            df_membership.loc[:, col] = pd.to_numeric(df_membership[col], errors='coerce').fillna(0).astype(int)
        else:
            print(f"Warning: Grade column '{col}' not found in membership file.")

    for _, row in df_membership.iterrows():
        school_code = str(row[SCHOOL_CODE_COLUMN_NAME])
        levels_served = []
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['Preschool']) > 0:
            levels_served.append('Preschool')
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['Elementary']) > 0:
            levels_served.append('Elementary')
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['Middle']) > 0:
            levels_served.append('Middle')
        if sum(row.get(grade_col, 0) for grade_col in INDIVIDUAL_GRADE_COLUMNS['High']) > 0:
            levels_served.append('High')
        school_level_mapping[school_code] = levels_served
    print("Built school level mapping from membership data.")
    return school_level_mapping

def process_and_load_data(file_path):
    """
    The main processing pipeline for each file.
    """
    print(f"\nProcessing {os.path.basename(file_path)}...")
    try:
        # Step 1: Read the CSV file
        df = pd.read_csv(file_path, low_memory=False)
        file_name = os.path.basename(file_path)
        file_key = file_name.replace(".csv", "").replace("KYRC24_","").lower()

        # Step 2: Select only the columns we need
        if file_key in COLUMNS_TO_KEEP:
            columns_list = COLUMNS_TO_KEEP[file_key]
            essential_cols = [SCHOOL_CODE_COLUMN_NAME, COUNTY_FILTER_COLUMN]
            for col in essential_cols:
                if col not in columns_list: columns_list.append(col)
            existing_columns_to_keep = [col for col in columns_list if col in df.columns]
            print(f" -> Selecting {len(existing_columns_to_keep)} of {len(df.columns)} columns.")
            df = df[existing_columns_to_keep]
        else:
            print(f" -> No column specification found. Keeping all {len(df.columns)} columns.")

        # Step 3: Filter for Jefferson County (District 275)
        if COUNTY_FILTER_COLUMN in df.columns:
            df.loc[:, COUNTY_FILTER_COLUMN] = pd.to_numeric(df[COUNTY_FILTER_COLUMN], errors='coerce')
            df_jcps = df[df[COUNTY_FILTER_COLUMN] == COUNTY_FILTER_VALUE].copy()
            print(f" -> Filtered for District {COUNTY_FILTER_VALUE}. Rows remaining: {len(df_jcps)}")
        else:
            print(f" -> WARNING: District Number column not found. Skipping filter.")
            df_jcps = df.copy()
        
        if df_jcps.empty:
            print(" -> No data remaining after filtering. Skipping file.")
            return

        # Step 4: Perform file-specific data transformations
        if file_key == 'ovw_economically_disadvantaged':
            print(" -> Transforming: Calculating Percent Economically Disadvantaged.")
            df_jcps.loc[:, 'TOTAL COUNT ECON DISADVANTAGED'] = pd.to_numeric(df_jcps['TOTAL COUNT ECON DISADVANTAGED'], errors='coerce')
            df_jcps.loc[:, 'TOTAL MEMBERSHIP'] = pd.to_numeric(df_jcps['TOTAL MEMBERSHIP'], errors='coerce')
            with np.errstate(divide='ignore', invalid='ignore'):
                 percent = np.divide(df_jcps['TOTAL COUNT ECON DISADVANTAGED'], df_jcps['TOTAL MEMBERSHIP']) * 100
            df_jcps['Percent Economically Disadvantaged'] = np.nan_to_num(percent, nan=0.0, posinf=0.0, neginf=0.0).round(2)
            df_jcps = df_jcps.drop(columns=['TOTAL COUNT ECON DISADVANTAGED', 'TOTAL MEMBERSHIP'])
        
        elif file_key == 'asmt_kentucky_summative_assessment':
            print(" -> Transforming: Pivoting assessment data to wide format.")
            df_jcps.loc[:, 'Proficient / Distinguished'] = pd.to_numeric(df_jcps['Proficient / Distinguished'], errors='coerce').fillna(0)
            pivot_df = df_jcps.pivot_table(index=[COUNTY_FILTER_COLUMN, SCHOOL_CODE_COLUMN_NAME, 'Grade'], columns=['Subject', 'Demographic'], values='Proficient / Distinguished', aggfunc='first').fillna(0)
            pivot_df.columns = [f"{subject} - {demographic} Proficient or Distinguished" for subject, demographic in pivot_df.columns]
            df_jcps = pivot_df.reset_index()

        # Step 5: Expand rows for multi-level schools and create 'School Code Adjusted'
        if SCHOOL_CODE_COLUMN_NAME not in df_jcps.columns:
            print(f" -> ERROR: School Code column not found after transformations. Skipping.")
            return
            
        df_to_load = pd.DataFrame()
        if "KYRC24_OVW_Student_Membership.csv" in file_name:
            build_school_level_mapping(df_jcps) # This file builds the mapping

        processed_dfs = []
        if not school_level_mapping and "KYRC24_OVW_Student_Membership.csv" not in file_name:
             print(" -> WARNING: School level mapping not available. Using original School Code.")
             df_jcps['School Code Adjusted'] = df_jcps[SCHOOL_CODE_COLUMN_NAME].astype(str)
             processed_dfs = [df_jcps]
        else:
            for _, row in df_jcps.iterrows():
                school_code = str(row[SCHOOL_CODE_COLUMN_NAME])
                levels = school_level_mapping.get(school_code, [])
                if not levels:
                    row_copy = row.copy()
                    row_copy['School Code Adjusted'] = school_code
                    processed_dfs.append(row_copy)
                else:
                    for level in levels:
                        row_copy = row.copy()
                        row_copy['School Code Adjusted'] = school_code + LEVEL_SUFFIXES.get(level, '')
                        row_copy['School Level'] = level
                        processed_dfs.append(row_copy)

        if processed_dfs:
            df_to_load = pd.DataFrame(processed_dfs)
        
        print(f" -> Generated {len(df_to_load)} final records for this file.")
        if df_to_load.empty:
            print(" -> No data to load into database. Skipping.")
            return

        # Step 6: Load the final DataFrame into the SQLite database
        if "KYRC24_OVW_Student_Membership.csv" in file_name:
            table_name = "student_membership_adjusted"
        else:
            table_name = file_key
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
    
    downloaded_filepaths = []
    for url in CSV_URLS:
        filepath = download_file(url)
        if filepath:
            downloaded_filepaths.append(filepath)

    if not downloaded_filepaths:
        exit("ABORT: No files were downloaded. Please check URLs and internet connection.")

    membership_filename = "KYRC24_OVW_Student_Membership.csv"
    membership_file_path = None
    other_files_paths = []
    for fp in downloaded_filepaths:
        if os.path.basename(fp) == membership_filename:
            membership_file_path = fp
        else:
            other_files_paths.append(fp)

    if membership_file_path:
        process_and_load_data(membership_file_path)
    else:
        exit(f"ABORT: Membership file ('{membership_filename}') was not found. Cannot continue.")
    
    if school_level_mapping:
        print("\n--- Processing Remaining Files ---")
        for file_path in other_files_paths:
            process_and_load_data(file_path)
    else:
        print("\nWARNING: School level mapping failed. Skipping processing of other files.")

    print("\n--- Data Ingestion Process Complete! ---")
    print(f"Database located at: '{os.path.normpath(DATABASE_NAME)}'")