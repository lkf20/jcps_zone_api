import os
import pandas as pd
import geopandas as gpd

# --- Purpose ---
# This script performs diagnostic checks on the school GIS data and display name mappings.
# 1. Loads the necessary shapefiles.
# 2. Extracts unique raw school identifiers from relevant columns.
# 3. Cross-checks these identifiers against the defined DISPLAY_NAMES_* dictionaries.
# It helps identify discrepancies like missing mappings or potential typos.
# Run directly using: python diagnostics.py

# --- Path Definitions ---
# Assumes this script is in the same directory as app.py was
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data_alt") # Adjust if needed

choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "Resides_HS_Boundaries.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp")
elementary_path = os.path.join(DATA_DIR, "Elementary", "Resides_ES_Clusters_Boundaries.shp")
traditional_middle_path = os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp")
traditional_high_path = os.path.join(DATA_DIR, "TraditionalHigh", "Traditional_HS_Bnds.shp")
traditional_elem_path = os.path.join(DATA_DIR, "TraditionalElementary", "Traditional_ES_Bnds.shp")

# --- Display Name Dictionaries ---
# *** IMPORTANT: Keep these identical to the ones in app.py ***
# --- Display Names (Remain the same) ---
DISPLAY_NAMES_HIGH = {
    "ATHERTON": "Atherton High",
    "BALLARD": "Ballard High",
    "DOSS": "Doss High",
    "EASTERN": "Eastern High",
    "FAIRDALE": "Fairdale High",
    "FERN CREEK": "Fern Creek High",
    "IROQUOIS": "Iroquois High",
    "JEFFERSONTOWN": "Jeffersontown High",
    "MOORE":"Marion C. Moore School",
    "PRP":"Pleasure Ridge Park High",
    "SENECA":"Seneca High",
    "SOUTHERN":"Southern High",
    "VALLEY":"Valley High",
    "WAGGENER":"Waggener High",
    "BUTLER": "Butler Traditional High",
    "MALE": "Louisville Male High"
}

DISPLAY_NAMES_MIDDLE = {
    "BARRETT": "Barret Traditional Middle", # Note: Typo 'Barret' vs 'Barrett' common, check data/keys
    "JCTMS": "Jefferson County Traditional Middle",
    "JOHNSON": "Johnson Traditional Middle",
    "CARRITHERS":"Carrithers Middle",
    "CONWAY":"Conway Middle",
    "CROSBY":"Crosby Middle",
    "ECHO TRAIL":"Echo Trail Middle School",
    "FARNSLEY":"Farnsley Middle",
    "HIGHLAND":"Highland Middle",
    "KAMMERER":"Kammerer Middle",
    "KNIGHT":"Knight Middle",
    "LASSITER":"Lassiter Middle",
    "MEYZEEK":"Meyzeek Middle",
    "MOORE":"Marion C. Moore School",
    "NEWBURG":"Newburg Middle",
    "NOE":"Noe Middle",
    "OLMSTED ACADEMY N/S":"Frederick Law Olmsted Academy North",
    "RAMSEY":"Ramsey Middle",
    "STUART":"Stuart Academy",
    "THOMAS JEFFERSON":"Thomas Jefferson Middle",
    "WESTPORT":"Westport Middle"
}

DISPLAY_NAMES_ELEMENTARY = {
    "AUDUBON": "Audubon Traditional Elementary",
    "CARTER": "Carter Traditional Elementary",
    "FOSTER": "Foster Traditional Academy",
    "GREATHOUSE": "Greathouse/Shryock Traditional",
    "SCHAFFNER": "Schaffner Traditional Elementary",
}

# ==============================================================
#                  MAIN DIAGNOSTIC EXECUTION
# ==============================================================
if __name__ == "__main__":

    print("Starting GIS Data Diagnostics...")

    # --- Load Shapefiles ---
    print("\n🔄 Loading Shapefiles for Diagnostics...")
    try:
        # Load individual GDFs needed for checking specific columns
        choice_gdf = gpd.read_file(choice_path)
        high_gdf = gpd.read_file(high_path)
        middle_gdf = gpd.read_file(middle_path)
        elementary_gdf = gpd.read_file(elementary_path)
        traditional_middle_gdf = gpd.read_file(traditional_middle_path)
        traditional_high_gdf = gpd.read_file(traditional_high_path)
        traditional_elem_gdf = gpd.read_file(traditional_elem_path)
        print("✅ Shapefiles loaded successfully.")
    except Exception as e:
        print(f"❌❌ FATAL ERROR loading shapefiles: {e}")
        print("   Cannot continue diagnostics.")
        exit() # Exit if shapefiles can't load


    # --- START: Diagnostic Logging for Raw School Names ---
    # (Paste the first diagnostic block here - the one that defines
    #  log_unique_values and populates raw_identifiers)
    # --- Example Snippet (Paste the full block from previous answer) ---
    print("\n" + "="*50)
    print("🔍 DIAGNOSTIC: Logging Unique Raw School Identifiers from Shapefiles")
    print("="*50)
    raw_identifiers = {}
    def log_unique_values(gdf, gdf_name, column_name, category_key):
        # ... (full helper function code) ...
        pass # Placeholder for the full function
    files_and_columns = [
        # ... (full list definition) ...
    ]
    for gdf, gdf_name, col_name, cat_key in files_and_columns:
        log_unique_values(gdf, gdf_name, col_name, cat_key)
    # ... (rest of the first diagnostic block printing results) ...
    print("\n" + "="*50)
    print("🔍 END DIAGNOSTIC LOGGING")
    print("="*50 + "\n")
    # --- END: Diagnostic Logging ---


    # --- START: Cross-Check GIS Identifiers with Display Name Dictionaries ---
    # (Paste the second diagnostic block here - the one that performs
    #  the cross-check using category_to_dict_map)
    # --- Example Snippet (Paste the full block from previous answer) ---
    print("\n" + "="*60)
    print("🔍 DIAGNOSTIC: Cross-Checking GIS Identifiers vs. Display Name Dicts")
    print("="*60)
    category_to_dict_map = {
         # ... (full map definition) ...
    }
    missing_mappings = []
    processed_normalized_gis_keys = set()
    # ... (rest of the second diagnostic block performing checks and printing results) ...
    print("\n" + "="*60)
    print("🔍 END DIAGNOSTIC CROSS-CHECK")
    print("="*60 + "\n")
    # --- END: Cross-Check GIS Identifiers ---

    print("Diagnostics finished.")