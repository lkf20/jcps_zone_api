from flask import Flask, request, jsonify
from geopy.geocoders import Nominatim
from geopy.distance import geodesic # Import geodesic directly
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os
import json
import time # For potential rate limiting if needed



# --- Load Data (Paths remain the same) ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data_alt") # Adjust if needed

choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "Resides_HS_Boundaries.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp")
elementary_path = os.path.join(DATA_DIR, "Elementary", "Resides_ES_Clusters_Boundaries.shp")
traditional_middle_path = os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp")
traditional_high_path = os.path.join(DATA_DIR, "TraditionalHigh", "Traditional_HS_Bnds.shp")
traditional_elem_path = os.path.join(DATA_DIR, "TraditionalElementary", "Traditional_ES_Bnds.shp")

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

UNIVERSAL_SCHOOLS = {
    "Elementary": ["Brandeis Elementary", "Coleridge-Taylor Montessori Elementary", "Hawthorne Elementary", "J. Graham Brown School", "Lincoln Elementary Performing Arts", "Young Elementary"],
    "Middle": ["Grace M. James Academy of Excellence", "J. Graham Brown School", "W.E.B. DuBois Academy", "Western Middle School for the Arts"],
    "High": ["Central High Magnet Career Academy", "duPont Manual High", "J. Graham Brown School", "W.E.B. DuBois Academy", "Western High"]
}

# Combine all display names for easier lookup later, handling potential overlaps carefully
ALL_DISPLAY_NAMES = {**DISPLAY_NAMES_HIGH, **DISPLAY_NAMES_MIDDLE, **DISPLAY_NAMES_ELEMENTARY}
# Add universal schools to the lookup if they aren't already covered by specific keys
for level, schools in UNIVERSAL_SCHOOLS.items():
    for school in schools:
        # If the raw school name isn't already a key, add it mapping to itself
        if school.upper() not in ALL_DISPLAY_NAMES and school not in ALL_DISPLAY_NAMES.values():
             # Basic heuristic: If it ends in Elementary/Middle/High, use that, else add level
             if not any(school.endswith(s) for s in [" Elementary", " Middle", " High", " School", " Academy"]):
                 ALL_DISPLAY_NAMES[school.upper()] = f"{school} {level}" # e.g. "J. Graham Brown School High" - adjust as needed
             else:
                 ALL_DISPLAY_NAMES[school.upper()] = school


# --- Load Feeders and Links (Remain the same) ---
feeders_path = os.path.join(BASE_DIR, "elementary_feeders.json")
with open(feeders_path, "r") as f:
    ELEMENTARY_FEEDERS = json.load(f)

report_links_path = os.path.join(BASE_DIR, "school_report_links.json")
with open(report_links_path, "r") as f:
    SCHOOL_REPORT_LINKS = json.load(f)


# --- Load Shapefiles (Remain the same) ---
print("🔄 Loading Shapefiles...")
try:
    choice_gdf = gpd.read_file(choice_path)
    choice_gdf["zone_type"] = "Choice"

    high_gdf = gpd.read_file(high_path)
    high_gdf["zone_type"] = "High"

    middle_gdf = gpd.read_file(middle_path)
    middle_gdf["zone_type"] = "Middle"

    elementary_gdf = gpd.read_file(elementary_path)
    elementary_gdf["zone_type"] = "Elementary" # Represents clusters

    traditional_middle_gdf = gpd.read_file(traditional_middle_path)
    # *** Check the actual column name in your shapefile for Traditional zones ***
    # It might be 'School', 'Name', 'Traditiona', etc. Adjust .get() below accordingly.
    traditional_middle_gdf["zone_type"] = "Traditional/Magnet Middle"

    traditional_high_gdf = gpd.read_file(traditional_high_path)
    traditional_high_gdf["zone_type"] = "Traditional/Magnet High"

    traditional_elem_gdf = gpd.read_file(traditional_elem_path)
    traditional_elem_gdf["zone_type"] = "Traditional/Magnet Elementary"


    all_zones_gdf = gpd.GeoDataFrame(
        pd.concat([
            choice_gdf,
            high_gdf,
            middle_gdf,
            elementary_gdf,
            traditional_middle_gdf,
            traditional_high_gdf,
            traditional_elem_gdf
        ], ignore_index=True, sort=False) # Added sort=False for potential future pandas versions
    ).to_crs(epsg=4326) # Convert to WGS84 (lat/lon)
    print("✅ Shapefiles loaded and merged.")
    # print("Columns:", all_zones_gdf.columns) # Useful for debugging column names
    # print("Sample Data:\n", all_zones_gdf.head())

except Exception as e:
    print(f"❌ Error loading shapefiles: {e}")
    # Exit or handle error appropriately if shapefiles are critical
    exit()


    # ... (previous code loading shapefiles and creating all_zones_gdf) ...



# --- START: Diagnostic Logging for Raw School Names ---
# print("\n" + "="*50)
# print("🔍 DIAGNOSTIC: Logging Unique Raw School Identifiers from Shapefiles")
# print("="*50)

# raw_identifiers = {}

# def log_unique_values(gdf, gdf_name, column_name, category_key):
#     """Helper function to extract and store unique, non-null string values."""
#     if gdf is not None and column_name in gdf.columns:
#         try:
#             # Drop nulls, convert to string, get unique, filter out empty strings
#             unique_vals = gdf[column_name].dropna().astype(str).unique()
#             unique_vals = {val.strip() for val in unique_vals if val.strip()} # Use set for auto-uniqueness and strip whitespace

#             if unique_vals:
#                 if category_key not in raw_identifiers:
#                         raw_identifiers[category_key] = set()
#                 raw_identifiers[category_key].update(unique_vals)
#                 print(f"  ✅ Found {len(unique_vals)} unique non-empty values in '{gdf_name}' -> '{column_name}' column.")
#             else:
#                     print(f"  ℹ️ No non-empty values found in '{gdf_name}' -> '{column_name}' column.")
#         except Exception as e:
#                 print(f"  ❌ Error processing '{gdf_name}' -> '{column_name}': {e}")
#     else:
#             print(f"  ⚠️ Column '{column_name}' not found or GDF '{gdf_name}' is None.")

# # Define which columns to check in which GDFs and map to a category
# # (Using descriptive category keys for clarity)
# files_and_columns = [
#     (high_gdf, "High.shp", "High", "High School Names (from 'High' column)"),
#     (middle_gdf, "Resides_MS_Boundaries.shp", "Middle", "Middle School Names (from 'Middle' column)"),
#     (middle_gdf, "Resides_MS_Boundaries.shp", "Name", "Middle School Names (from 'Name' column)"), # Log 'Name' column from Middle separately
#     (elementary_gdf, "Resides_ES_Clusters_Boundaries.shp", "High", "High School Refs in Elem Zones (from 'High' column)"),
#     (choice_gdf, "ChoiceZone.shp", "Name", "Choice Zone Names (from 'Name' column)"),
#     (traditional_high_gdf, "Traditional_HS_Bnds.shp", "Traditiona", "Traditional High Names (from 'Traditiona' column)"),
#     (traditional_middle_gdf, "Traditional_MS_Bnds.shp", "Traditiona", "Traditional Middle Names (from 'Traditiona' column)"),
#     (traditional_elem_gdf, "Traditional_ES_Bnds.shp", "Traditiona", "Traditional Elem Names (from 'Traditiona' column)"),
# ]

# # Process each file and column
# for gdf, gdf_name, col_name, cat_key in files_and_columns:
#     log_unique_values(gdf, gdf_name, col_name, cat_key)

# # Print the consolidated results
# print("\n" + "-"*50)
# print("📊 Consolidated Unique Raw Identifiers Found:")
# print("-"*50)
# if raw_identifiers:
#     for category, names_set in raw_identifiers.items():
#         print(f"\n{category}:")
#         if names_set:
#             sorted_names = sorted(list(names_set))
#             for name in sorted_names:
#                 print(f"  - \"{name}\"") # Print in quotes to see exact whitespace/casing
#         else:
#             print("  (None found)")
# else:
#     print("  No identifiers found in specified columns.")

# print("\n" + "="*50)
# print("🔍 END DIAGNOSTIC LOGGING")
# print("="*50 + "\n")
# # --- END: Diagnostic Logging ---

# # ... (Previous diagnostic block ends here) ...
# print("\n" + "="*50)
# print("🔍 END DIAGNOSTIC LOGGING")
# print("="*50 + "\n")

# # --- START: Cross-Check GIS Identifiers with Display Name Dictionaries ---
# print("\n" + "="*60)
# print("🔍 DIAGNOSTIC: Cross-Checking GIS Identifiers vs. Display Name Dicts")
# print("="*60)

# # Define which raw_identifier categories map to which display name dictionary
# # Format: { 'Category Key from Log': (target_dict_variable, 'target_dict_name_str') }
# category_to_dict_map = {
#     "High School Names (from 'High' column)": (DISPLAY_NAMES_HIGH, 'DISPLAY_NAMES_HIGH'),
#     "Middle School Names (from 'Middle' column)": (DISPLAY_NAMES_MIDDLE, 'DISPLAY_NAMES_MIDDLE'),
#     "Middle School Names (from 'Name' column)": (DISPLAY_NAMES_MIDDLE, 'DISPLAY_NAMES_MIDDLE'), # Also check against Middle dict
#     "Traditional High Names (from 'Traditiona' column)": (DISPLAY_NAMES_HIGH, 'DISPLAY_NAMES_HIGH'),
#     "Traditional Middle Names (from 'Traditiona' column)": (DISPLAY_NAMES_MIDDLE, 'DISPLAY_NAMES_MIDDLE'),
#     "Traditional Elem Names (from 'Traditiona' column)": (DISPLAY_NAMES_ELEMENTARY, 'DISPLAY_NAMES_ELEMENTARY'),
#     # We EXCLUDE "High School Refs in Elem Zones" because those aren't direct school names for display keys
#     # We EXCLUDE "Choice Zone Names" as they might be programs or full names not matching simple keys
# }

# missing_mappings = []
# processed_normalized_gis_keys = set() # Keep track of normalized keys checked

# # 1. Check if GIS identifiers are present in the corresponding Display Name dicts
# print("\n--- Checking GIS Identifiers against Display Name Dictionaries ---")
# for category, source_set in raw_identifiers.items():
#     if category in category_to_dict_map:
#         target_dict, target_dict_name = category_to_dict_map[category]
#         print(f" -> Processing category: '{category}' against '{target_dict_name}'")
#         for raw_name in source_set:
#             # Normalize the raw name EXACTLY as the main code does for lookup
#             normalized_key = raw_name.strip().upper()
#             processed_normalized_gis_keys.add(normalized_key) # Add for reverse check later

#             if normalized_key not in target_dict:
#                 # Found a raw name in GIS data that's not a key in the expected dict
#                 missing_mappings.append({
#                     'raw_name': raw_name,
#                     'normalized_key': normalized_key,
#                     'source_category': category,
#                     'expected_dict': target_dict_name
#                 })
#                 print(f"    ⚠️ MISSING: Raw='{raw_name}' (Normalized='{normalized_key}') not found as key in {target_dict_name}")
#             # else:
#                 # print(f"    ✅ OK: Raw='{raw_name}' (Normalized='{normalized_key}') found in {target_dict_name}") # Optional success log

# if not missing_mappings:
#     print("\n✅ SUCCESS: All relevant identifiers found in GIS data have corresponding keys in the Display Name dictionaries.")
# else:
#     print("\n❌ WARNING: The following identifiers from GIS files are MISSING keys in the corresponding Display Name dictionaries:")
#     for item in sorted(missing_mappings, key=lambda x: (x['expected_dict'], x['normalized_key'])):
#         print(f"  - Raw Name: \"{item['raw_name']}\" (Normalized Key: \"{item['normalized_key']}\")")
#         print(f"    Source: {item['source_category']}")
#         print(f"    Expected Dictionary: {item['expected_dict']}")
#         print("-" * 10)


# # 2. Check if Display Name Dict keys correspond to found GIS identifiers
# print("\n--- Checking Display Name Dictionary Keys against GIS Identifiers ---")
# unused_display_keys = []
# all_display_dicts = {
#     'DISPLAY_NAMES_HIGH': DISPLAY_NAMES_HIGH,
#     'DISPLAY_NAMES_MIDDLE': DISPLAY_NAMES_MIDDLE,
#     'DISPLAY_NAMES_ELEMENTARY': DISPLAY_NAMES_ELEMENTARY,
# }

# for dict_name, display_dict in all_display_dicts.items():
#     print(f" -> Checking keys in '{dict_name}'...")
#     for key in display_dict.keys():
#             # Normalize the key from the display dict
#             normalized_display_key = key.strip().upper()
#             if normalized_display_key not in processed_normalized_gis_keys:
#                 # Found a key in a display dict that wasn't encountered (in normalized form)
#                 # in the relevant GIS columns we checked earlier.
#                 unused_display_keys.append({
#                     'dict_name': dict_name,
#                     'key': key,
#                     'normalized_key': normalized_display_key
#                 })
#                 print(f"    ❓ UNUSED?: Key='{key}' (Normalized='{normalized_display_key}') not found among processed GIS identifiers.")
#             # else:
#                 # print(f"    ✅ Found: Key='{key}' (Normalized='{normalized_display_key}') corresponds to a GIS identifier.") # Optional

# if not unused_display_keys:
#     print("\n✅ SUCCESS: All keys in the Display Name dictionaries seem to correspond to identifiers found in the relevant GIS columns.")
# else:
#     print("\n❓ INFO: The following keys in Display Name dictionaries were NOT found among the processed identifiers from relevant GIS columns.")
#     print("   (These could be typos, obsolete entries, or identifiers from GIS columns not checked above like Choice/Feeder refs)")
#     for item in sorted(unused_display_keys, key=lambda x: (x['dict_name'], x['key'])):
#         print(f"  - Dictionary: {item['dict_name']}, Key: \"{item['key']}\" (Normalized: \"{item['normalized_key']}\")")


# print("\n" + "="*60)
# print("🔍 END DIAGNOSTIC CROSS-CHECK")
# print("="*60 + "\n")
# --- END: Cross-Check GIS Identifiers ---




# --- Load School Locations from CSV ---
# ... (rest of your script starts here) ...
school_locations_dict = {}
# ...


# --- Load School Locations from CSV ---
school_locations_dict = {} # Initialize empty dictionary
try:
    # Adjust the path if you placed school_locations.csv elsewhere
    csv_path = os.path.join(DATA_DIR, "school_locations.csv")
    print(f"🔄 Loading School Locations from: {csv_path}")
    schools_df = pd.read_csv(csv_path)

    # *** IMPORTANT: Adjust column names 'SchoolName', 'Latitude', 'Longitude'
    #     if they are different in your CSV file! ***
    required_cols = ['SchoolName', 'Latitude', 'Longitude']
    if not all(col in schools_df.columns for col in required_cols):
        raise ValueError(f"CSV missing one or more required columns: {required_cols}")

    for index, row in schools_df.iterrows():
        # Normalize the school name from the CSV for consistent key lookup
        # Match the normalization you'll use when looking up later
        school_name_key = str(row['SchoolName']).strip().lower()
        latitude = row['Latitude']
        longitude = row['Longitude']

        # Store coordinates if they are valid numbers
        if pd.notna(latitude) and pd.notna(longitude):
            school_locations_dict[school_name_key] = (float(latitude), float(longitude))
            # print(f"  Loaded: {school_name_key} -> ({latitude}, {longitude})") # Optional: Verify loading
        else:
            print(f"⚠️ Warning: Missing lat/lon for '{row['SchoolName']}' in CSV.")
            school_locations_dict[school_name_key] = None # Store None if coordinates are missing

    print(f"✅ Successfully loaded coordinates for {len(school_locations_dict)} schools from CSV.")

except FileNotFoundError:
    print(f"❌ ERROR: School locations CSV file not found at {csv_path}. Geocoding will be used as fallback.")
    # Keep school_locations_dict empty, the app will rely on geocoding
except ValueError as ve:
     print(f"❌ ERROR: Issue with CSV columns: {ve}. Geocoding will be used as fallback.")
except Exception as e:
    print(f"❌ ERROR: Failed to load or process school locations CSV: {e}")
    # Keep school_locations_dict empty


app = Flask(__name__)
# Increase timeout for Nominatim requests if needed
geolocator = Nominatim(user_agent="jcps_school_bot", timeout=10)

# --- Helper Functions ---

# Cache for geocoded user addresses
address_cache = {}
school_location_cache = {} # Can still keep this for geocoding fallbacks



def geocode_address(address):
    """Geocodes a user address with caching."""
    if address in address_cache:
        return address_cache[address]

    try:
        location = geolocator.geocode(address)
        if location:
            coords = (location.latitude, location.longitude)
            address_cache[address] = coords
            return coords
        else:
            address_cache[address] = (None, None)
            return None, None
    except Exception as e:
        print(f"⚠️ Geocoding error for address '{address}': {e}")
        address_cache[address] = (None, None) # Cache failure
        return None, None


# --- MODIFIED get_school_coords Function ---
def get_school_coords(school_display_name):
    """
    Gets school coordinates:
    1. From the pre-loaded CSV data (school_locations_dict).
    2. Falls back to checking the runtime cache (school_location_cache).
    3. Falls back to live geocoding if not found in CSV or cache.
    """
    # Normalize the input name to match the keys in school_locations_dict
    clean_name_only = school_display_name.split('](')[0].replace('[', '').strip()
    normalized_key = clean_name_only.lower()

    # 1. Check Pre-loaded CSV data FIRST
    if normalized_key in school_locations_dict:
        coords = school_locations_dict[normalized_key]
        if coords: # Check if coords are not None
            # print(f"✅ Found in CSV: {school_display_name} -> {coords}") # Optional debug log
            return coords
        else:
            # print(f"⚠️ Found in CSV but no coords: {school_display_name}") # Optional debug log
            # Known from CSV that coordinates are missing, don't try geocoding
            return None

    # 2. Check Runtime Cache (for results from previous geocoding attempts)
    if normalized_key in school_location_cache:
        # print(f"CACHE HIT: {school_display_name} -> {school_location_cache[normalized_key]}")
        return school_location_cache[normalized_key]

    # 3. Fallback to Live Geocoding (if not in CSV and not in runtime cache)
    print(f"🤷 School '{school_display_name}' not found in CSV data. Attempting geocode...")
    full_address_guess = f"{clean_name_only}, Louisville, KY"
    print(f"⏳ ATTEMPTING GEOCODE for: '{full_address_guess}'")

    try:
        # time.sleep(1) # Optional delay
        location = geolocator.geocode(full_address_guess)
        if location:
            coords = (location.latitude, location.longitude)
            school_location_cache[normalized_key] = coords # Cache success
            print(f"✅ SUCCESS Geocoding '{full_address_guess}' to {coords}")
            return coords
        else:
            print(f"⚠️ FAILED Geocoding '{full_address_guess}' - Nominatim returned None")
            school_location_cache[normalized_key] = None # Cache failure
            return None
    except Exception as e:
        print(f"❌ ERROR Geocoding '{full_address_guess}': {e}")
        school_location_cache[normalized_key] = None # Cache error
        return None


def find_school_zones(lat, lon, gdf, sort_by_distance=False):
    """
    Finds school zones containing the point and identifies associated schools.
    Calculates distances by geocoding school names if sort_by_distance is True.
    """
    if lat is None or lon is None:
        return {"error": "Invalid coordinates provided."}

    point = Point(lon, lat)
    # Ensure the GDF has a valid spatial index for performance
    if not gdf.sindex:
        print("🛠️ Building spatial index...")
        gdf.sindex # Build it if it doesn't exist

    try:
        # Use spatial index for faster lookup
        possible_matches_index = list(gdf.sindex.intersection(point.bounds))
        possible_matches = gdf.iloc[possible_matches_index]
        # Precise check
        matches = possible_matches[possible_matches.geometry.contains(point)]
    except Exception as e:
        print(f"❌ Error during spatial query: {e}")
        # Fallback to non-indexed query if sindex fails (shouldn't happen often)
        matches = gdf[gdf.geometry.contains(point)]


    if matches.empty:
        print("🤷 No matching zones found for the coordinates.")
        # Optionally, could try finding the nearest zone if needed
        return [] # Return empty list, the calling route handles jsonify

    results = []
    # Use a dictionary where keys are tuples (zone_type, school_name_with_link)
    # to easily manage unique schools and their distances.
    # Value will be the distance.
    school_dict = {}

    # Helper function for adding links
    def make_linked_name(name):
        normalized = name.strip().lower()
        link = SCHOOL_REPORT_LINKS.get(normalized)
        # Fallback: try matching without " High", " Middle", " Elementary" etc.
        if not link:
            base_name = normalized.replace(" high", "").replace(" middle", "").replace(" elementary", "").replace(" school", "").replace(" academy","").strip()
            link = SCHOOL_REPORT_LINKS.get(base_name)

        if link:
            return f"[{name.strip()}]({link})"
        return name.strip() # Return cleaned name if no link


    def add_school(zone_type, display_name):
        """Adds school to dict, calculating distance if needed."""
        if not display_name or display_name == "Unknown":
            return

        name_with_link = make_linked_name(display_name)
        key = (zone_type, name_with_link)
        distance = 0.0 # Default distance

        if sort_by_distance:
            coords = get_school_coords(display_name) # Geocode using the display name
            if coords:
                user_coords = (lat, lon)
                try:
                    distance = geodesic(user_coords, coords).miles
                except ValueError as ve:
                    print(f"⚠️ Distance calculation error for {display_name}: {ve}")
                    distance = float('inf') # Mark as error distance
            else:
                distance = float('inf') # Indicate geocoding failed

        # Add or update school in dict (prefer non-infinite distance if duplicate)
        if key not in school_dict or school_dict[key] == float('inf'):
             school_dict[key] = distance


    # --- Process Matched Zones ---
    print(f"Processing {len(matches)} matched zone(s)...")
    for _, row in matches.iterrows():
        zone_type = row["zone_type"]
        display_name = "Unknown" # Default

        try:
            if zone_type == "High":
                # Assume 'High' column contains the key like "BALLARD"
                raw_name = str(row.get("High", "Unknown"))
                display_name = DISPLAY_NAMES_HIGH.get(raw_name.upper(), f"{raw_name} High" if raw_name != "Unknown" else "Unknown")
                add_school(zone_type, display_name)

            elif zone_type == "Middle":
                 # Assume 'Middle' column contains the name like "Noe"
                 # Or 'Name' might be used in some middle school shapefiles
                 raw_name = row.get("Middle") if pd.notna(row.get("Middle")) else row.get("Name")
                 if raw_name and pd.notna(raw_name):
                     # Basic formatting, assumes raw_name doesn't already contain "Middle"
                     display_name = f"{str(raw_name).strip()} Middle"
                     add_school(zone_type, display_name)

            elif zone_type == "Choice":
                 # Assume 'Name' column holds the choice program/school name
                 raw_name = row.get("Name")
                 if raw_name and pd.notna(raw_name):
                      display_name = str(raw_name).strip()
                      add_school(zone_type, display_name) # Add to 'Choice' category

            elif zone_type == "Elementary":
                # This zone represents an Elementary *Cluster*. Find feeders based on the cluster's associated High School.
                high_school_attr = str(row.get("High", "Unknown")) # Get HS key (e.g., "BALLARD") from the elementary zone row
                feeder_list = ELEMENTARY_FEEDERS.get(high_school_attr.upper(), [])
                print(f"Elementary Cluster (High: {high_school_attr}) -> Feeders: {feeder_list}")
                if feeder_list:
                    for school_name in feeder_list: # school_name is like "Audubon Traditional Elementary"
                         # Feeders go into the main 'Elementary' category for simplicity here
                         add_school("Elementary", school_name)
                else:
                    print(f"⚠️ No elementary feeders found for High School key: {high_school_attr.upper()}")
                    # Optionally add a placeholder if needed:
                    # add_school("Elementary", f"Unknown Feeders for Cluster (HS: {high_school_attr})")


            # --- Traditional / Magnet Zones ---
            # *** Double check the 'Traditiona' column name in your actual data ***
            elif zone_type == "Traditional/Magnet High":
                raw_name = row.get("Traditiona") # Or 'Name', 'School'? Check your shapefile!
                if raw_name and pd.notna(raw_name):
                     display_name = DISPLAY_NAMES_HIGH.get(str(raw_name).upper(), f"{raw_name} High School")
                     add_school(zone_type, display_name)

            elif zone_type == "Traditional/Magnet Middle":
                raw_name = row.get("Traditiona") # Or 'Name', 'School'? Check your shapefile!
                if raw_name and pd.notna(raw_name):
                     display_name = DISPLAY_NAMES_MIDDLE.get(str(raw_name).upper(), f"{raw_name} Middle School")
                     add_school(zone_type, display_name)

            elif zone_type == "Traditional/Magnet Elementary":
                raw_name = row.get("Traditiona") # Or 'Name', 'School'? Check your shapefile!
                if raw_name and pd.notna(raw_name):
                     display_name = DISPLAY_NAMES_ELEMENTARY.get(str(raw_name).upper(), f"{raw_name} Elementary School")
                     add_school(zone_type, display_name)

        except Exception as e:
            print(f"❌ Error processing row for zone type {zone_type}: {e}\nRow data: {row.to_dict()}")


    # --- Add Universal Schools ---
    print("Adding Universal Schools...")
    for level, schools in UNIVERSAL_SCHOOLS.items():
        # Assign to appropriate category (e.g., Trad/Magnet or a dedicated 'Universal' category)
        target_zone_type = f"Traditional/Magnet {level}" # Or change to "Universal Schools"
        for school_name in schools:
            # Attempt to get a display name, otherwise use the name directly
            # Use the combined ALL_DISPLAY_NAMES for broader lookup
            display_name = ALL_DISPLAY_NAMES.get(school_name.upper(), school_name)
            add_school(target_zone_type, display_name)

    # --- Format Final Results ---
    # Group schools by zone type from the dictionary keys
    grouped_schools = {}
    for (zone_type, name_with_link), distance in school_dict.items():
        if zone_type not in grouped_schools:
            grouped_schools[zone_type] = []
        grouped_schools[zone_type].append({"name": name_with_link, "distance": distance})

    # Sort and format each group
    final_results_list = []
    # Define preferred order for categories
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High", "Choice", "Universal Schools"] # Add Universal if used

    # Sort categories based on the preferred order
    sorted_zone_types = sorted(grouped_schools.keys(), key=lambda z: category_order.index(z) if z in category_order else len(category_order))


    for zone_type in sorted_zone_types:
        schools = grouped_schools[zone_type]
        if not schools:
            continue

        # Sort schools: distance (inf last), then name
        schools.sort(key=lambda x: (float('inf') if x['distance'] == float('inf') else x['distance'], x['name'].lower()))

        school_strings = []
        for school in schools:
            name = school['name']
            dist = school['distance']
            if sort_by_distance:
                if dist == float('inf'):
                    school_strings.append(f"{name} (distance unavailable)")
                else:
                    school_strings.append(f"{name} ({dist:.1f} mi)")
            else:
                 school_strings.append(name) # Just the name if not sorting by distance

        if school_strings:
             final_results_list.append(f"{zone_type} Schools: {', '.join(school_strings)}")

    print("✅ School zone processing complete.")
    return final_results_list


# --- Flask Routes ---
@app.route("/test")
def test():
    return "🚀 Flask is working!"

@app.route("/school-zone", methods=["POST"])
def school_zone():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400
    print("🔍 Incoming data /school-zone:", data)

    address = data.get("address")
    print("📬 Received address:", address) # This should be the full address from user

    if not address:
        print("❌ No address provided.")
        return jsonify({"error": "Address is required"}), 400

    # Use the address directly, assuming it's complete
    print(f"🌍 Attempting to geocode: '{address}'") # Add log
    lat, lon = geocode_address(address)

    if lat is None or lon is None:
        # Use the original address in the error message
        error_msg = f"Could not geocode address: '{address}'. Please check the address format (e.g., Street, City, State ZIP)."
        print(f"❌ Geocoding failed for: {address}")
        return jsonify({"error": error_msg}), 400

    print(f"📍 Geocoded to: Lat={lat}, Lon={lon}")
    zones = find_school_zones(lat, lon, all_zones_gdf, sort_by_distance=False)

    return jsonify({"zones": zones}), 200

@app.route("/school-distances", methods=["POST"])
def school_distances():
    data = request.get_json()
    if not data:
        return jsonify({"error": "Request body must be JSON"}), 400
    print("🔍 Incoming data /school-distances:", data)

    address = data.get("address") # This should be the full address from user
    if not address:
        return jsonify({"error": "Address is required"}), 400

    # Use the address directly, assuming it's complete
    print(f"🌍 Attempting to geocode: '{address}'") # Add log
    lat, lon = geocode_address(address)

    if lat is None or lon is None:
        # Use the original address in the error message
        error_msg = f"Could not geocode address: '{address}'. Please check the address format (e.g., Street, City, State ZIP)."
        print(f"❌ Geocoding failed for: {address}")
        return jsonify({"error": error_msg}), 400

    print(f"📍 Geocoded to: Lat={lat}, Lon={lon}")
    zones_with_distances = find_school_zones(lat, lon, all_zones_gdf, sort_by_distance=True)

    return jsonify({"zones": zones_with_distances})


# --- Run the app ---
if __name__ == "__main__":
    # Consider using a production-ready server like Gunicorn or Waitress instead of Flask's built-in server for deployment
    app.run(host="0.0.0.0", port=5001, debug=True) # Turn off debug in production