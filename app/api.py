import os
import json
import time
import sqlite3
from collections import defaultdict

from flask import Flask, request, jsonify
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import pprint
import time
from flask_cors import CORS

# --- Configuration & Data Loading ---
app_start_time = time.time() # Overall start
print(f"[{time.time() - app_start_time:.2f}s] Initializing...")

# --- Configuration & Data Loading ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
print(f"Base Directory: {BASE_DIR}")
print(f"Data Directory: {DATA_DIR}")

# --- Database Configuration ---
DATABASE_PATH = os.path.join(BASE_DIR, 'jcps_school_data.db')
DB_SCHOOLS_TABLE = 'schools'

# Shapefile paths
# ... (keep shapefile paths as before) ...
choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "Resides_HS_Boundaries.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp")
elementary_path = os.path.join(DATA_DIR, "Elementary", "Resides_ES_Clusters_Boundaries.shp")
traditional_middle_path = os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp")
traditional_high_path = os.path.join(DATA_DIR, "TraditionalHigh", "Traditional_HS_Bnds.shp")
traditional_elem_path = os.path.join(DATA_DIR, "TraditionalElementary", "Traditional_ES_Bnds.shp")
mst_middle_path = os.path.join(DATA_DIR, "MagnetMiddle", "MST_MS_Bnds.shp")

# Preview column names
tm_middle_gdf = gpd.read_file(traditional_middle_path)
print("Columns:", tm_middle_gdf.columns)

# Print distinct school names
print("\nTraditional/Magnet Middle Schools:")
print(tm_middle_gdf["Traditiona"].dropna().unique())

# # --- Load Shapefiles ---
# app_start_time is already defined at the top

print(f"[{time.time() - app_start_time:.2f}s] --- Attempting to load shapefiles ---", flush=True)
shapefile_load_overall_start_time = time.time() # For the whole shapefile process

all_zones_gdf = None # Initialize before the try block

try:
    # Define shapefile_configs HERE, inside the try block or just before it if it uses variables defined above.
    # For clarity, defining it right where it's needed is good.
    shapefile_configs = [
        (mst_middle_path, "MST Magnet Middle"),
        (traditional_high_path, "Traditional/Magnet High"),
        (traditional_middle_path, "Traditional/Magnet Middle"),
        (traditional_elem_path, "Traditional/Magnet Elementary"),
        (high_path, "High"),
        (middle_path, "Middle"),
        (elementary_path, "Elementary"),
        (choice_path, "Choice"),
    ]

    gdfs = []
    loaded_files_count = 0
    print(f"[{time.time() - app_start_time:.2f}s] Looking for shapefiles in: {DATA_DIR}", flush=True)

    for path, zone_type in shapefile_configs:
        file_load_iter_start = time.time()
        if os.path.exists(path):
            try:
                gdf = gpd.read_file(path)
                gdf["zone_type"] = zone_type
                gdfs.append(gdf)
                print(f"[{time.time() - app_start_time:.2f}s]   Loaded: {os.path.basename(path)} (took {time.time() - file_load_iter_start:.2f}s)", flush=True)
                loaded_files_count += 1
            except Exception as load_err:
                print(f"[{time.time() - app_start_time:.2f}s]   ❌ Error loading {os.path.basename(path)}: {load_err}", flush=True)
        else:
            print(f"[{time.time() - app_start_time:.2f}s]   ⚠️ Warning: Shapefile not found at {path}", flush=True)

    if not gdfs:
        print(f"[{time.time() - app_start_time:.2f}s] ❌ No valid shapefiles were loaded.", flush=True)
        raise FileNotFoundError("No valid shapefiles loaded, application cannot proceed with GIS operations.") # Re-raise

    # Concatenation and CRS conversion
    concat_start_time = time.time()
    all_zones_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True, sort=False))
    print(f"[{time.time() - app_start_time:.2f}s]   Concatenated GDFs in {time.time() - concat_start_time:.2f}s", flush=True)
    
    crs_convert_start_time = time.time()
    all_zones_gdf = all_zones_gdf.to_crs(epsg=4326)
    print(f"[{time.time() - app_start_time:.2f}s]   Converted to EPSG:4326 in {time.time() - crs_convert_start_time:.2f}s", flush=True)

    print(f"[{time.time() - app_start_time:.2f}s] ✅ Successfully loaded and processed {loaded_files_count} shapefiles.", flush=True)

    # Geometry Cleaning
    print(f"[{time.time() - app_start_time:.2f}s] 🛠️ Cleaning geometries...", flush=True)
    geom_clean_start_time = time.time()
    # Defensive: check if geometry column exists and is not empty
    if 'geometry' in all_zones_gdf.columns and not all_zones_gdf.geometry.empty:
        try:
            all_zones_gdf['geometry'] = all_zones_gdf.geometry.buffer(0)
            print(f"[{time.time() - app_start_time:.2f}s] ✅ Geometries cleaning complete (took {time.time() - geom_clean_start_time:.2f}s).", flush=True)
        except Exception as geom_err:
            print(f"[{time.time() - app_start_time:.2f}s]   ❌ Error cleaning geometries: {geom_err}. Proceeding without cleaning.", flush=True)
    else:
        print(f"[{time.time() - app_start_time:.2f}s]   ⚠️ No geometries to clean or geometry column missing.", flush=True)


    # Spatial Index Building
    print(f"[{time.time() - app_start_time:.2f}s] 🛠️ Building spatial index...", flush=True)
    sindex_build_start_time = time.time()
    # Defensive: ensure gdf is not None and has geometry
    if all_zones_gdf is not None and 'geometry' in all_zones_gdf.columns and not all_zones_gdf.empty:
        # Ensure sindex is fresh if it somehow existed
        if hasattr(all_zones_gdf, '_sindex'): delattr(all_zones_gdf, '_sindex')
        if hasattr(all_zones_gdf, '_sindex_generated'): delattr(all_zones_gdf, '_sindex_generated')
        
        all_zones_gdf.sindex # This triggers the build
        print(f"[{time.time() - app_start_time:.2f}s] ✅ Spatial index built (took {time.time() - sindex_build_start_time:.2f}s).", flush=True)
    else:
        print(f"[{time.time() - app_start_time:.2f}s]   ⚠️ Cannot build spatial index, GeoDataFrame is invalid or empty.", flush=True)
        raise ValueError("Cannot build spatial index on invalid or empty GeoDataFrame.") # Re-raise

    print(f"[{time.time() - app_start_time:.2f}s] --- Shapefile loading and processing complete. Total time: {time.time() - shapefile_load_overall_start_time:.2f}s ---", flush=True)

except FileNotFoundError as fnf:
    print(f"[{time.time() - app_start_time:.2f}s] ❌❌ FATAL ERROR (FileNotFound during shapefile processing): {fnf}.", flush=True)
    raise # Re-raise to let Gunicorn worker fail clearly
except Exception as e:
    print(f"[{time.time() - app_start_time:.2f}s] ❌❌ FATAL ERROR during shapefile loading/processing: {e}", flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr) # Print to stderr for Cloud Run logs
    sys.stderr.flush()
    raise # Re-raise



# --- Constants ---
# Define the bounding box for Jefferson County, KY
JEFFERSON_COUNTY_BOUNDS = {
    "min_lat": 37.9,
    "max_lat": 38.4,
    "min_lon": -86.1,
    "max_lon": -85.3,
}

# --- Database Helper Functions ---
def get_db_connection():
    """Establishes a connection to the database."""
    if not os.path.exists(DATABASE_PATH): print(f"FATAL ERROR: DB not found at {DATABASE_PATH}"); return None
    try: conn = sqlite3.connect(DATABASE_PATH); conn.row_factory = sqlite3.Row; return conn
    except sqlite3.Error as e: print(f"Database connection error: {e}"); return None

def get_info_from_gis(gis_name_key):
    """Looks up SCA and display name from the schools table using the gis_name."""
    # ... (Keep this function exactly as before) ...
    info = {'sca': None, 'display_name': None};
    if not gis_name_key: return info
    lookup_key = str(gis_name_key).strip().upper(); conn = get_db_connection()
    if conn:
        try: 
            cursor = conn.cursor(); sql = f"SELECT school_code_adjusted, display_name FROM {DB_SCHOOLS_TABLE} WHERE gis_name = ?"; cursor.execute(sql, (lookup_key,)); result = cursor.fetchone()
            if result: info['sca'] = result['school_code_adjusted']; info['display_name'] = result['display_name']
        except sqlite3.Error as e: print(f"Error looking up info for GIS key '{lookup_key}': {e}")
        finally: conn.close()
    return info

def get_elementary_feeder_scas(high_school_gis_key):
    """Finds elementary school SCAs feeding into a high school using the DB."""
    # ... (Keep this function exactly as before) ...
    feeder_school_scas = []; hs_info = get_info_from_gis(high_school_gis_key); standard_hs_name = hs_info.get('display_name')
    if not standard_hs_name: return feeder_school_scas
    conn = get_db_connection()
    if conn:
        try: cursor = conn.cursor(); sql = f"SELECT school_code_adjusted FROM {DB_SCHOOLS_TABLE} WHERE feeder_to_high_school = ? AND school_level = ?"; cursor.execute(sql, (standard_hs_name, "Elementary School")); results = cursor.fetchall(); feeder_school_scas = [row['school_code_adjusted'] for row in results if row['school_code_adjusted']]
        except sqlite3.Error as e: print(f"Error querying elementary feeder SCAs for '{standard_hs_name}': {e}")
        finally: conn.close()
    return feeder_school_scas

# ADD THIS NEW FUNCTION IN THE SAME SPOT
# Replace the entire existing function with this one.

def get_address_independent_schools_info():
    """
    Fetches comprehensive details for all schools that have ANY address-independent flag.
    This provides the data needed for the main logic to make a final decision.
    """
    schools_info = []
    # This list includes ALL flags we might need to check
    flag_columns = [
        "universal_magnet_traditional_school",
        "universal_magnet_traditional_program",
        "universal_academies_or_other",
        "the_academies_of_louisville"
    ]
    # We also need the network column to check for a match
    all_needed_cols = ["school_code_adjusted", "display_name", "school_level", "network"] + flag_columns
    select_cols_str = ", ".join(f'"{col}"' for col in sorted(list(set(all_needed_cols))))

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            where_conditions = ' OR '.join([f'"{col}" = ?' for col in flag_columns])
            sql = f"SELECT {select_cols_str} FROM {DB_SCHOOLS_TABLE} WHERE {where_conditions}"
            params = tuple(['Yes'] * len(flag_columns))

            cursor.execute(sql, params)
            results = cursor.fetchall()
            schools_info = [dict(row) for row in results]
            print(f"  DB Query: Found {len(schools_info)} candidate address-independent schools (based on DB flags).")
        except sqlite3.Error as e:
            print(f"❌ Error querying address-independent schools: {e}")
            print(f"  >>> PLEASE VERIFY that all flag columns exist in your '{DB_SCHOOLS_TABLE}' table.")
        finally:
            conn.close()
    return schools_info

# --- UPDATED to select ALL potentially needed columns ---
def get_school_details_by_scas(school_codes_adjusted):
    """Fetches a comprehensive set of details for schools by 'school_code_adjusted'."""
    details_map = {} # Keyed by SCA
    if not school_codes_adjusted: return details_map
    unique_scas = {str(sca).strip() for sca in school_codes_adjusted if sca}
    if not unique_scas: return details_map

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Define ALL columns potentially needed by ANY endpoint/formatter
            select_columns_list = [
                 "address", "african_american_percent", "all_grades_with_preschool_membership",
                    "asian_percent", "attendance_rate", "behavior_events_drugs", "choice_zone", "city",
                    "display_name", "dropout_rate", "economically_disadvantaged_percent", "end_time",
                    "enrollment", "explore_pathways", "explore_pathways_programs",
                    "feeder_to_high_school", "geographical_magnet_traditional", "gifted_talented_percent",
                    "gis_name", "great_schools_rating", "great_schools_url", "high_grade",
                    "hispanic_percent", "ky_reportcard_URL", "latitude", "longitude", "low_grade",
                    "magnet_programs", "math_all_proficient_distinguished",
                    "math_econ_disadv_proficient_distinguished", "membership", "network",
                    "overall_indicator_rating", "parent_satisfaction", "percent_disciplinary_resolutions",
                    "percent_teachers_3_years_or_less_experience", "percent_total_behavior", "phone",
                    "pta_membership_percent", "reading_all_proficient_distinguished",
                    "reading_econ_disadv_proficient_distinguished", "reside", "school_code_adjusted",
                    "school_level", "school_name", "school_website_link", "school_zone", "start_time",
                    "state", "student_teacher_ratio", "student_teacher_ratio_value",
                    "teacher_avg_years_experience", "the_academies_of_louisville",
                    "the_academies_of_louisville_programs", "title_i_status", "total_assault_weapons",
                    "two_or_more_races_percent", "universal_academies_or_other",
                    "universal_magnet_traditional_program", "universal_magnet_traditional_school",
                    "white_percent", "zipcode",
                # Add any other columns from your DB here
            ]
            # Ensure no duplicates (e.g., if added manually and also in list)
            unique_select_columns = sorted(list(set(select_columns_list)))
            select_columns_str = ", ".join(f'"{col}"' for col in unique_select_columns)

            placeholders = ', '.join('?' * len(unique_scas))
            sql = f"SELECT {select_columns_str} FROM {DB_SCHOOLS_TABLE} WHERE school_code_adjusted IN ({placeholders})"

            cursor.execute(sql, tuple(unique_scas))
            results = cursor.fetchall()
            for row in results:
                school_dict = dict(row)
                sca = school_dict.get('school_code_adjusted')
                if sca in unique_scas: details_map[sca] = school_dict
        except sqlite3.Error as e: print(f"Error querying details for SCAs {unique_scas}: {e}")
        finally: conn.close()
    return details_map


# --- Flask App Initialization ---
app = Flask(__name__)
print(f"[{time.time() - app_start_time:.2f}s] Flask app initialized. Gunicorn should take over now.")

CORS(app) # Make sure this is here and applies to the whole app
geolocator = Nominatim(user_agent="jcps_school_bot/1.0 (lkf20@hotmail.com)", timeout=15)

# --- Helper Functions ---
address_cache = {}
def geocode_address(address):
    """
    Geocodes a user address string with caching.
    Returns (lat, lon, error_type)
    error_type can be None (success), 'not_found', or 'service_error'.
    """
    address = str(address).strip()
    if not address:
        print(f"  [API DEBUG GEOCODE] ⚠️ Address input is empty.")
        address_cache[address] = (None, None, 'not_found') # Or 'invalid_input'
        return None, None, 'not_found'

    if address in address_cache:
        cached_lat, cached_lon, cached_error_type = address_cache[address]
        print(f"  [API DEBUG GEOCODE] Cache HIT for '{address}': ({cached_lat}, {cached_lon}, {cached_error_type})")
        return cached_lat, cached_lon, cached_error_type

    print(f"  [API DEBUG GEOCODE] Cache MISS. Geocoding '{address}' via Nominatim...")
    try:
        location = geolocator.geocode(address)
        if location:
            coords = (location.latitude, location.longitude)
            address_cache[address] = (coords[0], coords[1], None) # Cache success
            print(f"  [API DEBUG GEOCODE] ✅ Nominatim success: ({coords[0]:.5f}, {coords[1]:.5f})")
            return coords[0], coords[1], None
        else:
            print(f"  [API DEBUG GEOCODE] ⚠️ Nominatim failed (address not found) for: '{address}'")
            address_cache[address] = (None, None, 'not_found') # Cache "not found"
            return None, None, 'not_found'
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as geo_err:
         print(f"  [API DEBUG GEOCODE] ❌ Nominatim Service ERROR: {geo_err}")
         address_cache[address] = (None, None, 'service_error') # Cache "service error"
         return None, None, 'service_error'
    except Exception as e:
        # Catching a general exception here is okay for robustness,
        # but still classify it as a service_error for the user.
        print(f"  [API DEBUG GEOCODE] ❌ Nominatim UNEXPECTED EXCEPTION: {e}")
        address_cache[address] = (None, None, 'service_error') # Cache "service error"
        return None, None, 'service_error'


# Replace the entire existing function with this new, final version.

# Replace the entire existing function with this new, definitive version.

def find_school_zones_and_details(lat, lon, gdf, sort_key=None, sort_desc=False):
    """Finds zones, uses DB lookups, fetches FULL details by SCA, sorts, and returns structured data."""
    if lat is None or lon is None: print("Error: Invalid user coords."); return None
    point = Point(lon, lat)
    if gdf is None or not hasattr(gdf, 'sindex') or gdf.empty: print("Error: GDF invalid."); return None

    # --- 1. SPATIAL QUERY ---
    matches = gpd.GeoDataFrame()
    try:
        possible_matches_index = list(gdf.sindex.query(point, predicate='contains'))
        if possible_matches_index: matches = gdf.iloc[possible_matches_index]; contains_mask = matches.geometry.contains(point); matches = matches[contains_mask]
    except Exception as e: print(f"❌ Error during spatial query: {e}.")
    if matches.empty: matches = gdf[gdf.geometry.contains(point)]
    print(f"✅ Found {len(matches)} matching zone(s).")

    # --- NEW, IMPROVED DIAGNOSTIC PRINT ---
    print("\n--- ✅ Found Matching GIS Zones ---")
    for index, row in matches.iterrows():
        zone_type = row.get("zone_type", "Unknown")
        school_name = "N/A"
        if zone_type == "Elementary":
            school_name = f"Feeder for {row.get('High', 'N/A')}"
        elif zone_type == "Middle":
            school_name = row.get("Middle", "N/A")
        elif zone_type == "High":
            school_name = row.get("High", "N/A")
        elif zone_type in ["Traditional/Magnet Middle", "Traditional/Magnet High", "Traditional/Magnet Elementary"]:
            school_name = row.get("Traditiona", "N/A")
        elif zone_type == "Choice":
            school_name = row.get("Name", "N/A")
            
        print(f"  - Type: {zone_type:<30} | School/Zone Identified: {school_name}")
    print("----------------------------------\n")
    # --- END OF NEW DIAGNOSTIC ---

    # --- 2. DETERMINE USER'S HOME NETWORK ---
    user_network = None
    for _, row in matches.iterrows():
        if row.get("zone_type") == "High":
            hs_gis_key = str(row.get("High", "")).strip().upper()
            if hs_gis_key:
                hs_info = get_info_from_gis(hs_gis_key)
                if hs_info.get('sca'):
                    hs_details = get_school_details_by_scas([hs_info['sca']]).get(hs_info['sca'])
                    if hs_details:
                        user_network = hs_details.get('network')
                        print(f"  📌 User's Resides Network identified as: '{user_network}' from High School '{hs_gis_key}'")
                break

    # --- 3. IDENTIFY ALL ELIGIBLE SCHOOLS ---
    final_schools_map = defaultdict(set)
    def add_school_to_final_list(sca, zone_type):
        if sca: final_schools_map[str(sca).strip()].add(zone_type)

    # A. Add all GIS-based schools first (Resides schools)
    print("Processing GIS-based schools (from shapefiles)...")
    for _, row in matches.iterrows():
        zone_type = row.get("zone_type", "Unknown")
        gis_key, info = None, None
        if zone_type == "Elementary":
            high_school_gis_key = str(row.get("High", "")).strip().upper()
            for sca in get_elementary_feeder_scas(high_school_gis_key): add_school_to_final_list(sca, zone_type)
            continue
        elif zone_type == "High": gis_key = str(row.get("High", "")).strip().upper()
        elif zone_type == "Middle": gis_key = str(row.get("Middle", "")).strip().upper()
        elif zone_type == "MST Magnet Middle":
            # This handles Farnsley, Meyzeek, Newburg from the MST file
            gis_key = str(row.get("MST", "")).strip().upper() 
        elif zone_type in ["Traditional/Magnet High", "Traditional/Magnet Middle", "Traditional/Magnet Elementary"]:
             gis_key = str(row.get("Traditiona") or row.get("Name", "")).strip().upper()
        elif zone_type == "Choice": gis_key = str(row.get("Name", "")).strip().upper()
        
        if gis_key:
            info = get_info_from_gis(gis_key)
            if info and info.get('sca'): 
                if zone_type == "MST Magnet Middle":
                    add_school_to_final_list(info['sca'], "Traditional/Magnet Middle")
                else:
                    add_school_to_final_list(info['sca'], zone_type)

    # B. Add address-independent schools based on your specific rules
    print("Processing address-independent schools with corrected rules...")
    address_independent_schools = get_address_independent_schools_info()
    for school_info in address_independent_schools:
        sca = school_info.get('school_code_adjusted')
        school_lvl = school_info.get('school_level')
        should_add = False

        # NEW: Check if it qualifies as an Elementary choice school
        if school_lvl == "Elementary School":
            # Any of these flags makes it a choice school.
            # We add all universal/magnet elementary schools for every user.
            if (school_info.get('universal_magnet_traditional_school') == 'Yes' or
                school_info.get('universal_magnet_traditional_program') == 'Yes' or 
                school_info.get('universal_academies_or_other') == 'Yes'):
                should_add = True
        
        # Check if it qualifies as a Middle School choice
        elif school_lvl == "Middle School":
            # For Middle School, ANY of these flags makes it a choice school.
            # This correctly IGNORES the 'the_academies_of_louisville' flag.
            if (school_info.get('universal_magnet_traditional_school') == 'Yes' or
                school_info.get('universal_magnet_traditional_program') == 'Yes' or
                school_info.get('universal_academies_or_other') == 'Yes'):
                should_add = True
        
        # Check if it qualifies as a High School choice
        elif school_lvl == "High School":
            # Rule 1: Is it an Academy of Louisville that ALSO matches the user's network?
            is_network_academy = (
                school_info.get('the_academies_of_louisville') == 'Yes' and
                school_info.get('network') == user_network
            )
            # Rule 2: Does it have any of the truly "universal" flags?
            is_universal = (
                school_info.get('universal_magnet_traditional_school') == 'Yes' or
                school_info.get('universal_magnet_traditional_program') == 'Yes' or
                school_info.get('universal_academies_or_other') == 'Yes' 
            )
            # A high school is added if it meets EITHER of these conditions.
            if is_network_academy or is_universal:
                should_add = True
        
        if should_add:
            # Only add if the school is not already in our list
            if sca not in final_schools_map:
                target_zone_type = None # Start with a clean slate
                if school_lvl == "Elementary School":
                    target_zone_type = "Traditional/Magnet Elementary"
                elif school_lvl == "Middle School":
                    target_zone_type = "Traditional/Magnet Middle"
                elif school_lvl == "High School":
                    target_zone_type = "Traditional/Magnet High"

                if target_zone_type:
                    add_school_to_final_list(sca, target_zone_type)

    # --- 4. FETCH DETAILS AND BUILD FINAL OUTPUT ---
    identified_scas = list(final_schools_map.keys())
    print(f"🔎 Found {len(identified_scas)} unique schools to display. Querying DB for details...")
    if not identified_scas: 
        return {}
    
    school_details_lookup = get_school_details_by_scas(identified_scas)
    print(f"✅ Found details for {len(school_details_lookup)} schools in DB.")
    
    output_structure = {"results_by_zone": []}
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High", "Choice"]
    for zone_type in category_order:
        zone_output = {"zone_type": zone_type, "schools": []}
        for sca, zones in final_schools_map.items():
            if zone_type in zones:
                details = school_details_lookup.get(sca)
                if details:
                    distance = None
                    school_lat, school_lon = details.get('latitude'), details.get('longitude')
                    if school_lat is not None and school_lon is not None:
                        try: distance = round(geodesic((lat, lon), (school_lat, school_lon)).miles, 1)
                        except ValueError: pass
                    details['distance_mi'] = distance
                    zone_output["schools"].append(details)
        if not zone_output["schools"]: continue
        effective_sort_key = sort_key or 'display_name'
        effective_sort_desc = sort_desc if sort_key else False
        if effective_sort_key in zone_output["schools"][0]:
            def sort_helper(item):
                val = item.get(effective_sort_key); is_none = val is None; is_numeric = isinstance(val, (int, float))
                if is_numeric: return (is_none, val if not is_none else (-float('inf') if effective_sort_desc else float('inf')))
                return (is_none, str(val).lower() if not is_none else "")
            try: zone_output["schools"].sort(key=sort_helper, reverse=effective_sort_desc)
            except Exception: zone_output["schools"].sort(key=lambda x: str(x.get('display_name','')).lower())
        else: zone_output["schools"].sort(key=lambda x: str(x.get('display_name','')).lower())
        output_structure["results_by_zone"].append(zone_output)

    print(f"✅ School zone processing complete.")
    return output_structure

# Helper to process request and call core logic
def handle_school_request(sort_key=None, sort_desc=False):
    try: # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< ADD TOP-LEVEL TRY
        start_time = time.time()
        data = request.get_json()
        if not data:
            print("❌ API Error: Request body missing.")
            # This error is fine as is, client expects JSON error here
            return jsonify({"error": "Request body must be JSON"}), 400
        address = data.get("address", "").strip()
        if not address:
            print("❌ API Error: Address missing.")
            # This error is fine as is
            return jsonify({"error": "Address string is required"}), 400
        print(f"\n--- Request {request.path} --- Received Address: '{address}'")

        # --- Geocode Address FIRST ---
        print("[API DEBUG] Calling geocode_address...")
        lat, lon, geocode_error_type = geocode_address(address)
        print(f"[API DEBUG] geocode_address returned: lat={lat}, lon={lon}, error_type={geocode_error_type}")

        user_facing_error_message = None

        if geocode_error_type == 'service_error':
            user_facing_error_message = f"We're experiencing a temporary technical issue trying to locate the address: '{address}'. Please try again in a few moments."
            print(f"[API DEBUG] ❌ Geocoding service error for '{address}'.")
        elif lat is None or lon is None:
            user_facing_error_message = f"Could not determine a specific location for the address: '{address}'. Please ensure the address is correct and complete, or try a nearby landmark."
            print(f"[API DEBUG] ❌ Geocoding failed (address not found or invalid).")
        elif not (JEFFERSON_COUNTY_BOUNDS["min_lat"] <= lat <= JEFFERSON_COUNTY_BOUNDS["max_lat"] and
                  JEFFERSON_COUNTY_BOUNDS["min_lon"] <= lon <= JEFFERSON_COUNTY_BOUNDS["max_lon"]):
            user_facing_error_message = f"The location found for '{address}' appears to be outside the Jefferson County service area. Please provide a local address."
            print(f"[API DEBUG] ❌ Geocoded location ({lat},{lon}) is outside defined bounds.")

        if user_facing_error_message:
            print(f"[API DEBUG] Returning error response: {user_facing_error_message}")
            status_code = 503 if geocode_error_type == 'service_error' else 400
            return jsonify({"error": user_facing_error_message}), status_code
        else:
            print(f"[API DEBUG] ✅ Geocoding successful and within bounds.")

        print(f"📍 Geocoded to: Lat={lat:.5f}, Lon={lon:.5f}")

        print("[API DEBUG] Calling find_school_zones_and_details...")
        # It's possible find_school_zones_and_details could raise an error too
        structured_results = find_school_zones_and_details(lat, lon, all_zones_gdf, sort_key=sort_key, sort_desc=sort_desc)
        
        print("[API DEBUG] Preparing final 200 OK response.")
        response_data = {"query_address": address, "query_lat": lat, "query_lon": lon, **(structured_results or {"results_by_zone": []})}
        end_time = time.time()
        print(f"--- Request {request.path} completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(response_data), 200

    except Exception as e: # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<< CATCH ALL OTHER UNEXPECTED ERRORS
        # Log the full error for server-side debugging
        print(f"❌❌❌ UNHANDLED EXCEPTION in /school-details-by-address endpoint: {e}")
        import traceback
        traceback.print_exc() # This will print the full stack trace to your server logs

        # Return a generic JSON error to the client
        return jsonify({"error": "An unexpected server error occurred. Please try again later."}), 500



# --- Flask Routes ---
@app.route("/test")
def test():
    return "🚀 Flask API (Single Endpoint Style - Structure) is working!"

# --- PRIMARY ENDPOINT ---
@app.route("/school-details-by-address", methods=["POST"])
def school_details_by_address():
    """
    Returns full structured details for schools associated with an address.
    Accepts optional sort parameters in the JSON body.
    Body: {"address": "...", "sort_key": "...", "sort_desc": true/false }
    """
    data = request.get_json()
    sort_key = data.get('sort_key', 'display_name') # Default sort by name
    sort_desc = data.get('sort_desc', False)       # Default sort ascending

    # Validate sort_key against a list of allowed keys if desired for security/robustness
    allowed_sort_keys = [ # Mirror keys available in get_school_details_by_scas + distance_mi
        "display_name", "distance_mi", "great_schools_rating", "parent_satisfaction",
        "enrollment", "membership", "student_teacher_ratio_value", "attendance_rate",
        "dropout_rate", "teacher_avg_years_experience", "percent_total_behavior",
        # Add others as needed
    ]
    if sort_key not in allowed_sort_keys:
        print(f"⚠️ Warning: Invalid sort_key '{sort_key}' received. Defaulting to display_name.")
        sort_key = 'display_name'
        sort_desc = False

    return handle_school_request(sort_key=sort_key, sort_desc=sort_desc)


# --- Optional: Keep old endpoints as convenience wrappers (or remove them) ---
@app.route("/school-zone", methods=["POST"])
def school_zone():
    """Convenience endpoint: Returns schools sorted by name (ascending)."""
    return handle_school_request(sort_key='display_name', sort_desc=False)

@app.route("/school-distances", methods=["POST"])
def school_distances():
    """Convenience endpoint: Returns schools sorted by distance (ASC)."""
    return handle_school_request(sort_key='distance_mi', sort_desc=False)

@app.route("/school-ratings", methods=["POST"])
def school_ratings():
    """Convenience endpoint: Returns schools sorted by Great Schools Rating (DESC)."""
    return handle_school_request(sort_key='great_schools_rating', sort_desc=True)

@app.route("/school-parent-satisfaction", methods=["POST"])
def school_parent_satisfaction():
    """Convenience endpoint: Returns schools sorted by Parent Satisfaction Rating (DESC)."""
    # Ensure this key matches the one selected/available
    return handle_school_request(sort_key='parent_satisfaction', sort_desc=True)

# --- Run App ---
if __name__ == "__main__":
    if not os.path.exists(DATABASE_PATH): print("\n"+"!"*30+f"\n  DB NOT FOUND at '{DATABASE_PATH}'\n  Run setup scripts first!\n"+"!"*30+"\n"); exit()
    else: print(f"✅ Database file found at '{DATABASE_PATH}'")
    print("\n"+"="*30+"\n Starting Flask server...\n Access via http://localhost:5001\n"+"="*30+"\n")
    # For local development only. Gunicorn will be used in production.
    # app.run(host="0.0.0.0", port=port, debug=False)
    app.run(host="0.0.0.0", port=5001, debug=True)