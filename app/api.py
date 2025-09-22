import re
import os
import json
import time
import sqlite3
from collections import defaultdict

from flask import Flask, request, jsonify
from geopy.distance import geodesic
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import pprint
import time
from flask_cors import CORS
import googlemaps
import sys


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

SATELLITE_ZONES_PATH = os.path.join(DATA_DIR, 'satellite_zones.json')
satellite_data = {}
try:
    with open(SATELLITE_ZONES_PATH, 'r') as f:
        satellite_data = json.load(f)
    print(f"✅ Successfully loaded satellite zone data.")
except Exception as e:
    print(f"⚠️ Warning: Could not load satellite_zones.json. Satellite feature will be disabled. Error: {e}")

# --- Load Choice Zone Options Data ---
CHOICE_ZONE_OPTIONS_PATH = os.path.join(DATA_DIR, 'choice_zone_options.json')
choice_zone_data = {}
try:
    with open(CHOICE_ZONE_OPTIONS_PATH, 'r') as f:
        choice_zone_data = json.load(f)
    print(f"✅ Successfully loaded choice zone options data.")
except Exception as e:
    print(f"⚠️ Warning: Could not load {os.path.basename(CHOICE_ZONE_OPTIONS_PATH)}. This feature will be disabled. Error: {e}")



# <<< START: ADDED CODE >>>
# --- Load Zone-Specific Magnet Data ---
ZONE_MAGNETS_PATH = os.path.join(DATA_DIR, 'zone_specific_magnets.json')
zone_specific_magnets_data = {}
try:
    with open(ZONE_MAGNETS_PATH, 'r') as f:
        zone_specific_magnets_data = json.load(f)
    print(f"✅ Successfully loaded zone-specific magnet data.")
except Exception as e:
    print(f"⚠️ Warning: Could not load {os.path.basename(ZONE_MAGNETS_PATH)}. This feature will be disabled. Error: {e}")
# <<< END: ADDED CODE >>>

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

def get_info_from_gis(gis_name_key, school_level_hint=None):
    """
    Looks up SCA and display name from the schools table using the gis_name.
    An optional school_level_hint ('Middle School' or 'High School') can be provided
    to resolve ambiguities for schools with multiple levels.
    """
    info = {'sca': None, 'display_name': None}
    if not gis_name_key: return info
    
    lookup_key = str(gis_name_key).strip().upper()
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            sql = f"SELECT school_code_adjusted, display_name FROM {DB_SCHOOLS_TABLE} WHERE gis_name = ?"
            params = [lookup_key]
            
            if school_level_hint:
                sql += " AND school_level = ?"
                params.append(school_level_hint)
            
            cursor.execute(sql, tuple(params))
            result = cursor.fetchone()
            
            if result:
                info['sca'] = result['school_code_adjusted']
                info['display_name'] = result['display_name']
        except sqlite3.Error as e:
            print(f"Error looking up info for GIS key '{lookup_key}': {e}")
        finally:
            conn.close()
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
    # Add our new flag and ensure the new programs column is selected
    flag_columns = [
        "universal_magnet_traditional_school",
        "universal_magnet_traditional_program",
        "the_academies_of_louisville",
        "districtwide_pathways" # This is now our flag for universal pathways
    ]
    
    # <<< START: MODIFIED CODE >>>
    all_needed_cols = [
        "school_code_adjusted", "display_name", "school_level", "network",
        "districtwide_pathways_programs" # Make sure we fetch the new programs
    ] + flag_columns
    # <<< END: MODIFIED CODE >>>

    select_cols_str = ", ".join(f'"{col}"' for col in sorted(list(set(all_needed_cols))))

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # This logic now checks if any of the flag columns are set to 'Yes'
            where_conditions = ' OR '.join([f'"{col}" = "Yes"' for col in flag_columns])
            sql = f"SELECT {select_cols_str} FROM {DB_SCHOOLS_TABLE} WHERE {where_conditions}"
            
            cursor.execute(sql)
            results = cursor.fetchall()
            schools_info = [dict(row) for row in results]
            
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
                # <<< START: ADDED CODE >>>
                "districtwide_pathways", 
                "districtwide_pathways_programs"
                # <<< END: ADDED CODE >>>
            ]
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

# --- NEW: Google Maps Client Initialization ---
# It's best practice to get the key from an environment variable
GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY')
if not GOOGLE_MAPS_API_KEY:
    print("⚠️ WARNING: GOOGLE_MAPS_API_KEY environment variable not set.")
gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
# --- END NEW ---

# --- Helper Functions ---
address_cache = {}
def geocode_address(address):
    """
    Geocodes an address using the Google Maps API with caching and a bounding box.
    Returns (lat, lon, error_type).
    """
    address = str(address).strip()
    if not address:
        return None, None, 'not_found'

    if address in address_cache:
        cached_lat, cached_lon, cached_error_type = address_cache[address]
        
        return cached_lat, cached_lon, cached_error_type

    
    try:
        # Use the bounds to hint to Google where to look
        jc_bounds = {
            "northeast": (JEFFERSON_COUNTY_BOUNDS["max_lat"], JEFFERSON_COUNTY_BOUNDS["max_lon"]),
            "southwest": (JEFFERSON_COUNTY_BOUNDS["min_lat"], JEFFERSON_COUNTY_BOUNDS["min_lon"])
        }
        
        results = gmaps.geocode(address, bounds=jc_bounds)
        
        if results:
            location = results[0]['geometry']['location']
            coords = (location['lat'], location['lng'])
            
            # Optional but recommended: Check if the result is actually in our bounds
            if not (JEFFERSON_COUNTY_BOUNDS["min_lat"] <= coords[0] <= JEFFERSON_COUNTY_BOUNDS["max_lat"] and
                    JEFFERSON_COUNTY_BOUNDS["min_lon"] <= coords[1] <= JEFFERSON_COUNTY_BOUNDS["max_lon"]):
                
                address_cache[address] = (None, None, 'not_found')
                return None, None, 'not_found'

            address_cache[address] = (coords[0], coords[1], None)
            print(f"  [API DEBUG GEOCODE] ✅ Google success: ({coords[0]:.5f}, {coords[1]:.5f})")
            return coords[0], coords[1], None
        else:
            print(f"  [API DEBUG GEOCODE] ❌ Google failed (address not found) for: '{address}'")
            address_cache[address] = (None, None, 'not_found')
            return None, None, 'not_found'

    except Exception as e:
        print(f"  [API DEBUG GEOCODE] ❌ Google API UNEXPECTED EXCEPTION: {e}")
        address_cache[address] = (None, None, 'service_error')
        return None, None, 'service_error'


def find_school_zones_and_details(lat, lon, gdf, sort_key=None, sort_desc=False):
    """Finds all zones, adds satellite/choice schools, fetches details, and returns structured data."""
    if lat is None or lon is None: print("Error: Invalid user coords."); return None, False
    point = Point(lon, lat)
    matches = gdf[gdf.geometry.contains(point)]
    
    user_reside_high_school_zone_name = None
    user_network = None
    is_in_choice_zone = any(row.get("zone_type") == "Choice" for _, row in matches.iterrows())
    
    if is_in_choice_zone: print("  [API DEBUG] User location IS within the Choice Zone.")

    for _, row in matches.iterrows():
        if row.get("zone_type") == "High":
            hs_gis_key = str(row.get("High", "")).strip().upper()
            if hs_gis_key:
                hs_info = get_info_from_gis(hs_gis_key, school_level_hint="High School")
                if hs_info.get('sca'):
                    hs_details = get_school_details_by_scas([hs_info['sca']]).get(hs_info['sca'])
                    if hs_details:
                        user_network = hs_details.get('network')
                        user_reside_high_school_zone_name = hs_details.get('school_zone')
                        print(f"  📌 User's Reside High School Zone: '{user_reside_high_school_zone_name}' | Network: '{user_network}'")
                break 

    final_schools_map = defaultdict(dict)
    def add_school(sca, zone_type, status):
        if sca:
            current_priority = {"Academy Choice": 1, "Magnet/Choice Program": 2, "Satellite School": 3, "Reside": 4}
            existing_status = final_schools_map.get(sca, {}).get('status', '')
            if current_priority.get(status, 0) >= current_priority.get(existing_status, 0):
                final_schools_map[sca]['zone_type'] = zone_type
                final_schools_map[sca]['status'] = status

    # GIS-based schools
    for _, row in matches.iterrows():
        zone_type = row.get("zone_type"); gis_key = None; info = None; level_hint = None; current_status = "Reside"
        if "High" in zone_type: level_hint = "High School"
        elif "Middle" in zone_type: level_hint = "Middle School"
        if zone_type == "Elementary":
            for sca in get_elementary_feeder_scas(str(row.get("High", "")).strip().upper()): add_school(sca, 'Elementary', 'Reside')
            continue
        elif zone_type in ["High", "Middle"]: gis_key = str(row.get(zone_type, "")).strip().upper()
        else: 
            current_status = "Magnet/Choice Program"
            if zone_type == "MST Magnet Middle": gis_key = str(row.get("MST", "")).strip().upper(); zone_type = "Traditional/Magnet Middle"
            elif zone_type in ["Traditional/Magnet High", "Traditional/Magnet Middle", "Traditional/Magnet Elementary"]: gis_key = str(row.get("Traditiona", "")).strip().upper()
            elif zone_type == "Choice": continue
        if gis_key:
            info = get_info_from_gis(gis_key, school_level_hint=level_hint)
            if info and info.get('sca'): add_school(info['sca'], zone_type, current_status)

    # JSON-based schools
    if user_reside_high_school_zone_name and user_reside_high_school_zone_name in satellite_data:
        for school_info in satellite_data[user_reside_high_school_zone_name]: add_school(school_info.get('school_code_adjusted'), "Traditional/Magnet Elementary", "Satellite School")
    if user_reside_high_school_zone_name and user_reside_high_school_zone_name in zone_specific_magnets_data:
        for school_info in zone_specific_magnets_data[user_reside_high_school_zone_name]: add_school(school_info.get('school_code_adjusted'), "Traditional/Magnet Elementary", "Magnet/Choice Program")
    if is_in_choice_zone and user_reside_high_school_zone_name in choice_zone_data:
        for school_info in choice_zone_data[user_reside_high_school_zone_name].get("Elementary", []): add_school(school_info.get('school_code_adjusted'), "Elementary", "Reside")
        for school_info in choice_zone_data[user_reside_high_school_zone_name].get("Middle", []): add_school(school_info.get('school_code_adjusted'), "Middle", "Reside")

    # Database-flag based schools
    address_independent_schools = get_address_independent_schools_info()
    for school_info in address_independent_schools:
        sca = school_info.get('school_code_adjusted')
        school_lvl = school_info.get('school_level')
        status = None
        is_districtwide_pathway = school_info.get('districtwide_pathways') == 'Yes'
        is_universal_magnet = school_info.get('universal_magnet_traditional_school') == 'Yes' or school_info.get('universal_magnet_traditional_program') == 'Yes' or school_info.get('choice_zone') == 'Yes'
        is_academy_choice = school_info.get('the_academies_of_louisville') == 'Yes' and school_info.get('network') == user_network
        if is_districtwide_pathway: status = "Magnet/Choice Program"
        elif is_academy_choice: status = "Academies of Louisville"
        elif is_universal_magnet: status = "Magnet/Choice Program"
        if status:
            zone_type = {"Elementary School": "Traditional/Magnet Elementary", "Middle School": "Traditional/Magnet Middle", "High School": "Traditional/Magnet High"}.get(school_lvl)
            if zone_type: add_school(sca, zone_type, status)
    
    # Final assembly
    identified_scas = list(final_schools_map.keys())
    school_details_lookup = get_school_details_by_scas(identified_scas)
    schools_by_zone_type = defaultdict(list)
    for sca, info in final_schools_map.items():
        details = school_details_lookup.get(sca)
        if details:
            details['display_status'] = info['status']
            details['distance_mi'] = round(geodesic((lat, lon), (details['latitude'], details['longitude'])).miles, 1) if details.get('latitude') else None
            
            # <<< START: MODIFIED CODE >>>
            # Add explicit program type and program list to the final school object
            details['display_program_type'] = None
            details['display_programs'] = None
            if details['display_status'] == 'Magnet/Choice Program':
                if details.get('districtwide_pathways') == 'Yes' and details.get('districtwide_pathways_programs'):
                    details['display_program_type'] = 'Districtwide Pathway'
                    details['display_programs'] = details['districtwide_pathways_programs']
                elif details.get('magnet_programs'):
                    details['display_program_type'] = 'Magnet'
                    details['display_programs'] = details['magnet_programs']
            elif details['display_status'] == 'Academies of Louisville' or (details['display_status'] == 'Reside' and details.get('the_academies_of_louisville_programs')):
                details['display_program_type'] = 'Academies of Louisville'
                details['display_programs'] = details.get('the_academies_of_louisville_programs')
            # <<< END: MODIFIED CODE >>>

            final_zone_type = info['zone_type']
            if info['status'] == 'Reside':
                school_lvl = details.get('school_level')
                if school_lvl == "Elementary School": final_zone_type = "Elementary"
                elif school_lvl == "Middle School": final_zone_type = "Middle"
                elif school_lvl == "High School": final_zone_type = "High"
            schools_by_zone_type[final_zone_type].append(details)

    output_structure = {"results_by_zone": []}
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High"]
    for zone_type in category_order:
        if schools_by_zone_type[zone_type]:
            schools = schools_by_zone_type[zone_type]
            schools.sort(key=lambda x: (x.get('distance_mi') is None, x.get('distance_mi', float('inf'))))
            output_structure["results_by_zone"].append({"zone_type": zone_type, "schools": schools})
    return output_structure, is_in_choice_zone

# Helper to process request and call core logic
def handle_school_request(sort_key=None, sort_desc=False):
    try:
        start_time = time.time()
        data = request.get_json()
        if not data:
            return jsonify({"error": "Request body must be JSON"}), 400
        address = data.get("address", "").strip()
        if not address:
            return jsonify({"error": "Address string is required"}), 400
        print(f"\n--- Request {request.path} --- Received Address: '{address}'")

        lat, lon, geocode_error_type = geocode_address(address)
        user_facing_error_message = None

        if geocode_error_type == 'service_error':
            user_facing_error_message = f"We're experiencing a temporary technical issue trying to locate the address: '{address}'. Please try again in a few moments."
        elif lat is None or lon is None:
            user_facing_error_message = f"Could not determine a specific location for the address: '{address}'. Please ensure the address is correct and complete, or try a nearby landmark."
        elif not (JEFFERSON_COUNTY_BOUNDS["min_lat"] <= lat <= JEFFERSON_COUNTY_BOUNDS["max_lat"] and
                  JEFFERSON_COUNTY_BOUNDS["min_lon"] <= lon <= JEFFERSON_COUNTY_BOUNDS["max_lon"]):
            user_facing_error_message = f"The location found for '{address}' appears to be outside the Jefferson County service area. Please provide a local address."

        if user_facing_error_message:
            status_code = 503 if geocode_error_type == 'service_error' else 400
            return jsonify({"error": user_facing_error_message}), status_code

        # <<< START: MODIFIED CODE >>>
        # Unpack the tuple returned by the main logic function
        structured_results, is_in_choice_zone = find_school_zones_and_details(lat, lon, all_zones_gdf, sort_key=sort_key, sort_desc=sort_desc)
        
        # Add the new flag to the response data
        response_data = {
            "query_address": address, 
            "query_lat": lat, 
            "query_lon": lon, 
            "is_in_choice_zone": is_in_choice_zone, # <-- NEW FLAG
            **(structured_results or {"results_by_zone": []})
        }
        # <<< END: MODIFIED CODE >>>
        
        end_time = time.time()
        print(f"--- Request {request.path} completed in {end_time - start_time:.2f} seconds ---")
        return jsonify(response_data), 200

    except Exception as e:
        print(f"❌❌❌ UNHANDLED EXCEPTION in /school-details-by-address endpoint: {e}")
        import traceback
        traceback.print_exc() 
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

@app.route("/school-details-by-coords", methods=["POST"])
def school_details_by_coords():
    """Endpoint for testing that bypasses geocoding."""
    start_time = time.time()
    data = request.get_json()
    if not data or 'lat' not in data or 'lon' not in data:
        return jsonify({"error": "lat and lon are required"}), 400
    lat, lon = data['lat'], data['lon']
    print(f"\n--- Request /school-details-by-coords --- Coords: ({lat}, {lon})")
    # For testing, the address string is just for human-readable context
    address_str = data.get("address", f"Coord lookup: {lat}, {lon}")
    response_data = handle_school_request(lat, lon, address_str, data.get('sort_key'), data.get('sort_desc'))
    print(f"--- Request completed in {time.time() - start_time:.2f} seconds ---")
    return jsonify(response_data)

@app.route("/generate-test-case-by-coords", methods=["POST"])
def generate_test_case_by_coords():
    """Test case generator that uses coordinates directly."""
    data = request.get_json()
    lat, lon, zone_name, address = data.get('lat'), data.get('lon'), data.get('zone_name'), data.get('address')
    if not all([lat, lon, zone_name, address]):
        return jsonify({"error": "lat, lon, zone_name, and address are required"}), 400
    
    structured_results, _ = find_school_zones_and_details(lat, lon, all_zones_gdf)
    
    expected_schools = {"Elementary": [], "Middle": [], "High": []}
    safe_results = structured_results or {}
    for zone in safe_results.get("results_by_zone", []):
        level = None
        if "Elementary" in zone.get("zone_type", ""): level = "Elementary"
        elif "Middle" in zone.get("zone_type", ""): level = "Middle"
        elif "High" in zone.get("zone_type", ""): level = "High"
        if level:
            for school in zone.get("schools", []):
                expected_schools[level].append({
                    "display_name": school.get("display_name"),
                    "expected_status": school.get("display_status")
                })
    for level in expected_schools:
        expected_schools[level].sort(key=lambda x: x['display_name'])
    
    test_case_output = {
        "zone_name": zone_name,
        "address": address, # Keep address for context
        "lat": lat,         # Add lat
        "lon": lon,         # Add lon
        "expected_schools": expected_schools
    }
    return jsonify(test_case_output)

@app.route("/generate-test-case", methods=["POST"])
def generate_test_case():
    """
    A temporary helper endpoint to generate JSON for the test_cases.json file.
    It takes an address and a zone name, and returns the formatted test case.
    """
    data = request.get_json()
    address = data.get("address")
    zone_name = data.get("zone_name")

    if not address or not zone_name:
        return jsonify({"error": "Address and zone_name are required"}), 400

    # --- This reuses your existing core logic ---
    lat, lon, _ = geocode_address(address)
    if lat is None:
        # Return an error object that the runner script can check
        return jsonify({"error": f"Could not geocode address: {address}"}), 400
    
    structured_results, _ = find_school_zones_and_details(lat, lon, all_zones_gdf)
    
    # --- Format the output ---
    expected_schools = {
        "Elementary": [],
        "Middle": [],
        "High": []
    }

    safe_results = structured_results or {}
    for zone in safe_results.get("results_by_zone", []):
        zone_type = zone.get("zone_type")
        level = None
        if "Elementary" in zone_type: level = "Elementary"
        elif "Middle" in zone_type: level = "Middle"
        elif "High" in zone_type: level = "High"
        
        if level:
            for school in zone.get("schools", []):
                expected_schools[level].append({
                    "display_name": school.get("display_name"),
                    "expected_status": school.get("display_status")
                })

    for level in expected_schools:
        expected_schools[level].sort(key=lambda x: x['display_name'])

    test_case_output = {
        "zone_name": zone_name,
        "address": address,
        "expected_schools": expected_schools
    }

    # <<< START: MODIFIED CODE >>>
    # Instead of printing, return the JSON object directly
    return jsonify(test_case_output), 200
    # <<< END: MODIFIED CODE >>> 

# --- Run App ---
if __name__ == "__main__":
    if not os.path.exists(DATABASE_PATH): print("\n"+"!"*30+f"\n  DB NOT FOUND at '{DATABASE_PATH}'\n  Run setup scripts first!\n"+"!"*30+"\n"); exit()
    else: print(f"✅ Database file found at '{DATABASE_PATH}'")
    print("\n"+"="*30+"\n Starting Flask server...\n Access via http://localhost:5001\n"+"="*30+"\n")
    # For local development only. Gunicorn will be used in production.
    # app.run(host="0.0.0.0", port=port, debug=False)
    app.run(host="0.0.0.0", port=5001, debug=True)