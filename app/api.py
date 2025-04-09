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

# --- Load Shapefiles ---
print("🔄 Loading Shapefiles...")
all_zones_gdf = None
try:
    shapefile_configs = [
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
    # Loop through each configured shapefile path
    for path, zone_type in shapefile_configs:
        if os.path.exists(path):
            # Try reading the file if it exists
            try:
                gdf = gpd.read_file(path)
                gdf["zone_type"] = zone_type
                gdfs.append(gdf)
                print(f"  Loaded: {os.path.basename(path)}")
                loaded_files_count += 1
            # Catch errors during file reading
            except Exception as load_err:
                print(f"  ❌ Error loading {os.path.basename(path)}: {load_err}")
        # Handle case where the file path doesn't exist
        else:
            print(f"  ⚠️ Warning: Shapefile not found at {path}")

    # Check if any shapefiles were successfully loaded after the loop
    if not gdfs:
        raise FileNotFoundError("No valid shapefiles were loaded.") # Raise error if none loaded

    # Concatenate all loaded GeoDataFrames into one
    all_zones_gdf = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True, sort=False)
    ).to_crs(epsg=4326) # Ensure WGS84 CRS
    print(f"✅ Successfully loaded and merged {loaded_files_count} shapefiles.")

    # Attempt to clean geometries (optional but good practice)
    print("🛠️ Cleaning geometries...")
    try:
        # buffer(0) can sometimes fix minor geometry issues
        all_zones_gdf['geometry'] = all_zones_gdf.geometry.buffer(0)
        print("✅ Geometries cleaning complete.") # Moved print to its own line
    except Exception as geom_err:
        print(f"  ❌ Error cleaning: {geom_err}.") # Log error but continue

    # Build the spatial index for fast querying
    print("🛠️ Building spatial index...")
    # Force sindex regeneration if it exists from previous operations
    if hasattr(all_zones_gdf, '_sindex'):
        delattr(all_zones_gdf, '_sindex')
    if hasattr(all_zones_gdf, '_sindex_generated'):
        delattr(all_zones_gdf, '_sindex_generated')
    all_zones_gdf.sindex # Accessing it triggers the build
    print("✅ Spatial index built.")

# Catch specific error if no shapefiles were found
except FileNotFoundError as fnf:
    print(f"❌❌ FATAL ERROR: {fnf}.")
    exit() # Stop the script

# Catch any other general exceptions during the whole shapefile process
except Exception as e:
    print(f"❌❌ FATAL ERROR loading/processing shapefiles: {e}")
    exit() # Stop the script


# --- Database Helper Functions ---
def get_db_connection():
    """Establishes a connection to the database."""
    # ... (Keep get_db_connection as before) ...
    if not os.path.exists(DATABASE_PATH): print(f"FATAL ERROR: DB not found at {DATABASE_PATH}"); return None
    try: conn = sqlite3.connect(DATABASE_PATH); conn.row_factory = sqlite3.Row; return conn
    except sqlite3.Error as e: print(f"Database connection error: {e}"); return None

# --- NEW Function: Lookup SCA and display_name from gis_name in the main schools table ---
def get_info_from_gis(gis_name_key):
    """Looks up SCA and display name from the schools table using the gis_name."""
    info = {'sca': None, 'display_name': None} # Return dict
    if not gis_name_key:
        return info

    lookup_key = str(gis_name_key).strip().upper() # Match how gis_name is likely stored
    conn = get_db_connection()

    if conn:
        try:
            cursor = conn.cursor()
            # Query the main table using the gis_name column
            sql = f"SELECT school_code_adjusted, display_name FROM {DB_SCHOOLS_TABLE} WHERE gis_name = ?"
            cursor.execute(sql, (lookup_key,))
            result = cursor.fetchone() # Get the first (should be only) matching row

            # If a result was found, populate the info dictionary
            if result:
                info['sca'] = result['school_code_adjusted']
                info['display_name'] = result['display_name']

        # Catch potential database errors during the query
        except sqlite3.Error as e:
            print(f"Error looking up info for GIS key '{lookup_key}': {e}")
        # Ensure the connection is always closed, whether error or success
        finally:
            conn.close()

    # Return the info dictionary (will contain None if lookup failed or DB error)
    # if not info['sca']: print(f"  DB Lookup MISSING for GIS Key: '{lookup_key}'") # Optional debug
    return info

def get_elementary_feeder_scas(high_school_gis_key):
    """Finds elementary school SCAs feeding into a high school using the DB."""
    # ... (Keep get_elementary_feeder_scas as before) ...
    feeder_school_scas = []; hs_info = get_info_from_gis(high_school_gis_key); standard_hs_name = hs_info.get('display_name')
    if not standard_hs_name: return feeder_school_scas
    conn = get_db_connection()
    if conn:
        try: cursor = conn.cursor(); sql = f"SELECT school_code_adjusted FROM {DB_SCHOOLS_TABLE} WHERE feeder_to_high_school = ? AND school_level = ?"; cursor.execute(sql, (standard_hs_name, "Elementary School")); results = cursor.fetchall(); feeder_school_scas = [row['school_code_adjusted'] for row in results if row['school_code_adjusted']]
        except sqlite3.Error as e: print(f"Error querying elementary feeder SCAs for '{standard_hs_name}': {e}")
        finally: conn.close()
    return feeder_school_scas

def get_universal_magnet_scas_and_info():
    """ Fetches SCA, display_name, and school_level for all universal magnets. """
    # ... (Keep get_universal_magnet_scas_and_info as before) ...
    universal_magnets_info = []; conn = get_db_connection()
    if conn:
        try: cursor = conn.cursor(); sql = f"SELECT school_code_adjusted, display_name, school_level FROM {DB_SCHOOLS_TABLE} WHERE universal_magnet = ?"; cursor.execute(sql, ('Yes',)); results = cursor.fetchall(); universal_magnets_info = [dict(row) for row in results]; print(f"  DB Query: Found {len(universal_magnets_info)} universal magnets.")
        except sqlite3.Error as e: print(f"Error querying universal magnets: {e}")
        finally: conn.close()
    return universal_magnets_info

def get_school_details_by_scas(school_codes_adjusted):
    """Fetches selected details for a list of schools by 'school_code_adjusted'."""
    details_map = {} # Keyed by SCA
    if not school_codes_adjusted: return details_map
    unique_scas = {str(sca).strip() for sca in school_codes_adjusted if sca}
    if not unique_scas: return details_map

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            select_columns_list = [
                "school_code_adjusted", "school_name", "display_name", "type", "zone", "gis_name",
                "feeder_to_high_school", "network", "great_schools_rating", "great_schools_url",
                "school_level", "enrollment", "student_teacher_ratio", "student_teacher_ratio_value",
                "attendance_rate", "dropout_rate", "school_website_link", "ky_reportcard_URL",
                "low_grade", "high_grade", "title_i_status", "address", "city", "state", "zipcode", "phone",
                "latitude", "longitude", "universal_magnet",
                "parent_satisfaction" 
                # Add any other relevant parent satisfaction columns if needed
            ]
            select_columns_str = ", ".join(f'"{col}"' for col in select_columns_list) # Quote names
            placeholders = ', '.join('?' * len(unique_scas))
            sql = f"SELECT {select_columns_str} FROM {DB_SCHOOLS_TABLE} WHERE school_code_adjusted IN ({placeholders})"

            cursor.execute(sql, tuple(unique_scas))
            results = cursor.fetchall()
            for row in results:
                school_dict = dict(row)
                sca = school_dict.get('school_code_adjusted')
                if sca in unique_scas: details_map[sca] = school_dict # Key by SCA
        except sqlite3.Error as e: print(f"Error querying details for SCAs {unique_scas}: {e}")
        finally: conn.close()
    return details_map


# --- Flask App Initialization ---
app = Flask(__name__)
geolocator = Nominatim(user_agent="jcps_school_bot/1.0 (your_email@example.com)", timeout=15)

# --- Helper Functions ---
address_cache = {}
def geocode_address(address):
    # ... (Keep geocode_address function exactly as before) ...
    address = str(address).strip();
    if not address: return None, None
    if address in address_cache: return address_cache[address]
    print(f"  -> Geocoding user address: '{address}'")
    try:
        location = geolocator.geocode(address)
        if location: coords = (location.latitude, location.longitude); address_cache[address] = coords; print(f"  ✅ Geocode success: {address} -> ({coords[0]:.5f}, {coords[1]:.5f})"); return coords
        else: print(f"  ⚠️ Geocoding failed (no result): '{address}'"); address_cache[address] = (None, None); return None, None
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as geo_err: print(f"  ❌ Geocoding Service ERROR for '{address}': {geo_err}"); address_cache[address] = (None, None); return None, None
    except Exception as e: print(f"  ❌ Geocoding UNEXPECTED EXCEPTION for '{address}': {e}"); address_cache[address] = (None, None); return None, None

# --- REFACTORED Main Logic Function with Sorting ---
def find_school_zones_and_details(lat, lon, gdf, sort_key=None, sort_desc=False): # Added sort parameters
    """Finds zones, uses DB lookups, fetches details, sorts, returns STRUCTURED data."""
    # Initial checks & Spatial query
    # ... (Keep this part exactly as before, up to finding matches) ...
    if lat is None or lon is None: print("Error: Invalid user coords."); return None
    point = Point(lon, lat)
    if gdf is None or not hasattr(gdf, 'sindex') or gdf.empty: print("Error: GDF invalid."); return None
    matches = gpd.GeoDataFrame()
    try:
        possible_matches_index = list(gdf.sindex.query(point, predicate='contains'))
        if possible_matches_index: matches = gdf.iloc[possible_matches_index]; contains_mask = matches.geometry.contains(point); matches = matches[contains_mask]
        if not matches.empty: print(f"  ℹ️ Spatial index found {len(matches)} precise matches.")
        else: print("  ℹ️ Spatial index found no containing polygons.")
        if matches.empty: print(f"  ℹ️ Falling back..."); matches = gdf[gdf.geometry.contains(point)]
        if not matches.empty: print(f"  ℹ️ Fallback check found {len(matches)} matches.")
    except Exception as e: print(f"❌ Error during spatial query: {e}. Trying fallback."); 
    try: matches = gdf[gdf.geometry.contains(point)] 
    except Exception as fe: print(f"❌ Final fallback error: {fe}"); matches = gpd.GeoDataFrame()

    print(f"✅ Found {len(matches)} matching zone(s). Identifying schools...")

    # Step 1: Identify SCAs and map them to zone types
    # ... (Keep this identification logic exactly as before, collecting SCAs
    #      and mapping them in sca_to_zone_types_map using get_info_from_gis,
    #      get_elementary_feeder_scas, and get_universal_magnet_scas_and_info) ...
    identified_scas = set(); sca_to_zone_types_map = defaultdict(set)
    def add_sca_to_map(zone_type, sca):
        if sca: sca_str = str(sca).strip(); identified_scas.add(sca_str); sca_to_zone_types_map[sca_str].add(zone_type)
    for _, row in matches.iterrows():
        zone_type = row.get("zone_type", "Unknown");
        try:
            gis_key = None; determined_sca = None; determined_display_name = None
            if zone_type == "Elementary": high_school_gis_key = str(row.get("High", "")).strip().upper(); [add_sca_to_map(zone_type, sca) for sca in get_elementary_feeder_scas(high_school_gis_key)]; continue
            elif zone_type in ["High", "Traditional/Magnet High"]: gis_key = str(row.get("High") or row.get("Traditiona", "")).strip().upper()
            elif zone_type in ["Middle", "Traditional/Magnet Middle"]: raw_middle = str(row.get("Middle", "")).strip().upper(); raw_name_col = str(row.get("Name", "")).strip().upper(); gis_key = raw_middle if raw_middle else raw_name_col
            elif zone_type == "Traditional/Magnet Elementary": gis_key = str(row.get("Traditiona", "")).strip().upper()
            elif zone_type == "Choice": potential_key = str(row.get("Name", "")).strip().upper(); info = get_info_from_gis(potential_key); determined_sca = info.get('sca')
            else: continue
            if gis_key and not determined_sca: info = get_info_from_gis(gis_key); determined_sca = info.get('sca');
            if not determined_sca and gis_key and zone_type != "Choice": print(f"  ⚠️ DB Lookup MISSING: Zone='{zone_type}', GIS Key='{gis_key}'")
            if determined_sca: add_sca_to_map(zone_type, determined_sca)
        except Exception as e: print(f"❌ Error processing row (Index: {row.name}, Zone: {zone_type}): {e}")
    print("Adding Universal Magnet schools from DB...")
    universal_magnets_info = get_universal_magnet_scas_and_info()
    for info in universal_magnets_info:
        sca = info.get('school_code_adjusted'); school_lvl = info.get('school_level'); target_zone_type = "Unknown Magnet"
        if school_lvl == "Elementary School": target_zone_type = "Traditional/Magnet Elementary"
        elif school_lvl == "Middle School": target_zone_type = "Traditional/Magnet Middle"
        elif school_lvl == "High School": target_zone_type = "Traditional/Magnet High"
        add_sca_to_map(target_zone_type, sca)


    # Step 2: Fetch details using SCAs
    print(f"🔎 Identified {len(identified_scas)} unique SCAs. Querying DB...")
    if not identified_scas: print("🤷 No SCAs identified."); return {}
    school_details_lookup = get_school_details_by_scas(list(identified_scas))
    print(f"✅ Found details for {len(school_details_lookup)} schools in DB.")

    # --- Step 3: Build STRUCTURED Output ---
    output_structure = {"results_by_zone": []}
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High", "Choice"]

    for zone_type in category_order:
        zone_output = {"zone_type": zone_type, "schools": []}
        scas_for_this_zone = {sca for sca, zones in sca_to_zone_types_map.items() if zone_type in zones}

        for sca in scas_for_this_zone:
            details = school_details_lookup.get(sca)
            if details:
                # Always calculate distance for potential sorting later
                distance = None
                school_lat = details.get('latitude'); school_lon = details.get('longitude')
                if school_lat is not None and school_lon is not None:
                    try: distance = round(geodesic((lat, lon), (school_lat, school_lon)).miles, 1)
                    except ValueError as ve: print(f"  ⚠️ Dist calc error SCA {sca}: {ve}")

                # Prepare the school dictionary with selected details
                school_output_dict = {k: details.get(k) for k in [ # Select desired output fields
                    "sca", "display_name", "type", "school_level", "address", "city", "zipcode",
                    "phone", "great_schools_rating", "student_teacher_ratio_value",
                    "attendance_rate", "parent_satisfaction",
                    "ky_reportcard_URL", "school_website_link", "latitude", "longitude"
                ]}
                school_output_dict["distance_mi"] = distance # Add distance

                zone_output["schools"].append(school_output_dict)
            else: print(f"  Error: Details missing for SCA '{sca}' (Zone: {zone_type}) during formatting.")

        if not zone_output["schools"]: continue

        # --- Perform Sorting Based on Parameters ---
        if sort_key and sort_key in zone_output["schools"][0]: # Check if key exists in first school's dict
             # Helper to handle None values during sort
            def sort_helper(item):
                val = item.get(sort_key)
                is_none_or_not_numeric = val is None or not isinstance(val, (int, float))
                # Treat None/non-numeric as very small for desc, very large for asc
                if sort_desc:
                    return (is_none_or_not_numeric, val if not is_none_or_not_numeric else -float('inf'))
                else:
                    return (is_none_or_not_numeric, val if not is_none_or_not_numeric else float('inf'))

            try:
                 zone_output["schools"].sort(key=sort_helper, reverse=sort_desc)
                 print(f"  Sorting zone '{zone_type}' by '{sort_key}' {'DESC' if sort_desc else 'ASC'}")
            except Exception as sort_err:
                 print(f"  ⚠️ Error sorting zone '{zone_type}' by key '{sort_key}': {sort_err}. Defaulting to name sort.")
                 zone_output["schools"].sort(key=lambda x: x.get('display_name','').lower())
        else:
             # Default sort by name if no valid sort key provided
             zone_output["schools"].sort(key=lambda x: x.get('display_name','').lower())
             if sort_key: print(f"  ⚠️ Sort key '{sort_key}' not found or invalid for zone '{zone_type}'. Defaulting to name sort.")

        output_structure["results_by_zone"].append(zone_output)

    print(f"✅ School zone processing complete. Returning structured data.")
    return output_structure

# --- Flask Routes ---
@app.route("/test")
def test():
    return "🚀 Flask API (Refactored Core Logic) is working!"

# Helper to process request and call core logic
def handle_school_request(sort_key=None, sort_desc=False):
    start_time = time.time()
    data = request.get_json()
    if not data: print("❌ Error: Request body missing."); return jsonify({"error": "Request body must be JSON"}), 400
    address = data.get("address", "").strip()
    if not address: print("❌ Error: Address missing."); return jsonify({"error": "Address string is required"}), 400
    print(f"\n--- Request {request.path} --- Address: '{address}'")
    lat, lon = geocode_address(address)
    if lat is None: print(f"❌ Geocoding failed."); return jsonify({"error": f"Could not geocode address: '{address}'"}), 400
    print(f"📍 Geocoded to: Lat={lat:.5f}, Lon={lon:.5f}")

    structured_results = find_school_zones_and_details(lat, lon, all_zones_gdf, sort_key=sort_key, sort_desc=sort_desc)

    response_data = {"query_address": address, "query_lat": lat, "query_lon": lon, **(structured_results or {"results_by_zone": []})}
    end_time = time.time(); print(f"--- Request {request.path} completed in {end_time - start_time:.2f} seconds ---")
    return jsonify(response_data), 200

@app.route("/school-zone", methods=["POST"])
def school_zone():
    """Returns schools sorted by name (default)."""
    # Calls helper with default sort (effectively by name via find_school_zones_and_details default)
    return handle_school_request(sort_key='display_name', sort_desc=False)

@app.route("/school-distances", methods=["POST"])
def school_distances():
    """Returns schools sorted by distance (ASC)."""
    return handle_school_request(sort_key='distance_mi', sort_desc=False)

# --- NEW ROUTE: School Ratings ---
@app.route("/school-ratings", methods=["POST"])
def school_ratings():
    """Returns schools sorted by Great Schools Rating (DESC)."""
    # Ensure 'great_schools_rating' is the correct column name selected
    return handle_school_request(sort_key='great_schools_rating', sort_desc=True)

# --- NEW ROUTE: Parent Satisfaction ---
@app.route("/school-parent-satisfaction", methods=["POST"])
def school_parent_satisfaction():
    """Returns schools sorted by Parent Satisfaction Rating (DESC)."""
    actual_parent_satisfaction_column = 'parent_satisfaction' 
    return handle_school_request(sort_key=actual_parent_satisfaction_column, sort_desc=True)

# --- Run App ---
if __name__ == "__main__":
    if not os.path.exists(DATABASE_PATH): print("\n"+"!"*30+f"\n  DB NOT FOUND at '{DATABASE_PATH}'\n  Run setup scripts first!\n"+"!"*30+"\n"); exit()
    else: print(f"✅ Database file found at '{DATABASE_PATH}'")
    print("\n"+"="*30+"\n Starting Flask server...\n Access via http://localhost:5001\n"+"="*30+"\n")
    app.run(host="0.0.0.0", port=5001, debug=True)