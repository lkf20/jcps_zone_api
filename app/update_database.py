import os
import json
import time
import sqlite3

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
# DB_MAPPINGS_TABLE = 'gis_name_mappings' # REMOVED - Mappings now in main table

# Shapefile paths
choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "Resides_HS_Boundaries.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp")
elementary_path = os.path.join(DATA_DIR, "Elementary", "Resides_ES_Clusters_Boundaries.shp")
traditional_middle_path = os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp")
traditional_high_path = os.path.join(DATA_DIR, "TraditionalHigh", "Traditional_HS_Bnds.shp")
traditional_elem_path = os.path.join(DATA_DIR, "TraditionalElementary", "Traditional_ES_Bnds.shp")

# --- Dictionaries REMOVED ---
# DISPLAY_NAMES_HIGH = {...}
# DISPLAY_NAMES_MIDDLE = {...}
# DISPLAY_NAMES_ELEMENTARY = {...}
# UNIVERSAL_SCHOOLS = {...}


# --- Load Shapefiles (Keep this logic) ---
print("🔄 Loading Shapefiles...")
all_zones_gdf = None
try:
    # ... (Keep the shapefile loading, merging, cleaning, and indexing logic exactly as before) ...
    shapefile_configs = [
        (traditional_high_path, "Traditional/Magnet High"), (traditional_middle_path, "Traditional/Magnet Middle"),
        (traditional_elem_path, "Traditional/Magnet Elementary"), (high_path, "High"), (middle_path, "Middle"),
        (elementary_path, "Elementary"), (choice_path, "Choice"),
    ]
    gdfs = []; loaded_files_count = 0
    for path, zone_type in shapefile_configs:
        if os.path.exists(path):
            try: gdf = gpd.read_file(path); gdf["zone_type"] = zone_type; gdfs.append(gdf); print(f"  Loaded: {os.path.basename(path)}"); loaded_files_count += 1
            except Exception as load_err: print(f"  ❌ Error loading {os.path.basename(path)}: {load_err}")
        else: print(f"  ⚠️ Warning: Shapefile not found at {path}")
    if not gdfs: raise FileNotFoundError("No valid shapefiles loaded.")
    all_zones_gdf = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True, sort=False)).to_crs(epsg=4326)
    print(f"✅ Successfully loaded {loaded_files_count} shapefiles.")
    print("🛠️ Cleaning geometries..."); try: all_zones_gdf['geometry'] = all_zones_gdf.geometry.buffer(0); print("✅ Geometries cleaning complete.")
    except Exception as geom_err: print(f"  ❌ Error cleaning: {geom_err}.")
    print("🛠️ Building spatial index..."); if hasattr(all_zones_gdf, '_sindex'): delattr(all_zones_gdf, '_sindex'); if hasattr(all_zones_gdf, '_sindex_generated'): delattr(all_zones_gdf, '_sindex_generated'); all_zones_gdf.sindex; print("✅ Spatial index built.")
except FileNotFoundError as fnf: print(f"❌❌ FATAL ERROR: {fnf}."); exit()
except Exception as e: print(f"❌❌ FATAL ERROR loading/processing shapefiles: {e}"); exit()

# --- Database Helper Functions ---
def get_db_connection():
    """Establishes a connection to the database."""
    if not os.path.exists(DATABASE_PATH): print(f"FATAL ERROR: DB not found at {DATABASE_PATH}"); return None
    try: conn = sqlite3.connect(DATABASE_PATH); conn.row_factory = sqlite3.Row; return conn
    except sqlite3.Error as e: print(f"Database connection error: {e}"); return None

# --- NEW Function: Lookup display_name from gis_name in the main schools table ---
def get_display_name_from_gis(gis_name_key):
    """Looks up the display name from the schools table using the gis_name."""
    display_name = None
    if not gis_name_key: return None
    lookup_key = str(gis_name_key).strip().upper() # Match how gis_name is likely stored

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Query the main table using the gis_name column
            sql = f"SELECT display_name FROM {DB_SCHOOLS_TABLE} WHERE gis_name = ?"
            cursor.execute(sql, (lookup_key,))
            result = cursor.fetchone()
            if result: display_name = result['display_name']
        except sqlite3.Error as e: print(f"Error looking up display name for GIS key '{lookup_key}': {e}")
        finally: conn.close()
    # if not display_name: print(f"  DB Lookup MISSING for GIS Key: '{lookup_key}'") # Optional debug
    return display_name

# --- MODIFIED Function: Query feeders using display_name looked up from GIS key ---
def get_elementary_feeders_for_high_school(high_school_gis_key):
    """Finds elementary school display names feeding into a high school using the DB."""
    feeder_schools_display_names = []
    # --- Step 1: Get the standard HS display name from the DB using the GIS key ---
    standard_hs_name = get_display_name_from_gis(high_school_gis_key)

    if not standard_hs_name: return feeder_schools_display_names # Warning already printed

    # --- Step 2: Query the schools table using the standard HS name ---
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            sql = f"""
                SELECT display_name
                FROM {DB_SCHOOLS_TABLE}
                WHERE feeder_to_high_school = ? AND school_level = ?
            """
            # Assuming school_level distinguishes elementary schools clearly
            cursor.execute(sql, (standard_hs_name, "Elementary School"))
            results = cursor.fetchall()
            feeder_schools_display_names = [row['display_name'] for row in results if row['display_name']]
        except sqlite3.Error as e: print(f"Error querying elementary feeders for '{standard_hs_name}': {e}")
        finally: conn.close()
    return feeder_schools_display_names

# --- Function to get school details (Same as before, ensures needed columns selected) ---
def get_school_details_by_display_names(display_names):
    """Fetches selected details for a list of schools by 'display_name'."""
    details_map = {}
    if not display_names: return details_map
    clean_display_names = {name.strip() for name in display_names if name and name.strip()}
    if not clean_display_names: return details_map

    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Select all columns needed
            select_columns = """
                school_code_adjusted, school_name, display_name, type, zone, gis_name,
                feeder_to_high_school, network, great_schools_rating, great_schools_url,
                school_level, enrollment, student_teacher_ratio, student_teacher_ratio_value,
                attendance_rate, dropout_rate, school_website_link, ky_reportcard_URL,
                low_grade, high_grade, title_i_status, address, city, state, zipcode, phone,
                latitude, longitude, universal_magnet
                -- Add/remove other columns as needed --
            """
            placeholders = ', '.join('?' * len(clean_display_names))
            sql = f"SELECT {select_columns} FROM {DB_SCHOOLS_TABLE} WHERE display_name IN ({placeholders})"
            cursor.execute(sql, tuple(clean_display_names))
            results = cursor.fetchall()
            for row in results:
                school_dict = dict(row)
                db_display_name = school_dict.get('display_name')
                if db_display_name in clean_display_names: details_map[db_display_name] = school_dict
        except sqlite3.Error as e: print(f"Error querying details for display_names {clean_display_names}: {e}")
        finally: conn.close()
    return details_map

# --- NEW Function: Get Universal Magnets directly from DB ---
def get_universal_magnets():
    """ Fetches display_name and school_level for all universal magnets. """
    universal_magnets = [] # List of tuples: (display_name, school_level)
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Query using the universal_magnet flag
            sql = f"SELECT display_name, school_level FROM {DB_SCHOOLS_TABLE} WHERE universal_magnet = ?"
            cursor.execute(sql, ('Yes',)) # Assuming 'Yes' indicates universal magnet
            results = cursor.fetchall()
            universal_magnets = [(row['display_name'], row['school_level']) for row in results if row['display_name']]
            print(f"  DB Query: Found {len(universal_magnets)} universal magnets.")
        except sqlite3.Error as e:
            print(f"Error querying universal magnets: {e}")
        finally:
            conn.close()
    return universal_magnets


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

# --- MODIFIED Main Logic Function ---
def find_school_zones_and_details(lat, lon, gdf, sort_by_distance=False):
    """Finds zones, uses DB for GIS mapping, feeders, universal magnets, details & links."""
    # ... (Coordinate check, Point creation, GDF check as before) ...
    if lat is None or lon is None: print("Error: Invalid user coords."); return []
    point = Point(lon, lat)
    if gdf is None or not hasattr(gdf, 'sindex') or gdf.empty: print("Error: GDF invalid."); return []

    # ... (Spatial query logic exactly as before) ...
    matches = gpd.GeoDataFrame()
    try:
        possible_matches_index = list(gdf.sindex.query(point, predicate='contains'))
        if possible_matches_index: matches = gdf.iloc[possible_matches_index]; contains_mask = matches.geometry.contains(point); matches = matches[contains_mask]
        if not matches.empty: print(f"  ℹ️ Spatial index found {len(matches)} precise matches.")
        else: print("  ℹ️ Spatial index found no containing polygons.")
        if matches.empty: print(f"  ℹ️ Falling back..."); matches = gdf[gdf.geometry.contains(point)]
        if not matches.empty: print(f"  ℹ️ Fallback check found {len(matches)} matches.")
    except Exception as e: print(f"❌ Error during spatial query: {e}. Trying fallback."); try: matches = gdf[gdf.geometry.contains(point)] except Exception as fe: print(f"❌ Final fallback error: {fe}"); matches = gpd.GeoDataFrame()
    if matches.empty: print("🤷 No matching school zones found."); return []

    print(f"✅ Found {len(matches)} matching zone(s). Identifying schools...")

    # --- Step 1: Identify potential DISPLAY names ---
    identified_display_names = set()
    zone_to_display_names_map = {}

    def add_display_name_to_sets(zone_type, disp_name):
         if not disp_name or not disp_name.strip(): return
         clean_disp_name = disp_name.strip()
         identified_display_names.add(clean_disp_name)
         if zone_type not in zone_to_display_names_map: zone_to_display_names_map[zone_type] = set()
         zone_to_display_names_map[zone_type].add(clean_disp_name)

    # --- Process Matched Zones to get DISPLAY NAMES using DB lookup ---
    for _, row in matches.iterrows():
        zone_type = row.get("zone_type", "Unknown")
        try:
            determined_display_name = None
            if zone_type == "Elementary":
                high_school_gis_key = str(row.get("High", "")).strip().upper()
                if high_school_gis_key:
                    # Get feeder DISPLAY names from DB
                    feeder_display_names = get_elementary_feeders_for_high_school(high_school_gis_key)
                    for name in feeder_display_names: add_display_name_to_sets(zone_type, name)
                continue # Handled feeders

            # --- Process other zone types: Get GIS Key ---
            gis_key = None
            if zone_type in ["High", "Traditional/Magnet High"]: gis_key = str(row.get("High") or row.get("Traditiona", "")).strip().upper()
            elif zone_type in ["Middle", "Traditional/Magnet Middle"]: raw_middle = str(row.get("Middle", "")).strip().upper(); raw_name_col = str(row.get("Name", "")).strip().upper(); gis_key = raw_middle if raw_middle else raw_name_col
            elif zone_type == "Traditional/Magnet Elementary": gis_key = str(row.get("Traditiona", "")).strip().upper()
            elif zone_type == "Choice":
                # Assume Choice zone 'Name' might be the display name OR a GIS key
                potential_name = str(row.get("Name", "")).strip()
                if potential_name:
                    # Try looking it up as a GIS key first
                    determined_display_name = get_display_name_from_gis(potential_name.upper())
                    # If lookup fails, assume the 'Name' field IS the display name
                    if not determined_display_name:
                        determined_display_name = potential_name
            else: continue # Skip unknown zone types

            # If we determined a GIS key, look up the display name in the DB
            if gis_key and not determined_display_name:
                determined_display_name = get_display_name_from_gis(gis_key)
                if not determined_display_name: print(f"  ⚠️ DB Lookup MISSING: Zone='{zone_type}', GIS Key='{gis_key}'")

            # Add the found display name
            if determined_display_name: add_display_name_to_sets(zone_type, determined_display_name)

        except Exception as e: print(f"❌ Error processing row (Index: {row.name}, Zone: {zone_type}): {e}")

    # --- Step 1b: Add Universal Magnet DISPLAY Names from DB ---
    print("Adding Universal Magnet schools from DB...")
    universal_magnets_data = get_universal_magnets()
    for disp_name, school_lvl in universal_magnets_data:
        target_zone_type = "Unknown Magnet" # Default
        if school_lvl == "Elementary School": target_zone_type = "Traditional/Magnet Elementary"
        elif school_lvl == "Middle School": target_zone_type = "Traditional/Magnet Middle"
        elif school_lvl == "High School": target_zone_type = "Traditional/Magnet High"
        add_display_name_to_sets(target_zone_type, disp_name)
    # The use of 'set' automatically handles duplicates added here and from zones

    # --- Step 2: Fetch details using DISPLAY NAMES ---
    print(f"🔎 Identified {len(identified_display_names)} unique display names. Querying DB...")
    if not identified_display_names: print("🤷 No display names identified."); return []
    school_details_lookup = get_school_details_by_display_names(list(identified_display_names))
    print(f"✅ Found details for {len(school_details_lookup)} schools in DB.")

    # --- Step 3: Format Final Results (Logic remains largely the same) ---
    final_results_list = []
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High", "Choice"]
    sorted_zone_types = sorted(zone_to_display_names_map.keys(), key=lambda z: category_order.index(z) if z in category_order else len(category_order))

    for zone_type in sorted_zone_types:
        schools_in_zone = []
        display_names_for_zone = zone_to_display_names_map.get(zone_type, set())

        for disp_name in display_names_for_zone:
            details = school_details_lookup.get(disp_name)
            if details:
                distance = float('inf')
                school_lat = details.get('latitude'); school_lon = details.get('longitude')
                if sort_by_distance and school_lat is not None and school_lon is not None:
                    try: distance = geodesic((lat, lon), (school_lat, school_lon)).miles
                    except ValueError as ve: print(f"  ⚠️ Dist calc error {disp_name}: {ve}")

                link_url = details.get('ky_reportcard_URL')
                name_cleaned = details.get('display_name', disp_name)
                # --- START TEMPORARY DEBUG ---
                if name_cleaned == "Coleridge-Taylor Montessori Elementary":
                    print(f"DEBUG CHECK: School='{name_cleaned}', URL='{link_url}', Type='{type(link_url)}'")
                # --- END TEMPORARY DEBUG ---
                linked_name = f"[{name_cleaned}]({link_url})" if link_url else name_cleaned

                schools_in_zone.append({"name_linked": linked_name, "name_plain": name_cleaned, "distance": distance})
            else:
                print(f"  Info: Details for '{disp_name}' (Zone: {zone_type}) missing in DB.")
                schools_in_zone.append({"name_linked": disp_name, "name_plain": disp_name, "distance": float('inf'), "error": "Details not found"})

        if not schools_in_zone: continue
        schools_in_zone.sort(key=lambda x: (float('inf') if x['distance'] == float('inf') else x['distance'], x['name_plain'].lower()))

        school_strings = []
        for school in schools_in_zone:
            name = school['name_linked']
            dist = school['distance']
            if sort_by_distance: dist_str = f" ({dist:.1f} mi)" if dist != float('inf') else " (distance unavailable)"; school_strings.append(f"{name}{dist_str}")
            else: school_strings.append(name)

        if school_strings: final_results_list.append(f"{zone_type} Schools: {', '.join(school_strings)}")

    print(f"✅ School zone processing complete. Returning {len(final_results_list)} formatted zone strings.")
    return final_results_list

# --- Flask Routes (No changes needed here) ---
@app.route("/test")
def test():
    return "🚀 Flask API using full DB integration is working!"

@app.route("/school-zone", methods=["POST"])
def school_zone():
    start_time = time.time(); data = request.get_json()
    if not data: print("❌ Error: Request body missing."); return jsonify({"error": "Request body must be JSON"}), 400
    address = data.get("address", "").strip()
    if not address: print("❌ Error: Address missing."); return jsonify({"error": "Address string is required"}), 400
    print(f"\n--- Request /school-zone --- Address: '{address}'")
    lat, lon = geocode_address(address)
    if lat is None: print(f"❌ Geocoding failed."); return jsonify({"error": f"Could not geocode address: '{address}'"}), 400
    print(f"📍 Geocoded to: Lat={lat:.5f}, Lon={lon:.5f}")
    zone_strings = find_school_zones_and_details(lat, lon, all_zones_gdf, sort_by_distance=False)
    end_time = time.time(); print(f"--- Request /school-zone completed in {end_time - start_time:.2f} seconds ---")
    return jsonify({"zones": zone_strings}), 200

@app.route("/school-distances", methods=["POST"])
def school_distances():
    start_time = time.time(); data = request.get_json()
    if not data: print("❌ Error: Request body missing."); return jsonify({"error": "Request body must be JSON"}), 400
    address = data.get("address", "").strip()
    if not address: print("❌ Error: Address missing."); return jsonify({"error": "Address string is required"}), 400
    print(f"\n--- Request /school-distances --- Address: '{address}'")
    lat, lon = geocode_address(address)
    if lat is None: print(f"❌ Geocoding failed."); return jsonify({"error": f"Could not geocode address: '{address}'"}), 400
    print(f"📍 Geocoded to: Lat={lat:.5f}, Lon={lon:.5f}")
    zones_with_distances = find_school_zones_and_details(lat, lon, all_zones_gdf, sort_by_distance=True)
    end_time = time.time(); print(f"--- Request /school-distances completed in {end_time - start_time:.2f} seconds ---")
    return jsonify({"zones": zones_with_distances})

# --- Run App ---
if __name__ == "__main__":
    if not os.path.exists(DATABASE_PATH): print("\n"+"!"*30+f"\n  DB NOT FOUND at '{DATABASE_PATH}'\n  Run setup scripts first!\n"+"!"*30+"\n"); exit()
    else: print(f"✅ Database file found at '{DATABASE_PATH}'")
    print("\n"+"="*30+"\n Starting Flask server...\n Access via http://localhost:5001\n"+"="*30+"\n")
    app.run(host="0.0.0.0", port=5001, debug=True) # Set debug=False for production