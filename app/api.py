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
# --- Load Shapefiles (Keep this logic) ---
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
    for path, zone_type in shapefile_configs:
        if os.path.exists(path):
            try:
                gdf = gpd.read_file(path)
                gdf["zone_type"] = zone_type
                gdfs.append(gdf)
                print(f"  Loaded: {os.path.basename(path)}")
                loaded_files_count += 1
            except Exception as load_err:
                print(f"  ❌ Error loading {os.path.basename(path)}: {load_err}")
        else:
            print(f"  ⚠️ Warning: Shapefile not found at {path}")

    if not gdfs:
        raise FileNotFoundError("No valid shapefiles loaded.")

    all_zones_gdf = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True, sort=False)
    ).to_crs(epsg=4326)
    print(f"✅ Successfully loaded {loaded_files_count} shapefiles.")

    print("🛠️ Cleaning geometries...")
    try:
        # buffer(0) can sometimes fix minor geometry issues
        all_zones_gdf['geometry'] = all_zones_gdf.geometry.buffer(0)
        print("✅ Geometries cleaning complete.")
        # Optional: Add check for geometry type changes here if needed
    except Exception as geom_err:
        print(f"  ❌ Error cleaning: {geom_err}.") # Continue even if cleaning fails

    print("🛠️ Building spatial index...")
    # Force sindex regeneration
    if hasattr(all_zones_gdf, '_sindex'):
        delattr(all_zones_gdf, '_sindex')
    if hasattr(all_zones_gdf, '_sindex_generated'):
        delattr(all_zones_gdf, '_sindex_generated')
    all_zones_gdf.sindex # Accessing it builds it
    print("✅ Spatial index built.")

except FileNotFoundError as fnf:
    print(f"❌❌ FATAL ERROR: {fnf}.")
    exit() # Stop if no shapefiles loaded
except Exception as e:
    print(f"❌❌ FATAL ERROR loading/processing shapefiles: {e}")
    exit() # Stop on other fatal errors during setup

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
        try: 
            cursor = conn.cursor(); sql = f"SELECT school_code_adjusted FROM {DB_SCHOOLS_TABLE} WHERE feeder_to_high_school = ? AND school_level = ?"; cursor.execute(sql, (standard_hs_name, "Elementary School")); results = cursor.fetchall(); feeder_school_scas = [row['school_code_adjusted'] for row in results if row['school_code_adjusted']]
        except sqlite3.Error as e: print(f"Error querying elementary feeder SCAs for '{standard_hs_name}': {e}")
        finally: conn.close()
    return feeder_school_scas

def get_universal_magnet_scas_and_info():
    """ Fetches SCA, display_name, and school_level for all universal magnets. """
    # ... (Keep this function exactly as before) ...
    universal_magnets_info = []; conn = get_db_connection()
    if conn:
        try: 
            cursor = conn.cursor(); sql = f"SELECT school_code_adjusted, display_name, school_level FROM {DB_SCHOOLS_TABLE} WHERE universal_magnet = ?"; cursor.execute(sql, ('Yes',)); results = cursor.fetchall(); universal_magnets_info = [dict(row) for row in results]; print(f"  DB Query: Found {len(universal_magnets_info)} universal magnets.")
        except sqlite3.Error as e: print(f"Error querying universal magnets: {e}")
        finally: conn.close()
    return universal_magnets_info

def get_school_details_by_scas(school_codes_adjusted):
    """Fetches selected details for a list of schools by 'school_code_adjusted'."""
    # ... (Keep this function exactly as before, ensuring it selects all needed columns) ...
    details_map = {};
    if not school_codes_adjusted: return details_map
    unique_scas = {str(sca).strip() for sca in school_codes_adjusted if sca};
    if not unique_scas: return details_map
    conn = get_db_connection()
    if conn:
        try:
            cursor = conn.cursor()
            # Define the columns Botpress might need in the structured response
            select_columns_list = [
                "school_code_adjusted", "school_name", "display_name", "type", "zone", "gis_name",
                "feeder_to_high_school", "network", "great_schools_rating", "great_schools_url",
                "school_level", "enrollment", "student_teacher_ratio", "student_teacher_ratio_value",
                "attendance_rate", "dropout_rate", "school_website_link", "ky_reportcard_URL",
                "low_grade", "high_grade", "title_i_status", "address", "city", "state", "zipcode", "phone",
                "latitude", "longitude", "universal_magnet"
            ]
            select_columns_str = ", ".join(select_columns_list)
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

# --- REVISED Main Logic Function ---
def find_school_zones_and_details(lat, lon, gdf, include_distance=False): # Changed flag name
    """Finds zones, uses DB lookups, fetches details by SCA, returns STRUCTURED data."""
    # Initial checks
    if lat is None or lon is None: print("Error: Invalid user coords."); return None
    point = Point(lon, lat)
    if gdf is None or not hasattr(gdf, 'sindex') or gdf.empty: print("Error: GDF invalid."); return None

    # Spatial query
    matches = gpd.GeoDataFrame()
    try:
        # ... (Keep spatial query logic exactly as before) ...
        possible_matches_index = list(gdf.sindex.query(point, predicate='contains'))
        if possible_matches_index: matches = gdf.iloc[possible_matches_index]; contains_mask = matches.geometry.contains(point); matches = matches[contains_mask]
        if not matches.empty: print(f"  ℹ️ Spatial index found {len(matches)} precise matches.")
        else: print("  ℹ️ Spatial index found no containing polygons.")
        if matches.empty: print(f"  ℹ️ Falling back..."); matches = gdf[gdf.geometry.contains(point)]
        if not matches.empty: print(f"  ℹ️ Fallback check found {len(matches)} matches.")
    except Exception as e: print(f"❌ Error during spatial query: {e}. Trying fallback."); 
    try: 
        matches = gdf[gdf.geometry.contains(point)] 
    except Exception as fe: print(f"❌ Final fallback error: {fe}"); matches = gpd.GeoDataFrame()
    # Continue even if spatial query fails, Universal Magnets might still apply

    print(f"✅ Found {len(matches)} matching zone(s). Identifying schools...")

    # Step 1: Identify SCAs and map them to zone types
    identified_scas = set()
    sca_to_zone_types_map = defaultdict(set)
    def add_sca_to_map(zone_type, sca):
        if sca: sca_str = str(sca).strip(); identified_scas.add(sca_str); sca_to_zone_types_map[sca_str].add(zone_type)

    # Process Matched Zones to get SCAs
    for _, row in matches.iterrows():
        zone_type = row.get("zone_type", "Unknown")
        try:
            gis_key = None; determined_sca = None; determined_display_name = None
            if zone_type == "Elementary":
                high_school_gis_key = str(row.get("High", "")).strip().upper()
                if high_school_gis_key: feeder_scas = get_elementary_feeder_scas(high_school_gis_key); [add_sca_to_map(zone_type, sca) for sca in feeder_scas]
                continue
            elif zone_type in ["High", "Traditional/Magnet High"]: gis_key = str(row.get("High") or row.get("Traditiona", "")).strip().upper()
            elif zone_type in ["Middle", "Traditional/Magnet Middle"]: raw_middle = str(row.get("Middle", "")).strip().upper(); raw_name_col = str(row.get("Name", "")).strip().upper(); gis_key = raw_middle if raw_middle else raw_name_col
            elif zone_type == "Traditional/Magnet Elementary": gis_key = str(row.get("Traditiona", "")).strip().upper()
            elif zone_type == "Choice": potential_key = str(row.get("Name", "")).strip().upper(); info = get_info_from_gis(potential_key); determined_sca = info.get('sca')
            else: continue
            if gis_key and not determined_sca: info = get_info_from_gis(gis_key); determined_sca = info.get('sca');
            if not determined_sca and gis_key and zone_type != "Choice": print(f"  ⚠️ DB Lookup MISSING: Zone='{zone_type}', GIS Key='{gis_key}'")
            if determined_sca: add_sca_to_map(zone_type, determined_sca)
        except Exception as e: print(f"❌ Error processing row (Index: {row.name}, Zone: {zone_type}): {e}")

    # Step 1b: Add Universal Magnet SCAs
    print("Adding Universal Magnet schools from DB...")
    universal_magnets_info = get_universal_magnet_scas_and_info()
    for info in universal_magnets_info:
        sca = info.get('school_code_adjusted'); school_lvl = info.get('school_level')
        target_zone_type = "Unknown Magnet"
        if school_lvl == "Elementary School": target_zone_type = "Traditional/Magnet Elementary"
        elif school_lvl == "Middle School": target_zone_type = "Traditional/Magnet Middle"
        elif school_lvl == "High School": target_zone_type = "Traditional/Magnet High"
        add_sca_to_map(target_zone_type, sca)

    # Step 2: Fetch details using SCAs
    print(f"🔎 Identified {len(identified_scas)} unique SCAs. Querying DB...")
    if not identified_scas: print("🤷 No SCAs identified."); return {} # Return empty structure
    school_details_lookup = get_school_details_by_scas(list(identified_scas))
    print(f"✅ Found details for {len(school_details_lookup)} schools in DB.")

    # --- Step 3: Build STRUCTURED Output ---
    output_structure = {"results_by_zone": []}
    processed_scas_in_output = set()
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High", "Choice"]

    for zone_type in category_order:
        zone_output = {"zone_type": zone_type, "schools": []}
        scas_for_this_zone = {sca for sca, zones in sca_to_zone_types_map.items() if zone_type in zones}

        for sca in scas_for_this_zone:
            details = school_details_lookup.get(sca)
            if details:
                # Prepare the school dictionary with selected details
                school_output_dict = {
                    "sca": details.get('school_code_adjusted'),
                    "display_name": details.get('display_name'),
                    "report_card_url": details.get('ky_reportcard_URL'),
                    "website_url": details.get('school_website_link'),
                    "great_schools_rating": details.get('great_schools_rating'),
                    "type": details.get('type'),
                    "level": details.get('school_level'),
                    "address": details.get('address'),
                    "city": details.get('city'),
                    "zipcode": details.get('zipcode'),
                    "phone": details.get('phone'),
                    "latitude": details.get('latitude'),
                    "longitude": details.get('longitude'),
                    # Add distance only if requested
                }

                # Calculate and add distance if needed
                if include_distance:
                    distance = None # Use None instead of inf for cleaner JSON
                    school_lat = details.get('latitude'); school_lon = details.get('longitude')
                    if school_lat is not None and school_lon is not None:
                        try: distance = round(geodesic((lat, lon), (school_lat, school_lon)).miles, 1)
                        except ValueError as ve: print(f"  ⚠️ Dist calc error SCA {sca}: {ve}")
                    school_output_dict["distance_mi"] = distance

                zone_output["schools"].append(school_output_dict)

            else:
                print(f"  Error: Details missing for SCA '{sca}' (Zone: {zone_type}) during formatting.")
                # Optionally add a placeholder if needed, but usually just skip
                # zone_output["schools"].append({"sca": sca, "error": "Details not found in DB"})

        if not zone_output["schools"]: continue # Skip zone types with no matching schools found

        # Sort schools within this zone type
        if include_distance:
            zone_output["schools"].sort(key=lambda x: (float('inf') if x.get('distance_mi') is None else x['distance_mi'], x['display_name'].lower()))
        else:
             zone_output["schools"].sort(key=lambda x: x['display_name'].lower())

        output_structure["results_by_zone"].append(zone_output)

    print(f"✅ School zone processing complete. Returning structured data.")
    return output_structure

# --- Flask Routes (MODIFIED TO RETURN STRUCTURE) ---
@app.route("/test")
def test():
    return "🚀 Flask API (structured response) is working!"

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

    # Get structured data, don't include distance
    structured_results = find_school_zones_and_details(lat, lon, all_zones_gdf, include_distance=False)

    # Add query info to the response
    response_data = {
        "query_address": address,
        "query_lat": lat,
        "query_lon": lon,
        **(structured_results or {"results_by_zone": []}) # Merge results or use empty list
    }
    end_time = time.time(); print(f"--- Request /school-zone completed in {end_time - start_time:.2f} seconds ---")
    # --- Return the structured JSON ---
    return jsonify(response_data), 200

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

    # Get structured data, INCLUDING distance
    structured_results = find_school_zones_and_details(lat, lon, all_zones_gdf, include_distance=True)

     # Add query info to the response
    response_data = {
        "query_address": address,
        "query_lat": lat,
        "query_lon": lon,
        **(structured_results or {"results_by_zone": []}) # Merge results or use empty list
    }
    end_time = time.time(); print(f"--- Request /school-distances completed in {end_time - start_time:.2f} seconds ---")
    # --- Return the structured JSON ---
    return jsonify(response_data), 200

# --- Run App ---
if __name__ == "__main__":
    if not os.path.exists(DATABASE_PATH): print("\n"+"!"*30+f"\n  DB NOT FOUND at '{DATABASE_PATH}'\n  Run setup scripts first!\n"+"!"*30+"\n"); exit()
    else: print(f"✅ Database file found at '{DATABASE_PATH}'")
    print("\n"+"="*30+"\n Starting Flask server...\n Access via http://localhost:5001\n"+"="*30+"\n")
    app.run(host="0.0.0.0", port=5001, debug=True) # Set debug=False for production