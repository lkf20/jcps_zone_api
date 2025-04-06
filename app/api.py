
"""
Flask API to find JCPS school zones and distances based on a user's address.

Endpoints:
  /school-zone (POST): Returns assigned schools based on zones.
                       Body: {"address": "Street, City, ST ZIP"}
  /school-distances (POST): Returns assigned schools with distances, sorted.
                            Body: {"address": "Street, City, ST ZIP"}
"""

import os
import json
import time # For request timing

from flask import Flask, request, jsonify
from geopy.geocoders import Nominatim
from geopy.distance import geodesic # Import geodesic directly
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import pprint # For potentially debugging dicts later if needed

# --- Configuration & Data Loading ---

# Path Definitions
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Assumes 'data' directory is one level up from the 'app' directory where api.py resides
DATA_DIR = os.path.join(BASE_DIR, "..", "data")

print(f"Base Directory: {BASE_DIR}")
print(f"Data Directory: {DATA_DIR}")

# Shapefile paths
choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "Resides_HS_Boundaries.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp")
elementary_path = os.path.join(DATA_DIR, "Elementary", "Resides_ES_Clusters_Boundaries.shp")
traditional_middle_path = os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp")
traditional_high_path = os.path.join(DATA_DIR, "TraditionalHigh", "Traditional_HS_Bnds.shp")
traditional_elem_path = os.path.join(DATA_DIR, "TraditionalElementary", "Traditional_ES_Bnds.shp")

# School Locations CSV Path (Prioritized for coordinates)
# *** Verify this path is correct relative to your api.py location ***
CSV_LOCATIONS_PATH = os.path.join(DATA_DIR, "school_locations.csv")
# Or if it's next to api.py:
# CSV_LOCATIONS_PATH = os.path.join(BASE_DIR, "school_locations.csv")

# JSON Data Paths (relative to this script)
FEEDERS_PATH = os.path.join(BASE_DIR, "elementary_feeders.json")
REPORT_LINKS_PATH = os.path.join(BASE_DIR, "school_report_links.json")


# --- Display Name Mappings (Single Source of Truth) ---
# Keys should match the (UPPERCASE) identifiers found in the relevant GIS columns
# Values are the desired final display names

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
    "MALE": "Louisville Male High",
    # Add other keys identified by diagnostics.py if needed
}

DISPLAY_NAMES_MIDDLE = {
    "BARRETT": "Barret Traditional Middle", # KEY = GIS spelling, VALUE = Correct spelling
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
    # Add "OLMSTED ACADEMY NORTH" if that's also a key in GIS
    "RAMSEY":"Ramsey Middle",
    "STUART":"Stuart Academy",
    "THOMAS JEFFERSON":"Thomas Jefferson Middle",
    "WESTPORT":"Westport Middle",
    # Add other keys identified by diagnostics.py if needed
}

DISPLAY_NAMES_ELEMENTARY = {
    # Primarily for Traditional Elementary zones (check GIS 'Traditiona' column)
    "AUDUBON": "Audubon Traditional Elementary",
    "CARTER": "Carter Traditional Elementary",
    "FOSTER": "Foster Traditional Academy",
    "GREATHOUSE": "Greathouse/Shryock Traditional",
    "SCHAFFNER": "Schaffner Traditional Elementary",
    # Add other keys identified by diagnostics.py if needed
}

# --- Universal Schools List ---
# Assumes names listed here ARE the desired final display names
# Ensure these names match entries in school_locations.csv and school_report_links.json
UNIVERSAL_SCHOOLS = {
    "Elementary": [
        "Brandeis Elementary", "Coleridge-Taylor Montessori Elementary",
        "Hawthorne Elementary", "J. Graham Brown School",
        "Lincoln Elementary Performing Arts", "Young Elementary"
    ],
    "Middle": [
        "Grace M. James Academy of Excellence", "J. Graham Brown School",
        "W.E.B. DuBois Academy", "Western Middle School for the Arts"
    ],
    "High": [
        "Central High Magnet Career Academy", "duPont Manual High",
        "J. Graham Brown School", "W.E.B. DuBois Academy", "Western High"
    ]
}

# --- Load JSON Data ---
try:
    with open(FEEDERS_PATH, "r", encoding="utf-8") as f:
        ELEMENTARY_FEEDERS = json.load(f)
except Exception as e:
    print(f"❌ ERROR loading elementary feeders from {FEEDERS_PATH}: {e}")
    ELEMENTARY_FEEDERS = {}

try:
    with open(REPORT_LINKS_PATH, "r", encoding="utf-8") as f:
        # Keys here MUST match the final display names (lowercase)
        SCHOOL_REPORT_LINKS = json.load(f)
except Exception as e:
     print(f"❌ ERROR loading school report links from {REPORT_LINKS_PATH}: {e}")
     SCHOOL_REPORT_LINKS = {}

# --- Load and Prepare Shapefiles ---
print("🔄 Loading Shapefiles...")
all_zones_gdf = None
try:
    shapefile_configs = [
        (choice_path, "Choice"),
        (high_path, "High"),
        (middle_path, "Middle"),
        (elementary_path, "Elementary"),
        (traditional_middle_path, "Traditional/Magnet Middle"),
        (traditional_high_path, "Traditional/Magnet High"),
        (traditional_elem_path, "Traditional/Magnet Elementary"),
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
        raise FileNotFoundError(f"No valid shapefiles were loaded from configured paths in {DATA_DIR}.")

    all_zones_gdf = gpd.GeoDataFrame(
        pd.concat(gdfs, ignore_index=True, sort=False)
    ).to_crs(epsg=4326) # Convert to WGS84 (lat/lon)
    print(f"✅ Successfully loaded and merged {loaded_files_count} shapefiles.")

    # Optional: Attempt to fix invalid geometries which might corrupt the index
    print("🛠️ Attempting to clean geometries (buffer(0))...")
    try:
        # Track original types to check for unexpected changes
        original_types = all_zones_gdf.geometry.type.copy()
        # buffer(0) can sometimes fix minor geometry issues like self-intersections
        all_zones_gdf['geometry'] = all_zones_gdf.geometry.buffer(0)
        # Check if types changed (buffer(0) shouldn't change valid simple types)
        if not all(original_types == all_zones_gdf.geometry.type):
            print("  ⚠️ Warning: Geometry types may have changed during buffer(0). Review data if issues arise.")
        print("✅ Geometries cleaning process completed.")
    except Exception as geom_err:
        print(f"  ❌ Error during geometry cleaning: {geom_err}. Proceeding without cleaning.")

    # Explicitly delete and rebuild the spatial index
    print("🛠️ Building spatial index...")
    if hasattr(all_zones_gdf, '_sindex'):
        delattr(all_zones_gdf, '_sindex')
    if hasattr(all_zones_gdf, '_sindex_generated'):
        delattr(all_zones_gdf, '_sindex_generated')
    all_zones_gdf.sindex # Force regeneration
    print("✅ Spatial index built/rebuilt.")

except FileNotFoundError as fnf:
    print(f"❌❌ FATAL ERROR: {fnf}. Cannot proceed without shapefiles.")
    exit()
except Exception as e:
    print(f"❌❌ FATAL ERROR loading or processing shapefiles: {e}")
    exit()


# --- Load School Locations from CSV ---
school_locations_dict = {} # Stores { lower_school_name: (lat, lon) }
print(f"🔄 Attempting to load School Locations from: {CSV_LOCATIONS_PATH}")
try:
    if not os.path.exists(CSV_LOCATIONS_PATH):
         raise FileNotFoundError(f"File not found at {CSV_LOCATIONS_PATH}")

    schools_df = pd.read_csv(CSV_LOCATIONS_PATH)
    required_cols = ['SchoolName', 'Latitude', 'Longitude']
    if not all(col in schools_df.columns for col in required_cols):
        raise ValueError(f"CSV missing one or more required columns: {required_cols}")

    loaded_count = 0
    warning_count = 0
    for _, row in schools_df.iterrows():
        school_name = str(row['SchoolName']).strip()
        latitude = row['Latitude']
        longitude = row['Longitude']
        if not school_name: continue # Skip rows with empty names
        school_name_key = school_name.lower() # Use lowercase name as key

        if pd.notna(latitude) and pd.notna(longitude):
            try:
                school_locations_dict[school_name_key] = (float(latitude), float(longitude))
                loaded_count += 1
            except ValueError:
                 print(f"⚠️ Warning: Invalid lat/lon format for '{school_name}' in CSV. Skipping.")
                 warning_count += 1
                 school_locations_dict[school_name_key] = None
        else:
            # Log only if name is present but coords are missing
            if school_name:
                print(f"⚠️ Warning: Missing lat/lon for '{school_name}' in CSV.")
                warning_count += 1
            school_locations_dict[school_name_key] = None

    print(f"✅ Successfully processed CSV. Loaded coordinates for {loaded_count} schools.")
    if warning_count > 0:
        print(f"  Encountered {warning_count} warnings (missing/invalid data).")

except FileNotFoundError:
    print(f"ℹ️ Info: School locations CSV file not found. Will rely solely on geocoding for distances.")
except ValueError as ve:
     print(f"❌ ERROR: Issue with CSV columns: {ve}. Will rely solely on geocoding.")
except Exception as e:
    print(f"❌ ERROR: Failed to load or process school locations CSV: {e}. Will rely solely on geocoding.")


# --- Flask App Initialization ---
app = Flask(__name__)
# IMPORTANT: Provide a unique user agent with contact info (replace email) per Nominatim policy
geolocator = Nominatim(user_agent="jcps_school_bot/1.0 (your_email@example.com)", timeout=15)


# --- Helper Functions ---

# Cache for geocoded user addresses
address_cache = {}
# Cache for school coordinates obtained via live geocoding (fallback only)
school_geocoding_cache = {}

def geocode_address(address):
    """Geocodes a user address string with caching. Returns (lat, lon) or (None, None)."""
    address = str(address).strip()
    if not address:
        return None, None

    if address in address_cache:
        return address_cache[address]

    print(f"  -> Geocoding user address: '{address}'")
    try:
        # time.sleep(1) # Consider adding delay if making many requests rapidly
        location = geolocator.geocode(address)
        if location:
            coords = (location.latitude, location.longitude)
            address_cache[address] = coords
            print(f"  ✅ Geocode success: {address} -> ({coords[0]:.5f}, {coords[1]:.5f})")
            return coords
        else:
            print(f"  ⚠️ Geocoding failed (no result): '{address}'")
            address_cache[address] = (None, None)
            return None, None
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as geo_err:
         print(f"  ❌ Geocoding Service ERROR for address '{address}': {geo_err}")
         address_cache[address] = (None, None)
         return None, None
    except Exception as e:
        print(f"  ❌ Geocoding UNEXPECTED EXCEPTION for address '{address}': {e}")
        address_cache[address] = (None, None)
        return None, None

def get_school_coords(final_display_name):
    """
    Gets school coordinates (lat, lon) tuple.
    Priority: 1. Pre-loaded CSV data. 2. Runtime geocoding cache. 3. Live geocoding.
    Returns (lat, lon) or None if coordinates cannot be determined.
    """
    if not final_display_name:
        return None

    clean_name_only = final_display_name.split('](')[0].replace('[', '').strip()
    normalized_key = clean_name_only.lower()

    # 1. Check Pre-loaded CSV data
    if normalized_key in school_locations_dict:
        return school_locations_dict[normalized_key] # Return coords or None if listed as None in CSV

    # 2. Check Runtime Geocoding Cache
    if normalized_key in school_geocoding_cache:
        return school_geocoding_cache[normalized_key]

    # 3. Fallback to Live Geocoding
    print(f"  -> Geocoding school (not in CSV/cache): '{final_display_name}'")
    full_address_guess = f"{clean_name_only}, Louisville, KY"

    try:
        # time.sleep(1) # Consider adding delay
        location = geolocator.geocode(full_address_guess)
        if location:
            coords = (location.latitude, location.longitude)
            school_geocoding_cache[normalized_key] = coords # Cache success
            print(f"  ✅ Geocode success: {full_address_guess} -> ({coords[0]:.5f}, {coords[1]:.5f})")
            return coords
        else:
            print(f"  ⚠️ Geocoding failed (no result): '{full_address_guess}'")
            school_geocoding_cache[normalized_key] = None # Cache failure
            return None
    except (GeocoderTimedOut, GeocoderUnavailable, GeocoderServiceError) as geo_err:
         print(f"  ❌ Geocoding Service ERROR for school '{full_address_guess}': {geo_err}")
         school_geocoding_cache[normalized_key] = None
         return None
    except Exception as e:
        print(f"  ❌ Geocoding UNEXPECTED EXCEPTION for school '{full_address_guess}': {e}")
        school_geocoding_cache[normalized_key] = None
        return None


def find_school_zones(lat, lon, gdf, sort_by_distance=False):
    """
    Finds school zones containing the point and identifies associated schools.
    Uses STRICT lookups in DISPLAY_NAMES_* dictionaries. Skips schools with missing mappings.
    """
    if lat is None or lon is None:
        print("Error: Invalid user coordinates provided to find_school_zones.")
        return []

    point = Point(lon, lat)
    if gdf is None or not hasattr(gdf, 'sindex') or gdf.empty:
         print("Error: GeoDataFrame not available, empty, or spatial index missing.")
         return []

    matches = gpd.GeoDataFrame() # Initialize empty
    try:
        # Attempt spatial index query first
        possible_matches_index = list(gdf.sindex.query(point, predicate='contains'))
        if possible_matches_index:
            matches = gdf.iloc[possible_matches_index]
            # Optional precise check if index might include boundary touches
            contains_mask = matches.geometry.contains(point)
            matches = matches[contains_mask]
            if not matches.empty:
                 print(f"  ℹ️ Spatial index query found {len(matches)} precise matches.")
        else:
             print("  ℹ️ Spatial index query found no containing polygons.")

        # If index failed or returned nothing, try non-indexed fallback
        if matches.empty:
            print(f"  ℹ️ Falling back to non-indexed check (can be slow)...")
            matches = gdf[gdf.geometry.contains(point)]
            if not matches.empty:
                 print(f"  ℹ️ Fallback non-indexed check found {len(matches)} matches.")

    except Exception as e:
        print(f"❌ Error during spatial query process: {e}. Attempting non-indexed check as final fallback.")
        try:
            # Final fallback attempt
            matches = gdf[gdf.geometry.contains(point)]
        except Exception as fallback_e:
            print(f"❌ Error during final fallback non-indexed query: {fallback_e}")
            matches = gpd.GeoDataFrame() # Ensure empty on error


    if matches.empty:
        print("🤷 No matching school zones found for the provided coordinates.")
        return []

    print(f"✅ Found {len(matches)} matching zone(s). Processing...")
    school_dict = {} # Stores {(zone_type, final_display_name_with_link): distance}

    # --- Link generation ---
    def make_linked_name(final_display_name):
        """Adds markdown link if found in SCHOOL_REPORT_LINKS."""
        if not final_display_name: return ""
        cleaned_name = final_display_name.strip()
        normalized_link_key = cleaned_name.lower()
        link = SCHOOL_REPORT_LINKS.get(normalized_link_key)
        # if not link: print(f"  Link Warning: No link for key: '{normalized_link_key}'") # Optional warning
        if link:
            return f"[{cleaned_name}]({link})"
        return cleaned_name

    # --- Add school (if display name is valid) ---
    def add_school(zone_type, final_display_name):
        """Adds school to dict if display name provided, calculating distance if needed."""
        if not final_display_name or not final_display_name.strip(): return
        cleaned_display_name = final_display_name.strip()
        name_with_link = make_linked_name(cleaned_display_name)
        key = (zone_type, name_with_link)
        key_exists = key in school_dict

        if sort_by_distance:
            distance = float('inf')
            coords = get_school_coords(cleaned_display_name)
            if coords:
                try:
                    distance = geodesic((lat, lon), coords).miles
                except ValueError as ve:
                    print(f"  ⚠️ Distance calculation error for {cleaned_display_name}: {ve}")
                    distance = float('inf')
            # Compare distances and update only if better
            existing_distance = school_dict.get(key, float('inf'))
            if distance < existing_distance:
                school_dict[key] = distance
        else: # Not sorting by distance
             if not key_exists:
                 school_dict[key] = 0.0 # Dummy value for non-distance mode

    # --- Process Matched Zones ---
    print(f"Processing {len(matches)} matched zone polygon(s)...")
    for _, row in matches.iterrows():
        zone_type = row.get("zone_type", "Unknown")
        raw_name_str = None
        display_name = None

        try:
            # --- High School Zone ---
            if zone_type == "High":
                raw_name_str = str(row.get("High", "")).strip()
                if raw_name_str:
                    raw_name_key = raw_name_str.upper()
                    display_name = DISPLAY_NAMES_HIGH.get(raw_name_key)
                    if display_name: add_school(zone_type, display_name)
                    else: print(f"  ⚠️ Mapping MISSING: Zone='High', Raw='{raw_name_str}' (Key='{raw_name_key}') not in DISPLAY_NAMES_HIGH.")

            # --- Middle School Zone ---
            elif zone_type == "Middle":
                 raw_name_from_middle = str(row.get("Middle", "")).strip()
                 raw_name_from_name = str(row.get("Name", "")).strip()
                 raw_name_str = raw_name_from_middle if raw_name_from_middle else raw_name_from_name
                 if raw_name_str:
                     raw_name_key = raw_name_str.upper()
                     display_name = DISPLAY_NAMES_MIDDLE.get(raw_name_key)
                     if display_name: add_school(zone_type, display_name)
                     else: print(f"  ⚠️ Mapping MISSING: Zone='Middle', Raw='{raw_name_str}' (Key='{raw_name_key}') not in DISPLAY_NAMES_MIDDLE.")

            # --- Choice Zone ---
            elif zone_type == "Choice":
                 display_name = str(row.get("Name", "")).strip()
                 if display_name: add_school(zone_type, display_name)

            # --- Elementary Zone (Feeders) ---
            elif zone_type == "Elementary":
                high_school_attr = str(row.get("High", "")).strip().upper()
                if high_school_attr:
                    feeder_list = ELEMENTARY_FEEDERS.get(high_school_attr, [])
                    if feeder_list:
                        for school_name_from_feeder in feeder_list:
                             feeder_name_clean = school_name_from_feeder.strip()
                             if feeder_name_clean: add_school("Elementary", feeder_name_clean)
                    # else: print(f"  Info: No elementary feeders for HS Key: '{high_school_attr}'") # Optional info

            # --- Traditional / Magnet Zones ---
            elif zone_type == "Traditional/Magnet High":
                raw_name_str = str(row.get("Traditiona", "")).strip() # Check 'Traditiona' column name
                if raw_name_str:
                     raw_name_key = raw_name_str.upper()
                     display_name = DISPLAY_NAMES_HIGH.get(raw_name_key)
                     if display_name: add_school(zone_type, display_name)
                     else: print(f"  ⚠️ Mapping MISSING: Zone='Trad/Mag High', Raw='{raw_name_str}' (Key='{raw_name_key}') not in DISPLAY_NAMES_HIGH.")

            elif zone_type == "Traditional/Magnet Middle":
                raw_name_str = str(row.get("Traditiona", "")).strip() # Check 'Traditiona' column name
                if raw_name_str:
                     raw_name_key = raw_name_str.upper()
                     display_name = DISPLAY_NAMES_MIDDLE.get(raw_name_key)
                     if display_name: add_school(zone_type, display_name)
                     else: print(f"  ⚠️ Mapping MISSING: Zone='Trad/Mag Middle', Raw='{raw_name_str}' (Key='{raw_name_key}') not in DISPLAY_NAMES_MIDDLE.")

            elif zone_type == "Traditional/Magnet Elementary":
                 raw_name_str = str(row.get("Traditiona", "")).strip() # Check 'Traditiona' column name
                 if raw_name_str:
                      raw_name_key = raw_name_str.upper()
                      display_name = DISPLAY_NAMES_ELEMENTARY.get(raw_name_key)
                      if display_name: add_school(zone_type, display_name)
                      else: print(f"  ⚠️ Mapping MISSING: Zone='Trad/Mag Elem', Raw='{raw_name_str}' (Key='{raw_name_key}') not in DISPLAY_NAMES_ELEMENTARY.")

        except Exception as e:
            print(f"❌ Error processing matched row (Index: {row.name}, Zone Type: {zone_type}): {e}")

    # --- Add Universal Schools ---
    # print("Adding Universal Schools...") # Usually not needed unless debugging this section
    for level, schools_list in UNIVERSAL_SCHOOLS.items():
        target_zone_type = f"Traditional/Magnet {level}"
        for school_name in schools_list:
            clean_universal_name = school_name.strip()
            if clean_universal_name: add_school(target_zone_type, clean_universal_name)

    # --- Format Final Results ---
    grouped_schools = {}
    for (zone_type, name_with_link), distance in school_dict.items():
        if zone_type not in grouped_schools: grouped_schools[zone_type] = []
        display_name_no_link = name_with_link.split('](')[0].replace('[', '').strip()
        grouped_schools[zone_type].append({
            "name_linked": name_with_link,
            "name_plain": display_name_no_link,
            "distance": distance
            })

    final_results_list = []
    category_order = ["Elementary", "Middle", "High", "Traditional/Magnet Elementary", "Traditional/Magnet Middle", "Traditional/Magnet High", "Choice"]
    sorted_zone_types = sorted(grouped_schools.keys(), key=lambda z: category_order.index(z) if z in category_order else len(category_order))

    for zone_type in sorted_zone_types:
        schools = grouped_schools[zone_type]
        if not schools: continue
        schools.sort(key=lambda x: (float('inf') if x['distance'] == float('inf') else x['distance'], x['name_plain'].lower()))
        school_strings = []
        for school in schools:
            name = school['name_linked']
            dist = school['distance']
            if sort_by_distance:
                if dist == float('inf'): school_strings.append(f"{name} (distance unavailable)")
                else: school_strings.append(f"{name} ({dist:.1f} mi)")
            else:
                 school_strings.append(name)
        if school_strings:
             final_results_list.append(f"{zone_type} Schools: {', '.join(school_strings)}")

    print(f"✅ School zone processing complete. Found {len(final_results_list)} categories with schools.")
    return final_results_list


# --- Flask Routes ---
@app.route("/test")
def test():
    """Simple endpoint to check if Flask is running."""
    return "🚀 Flask is working!"

@app.route("/school-zone", methods=["POST"])
def school_zone():
    """Handles POST requests to find school zones (names only)."""
    start_time = time.time()
    data = request.get_json()
    if not data:
        print("❌ Error: Request body missing or not JSON.")
        return jsonify({"error": "Request body must be JSON"}), 400

    address = data.get("address")
    if not address or not isinstance(address, str) or not address.strip():
        print("❌ Error: Address is missing or not a valid string.")
        return jsonify({"error": "Address string is required"}), 400

    address = address.strip()
    print(f"\n--- Request /school-zone --- Address: '{address}'")

    lat, lon = geocode_address(address)

    if lat is None or lon is None:
        error_msg = f"Could not geocode address: '{address}'. Please check the address format (e.g., Street, City, State ZIP)."
        print(f"❌ Geocoding failed for request.")
        return jsonify({"error": error_msg}), 400

    print(f"📍 Geocoded to: Lat={lat:.5f}, Lon={lon:.5f}")
    zones = find_school_zones(lat, lon, all_zones_gdf, sort_by_distance=False)

    end_time = time.time()
    print(f"--- Request /school-zone completed in {end_time - start_time:.2f} seconds ---")
    return jsonify({"zones": zones}), 200

@app.route("/school-distances", methods=["POST"])
def school_distances():
    """Handles POST requests to find school zones with distances."""
    start_time = time.time()
    data = request.get_json()
    if not data:
        print("❌ Error: Request body missing or not JSON.")
        return jsonify({"error": "Request body must be JSON"}), 400

    address = data.get("address")
    if not address or not isinstance(address, str) or not address.strip():
        print("❌ Error: Address is missing or not a valid string.")
        return jsonify({"error": "Address string is required"}), 400

    address = address.strip()
    print(f"\n--- Request /school-distances --- Address: '{address}'")

    lat, lon = geocode_address(address)

    if lat is None or lon is None:
        error_msg = f"Could not geocode address: '{address}'. Please check the address format (e.g., Street, City, State ZIP)."
        print(f"❌ Geocoding failed for request.")
        return jsonify({"error": error_msg}), 400

    print(f"📍 Geocoded to: Lat={lat:.5f}, Lon={lon:.5f}")
    zones_with_distances = find_school_zones(lat, lon, all_zones_gdf, sort_by_distance=True)

    end_time = time.time()
    print(f"--- Request /school-distances completed in {end_time - start_time:.2f} seconds ---")
    return jsonify({"zones": zones_with_distances})


# --- Run App ---
if __name__ == "__main__":
    print("\n" + "="*30)
    print(" Starting Flask development server... ")
    print(" Access via http://localhost:5001 or network IP ")
    print(" Press CTRL+C to quit ")
    print("="*30 + "\n")
    # Turn off debug mode in production for security and performance!
    # Use host="0.0.0.0" to make accessible on network, default is "127.0.0.1" (localhost only).
    # Consider using a production WSGI server like Gunicorn or Waitress instead of Flask's built-in server.
    app.run(host="0.0.0.0", port=5001, debug=True) # Set debug=False for production
