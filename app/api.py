from flask import Flask, request, jsonify
from geopy.geocoders import Nominatim
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os
import json



app = Flask(__name__)
geolocator = Nominatim(user_agent="jcps_school_bot")


# Load and prepare the zone data
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data_alt")

choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "Resides_HS_Boundaries.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp")
elementary_path = os.path.join(DATA_DIR, "Elementary", "Resides_ES_Clusters_Boundaries.shp")
traditional_middle_path = os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp")
traditional_high_path = os.path.join(DATA_DIR, "TraditionalHigh", "Traditional_HS_Bnds.shp")
traditional_elem_path = os.path.join(DATA_DIR, "TraditionalElementary", "Traditional_ES_Bnds.shp")



# High School display names
DISPLAY_NAMES_HIGH = {
    "ATHERTON": "Atherton High",
    "BALLARD": "Ballard High",
    "DOSS": "Doss High",
    "EASTERN": "Eastern High",
    "FAIRDALE": "Fairdale High",
    "FERN CREEK": "Fern Creek High",
    "IROQUOIS": "Iroquois High",
    "JEFFERSONTOWN": "Jeffersontown High",
    "Butler": "Butler Traditional High",
    "Male": "Louisville Male High"}

# Middle School display names
DISPLAY_NAMES_MIDDLE = {
    "Barrett": "Barret Traditional Middle",
    "JCTMS": "Jefferson County Traditional Middle",
    "Johnson": "Johnson Traditional Middle",
}

# Elementary School display names
DISPLAY_NAMES_ELEMENTARY = {
    "Audubon": "Audubon Traditional Elementary",
    "Carter": "Carter Traditional Elementary",
    "Foster": "Foster Traditional Academy",
    "Greathouse": "Greathouse/Shryock Traditional",
    "Schaffner": "Schaffner Traditional Elementary",
}

UNIVERSAL_SCHOOLS = {
    "Elementary": ["Brandeis Elementary","Coleridge-Taylor Montessori Elementary","Hawthorne Elementary","J. Graham Brown School","Lincoln Elementary Performing Arts","Young Elementary"],
    "Middle": ["Grace M. James Academy of Excellence","J. Graham Brown School","W.E.B. DuBois Academy","Western Middle School for the Arts"],
    "High": ["Central High Magnet Career Academy","duPont Manual High","J. Graham Brown School","W.E.B. DuBois Academy","Western High"]
}

# Load the elementary schools JSON
feeders_path = os.path.join(BASE_DIR, "elementary_feeders.json")
with open(feeders_path, "r") as f:
    ELEMENTARY_FEEDERS = json.load(f)


# Load the URLs JSON
report_links_path = os.path.join(BASE_DIR, "school_report_links.json")
with open(report_links_path, "r") as f:
    SCHOOL_REPORT_LINKS = json.load(f)



# Load the shapefiles
choice_gdf = gpd.read_file(choice_path)
choice_gdf["zone_type"] = "Choice"

high_gdf = gpd.read_file(high_path)
high_gdf["zone_type"] = "High"

middle_gdf = gpd.read_file(middle_path)
middle_gdf["zone_type"] = "Middle"

elementary_gdf = gpd.read_file(elementary_path)
elementary_gdf["zone_type"] = "Elementary"

traditional_middle_gdf = gpd.read_file(traditional_middle_path)
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
    ], ignore_index=True)
).to_crs(epsg=4326)



# Convert to lat/lon CRS
all_zones_gdf = all_zones_gdf.to_crs(epsg=4326)

# --- Helper Functions ---
def geocode_address(address):
    location = geolocator.geocode(address)
    if location:
        return location.latitude, location.longitude
    return None, None

def find_school_zones(lat, lon, gdf):
    def make_linked_name(display_name):
        normalized = display_name.strip().lower()
        link = SCHOOL_REPORT_LINKS.get(normalized)
        return f"[{display_name}]({link})" if link else display_name

    point = Point(lon, lat)
    matches = gdf[gdf.geometry.contains(point)]

    if matches.empty:
        return []

    results = []

    for _, row in matches.iterrows():
        zone_type = row["zone_type"]

        if zone_type == "High":
            raw_name = str(row.get("High", "Unknown"))
            display_name = DISPLAY_NAMES_HIGH.get(raw_name.upper(), f"{raw_name} High")
            name = make_linked_name(display_name)

        elif zone_type == "Middle":
            raw_name = row["Middle"] if pd.notna(row["Middle"]) else row["Name"]
            display_name = f"{raw_name} Middle"
            name = make_linked_name(display_name)

        elif zone_type == "Choice":
            name = row["Name"]

        elif zone_type == "Elementary":
            high_school = str(row.get("High", "Unknown"))
            feeder_list = ELEMENTARY_FEEDERS.get(high_school.upper(), [])
            linked_names = [
                make_linked_name(school) for school in feeder_list
            ] if feeder_list else ["Unknown"]
            name = ", ".join(linked_names)

        elif zone_type == "Traditional/Magnet High":
            raw_name = row.get("Traditiona", "Unknown")
            display_name = DISPLAY_NAMES_HIGH.get(str(raw_name), f"{raw_name} High School")
            name = make_linked_name(display_name)

        elif zone_type == "Traditional/Magnet Middle":
            raw_name = row.get("Traditiona")
            display_name = DISPLAY_NAMES_MIDDLE.get(str(raw_name), f"{raw_name} Middle School")
            name = make_linked_name(display_name)

        elif zone_type == "Traditional/Magnet Elementary":
            raw_name = row.get("Traditiona", "Unknown")
            display_name = DISPLAY_NAMES_ELEMENTARY.get(str(raw_name), f"{raw_name} Elementary School")
            name = make_linked_name(display_name)

        else:
            name = "Unknown"

        results.append(f"{zone_type} Schools: {name}")

    # --- Append universal schools to their respective sections ---
    for level, display_dict in [
        ("High", DISPLAY_NAMES_HIGH),
        ("Middle", DISPLAY_NAMES_MIDDLE),
        ("Elementary", DISPLAY_NAMES_ELEMENTARY),
    ]:
        universal = UNIVERSAL_SCHOOLS.get(level, [])
        if universal:
            linked = [make_linked_name(display_dict.get(s.upper(), s)) for s in universal]
            sorted_linked = sorted(linked, key=lambda x: x.lower())
            results.append(f"Traditional/Magnet {level} Schools: {', '.join(sorted_linked)}")

    return results


# --- Flask Route ---
@app.route("/test")
def test():
    return "🚀 Flask is working!"

@app.route("/school-zone", methods=["POST"])
def school_zone():
    data = request.get_json()
    print("🔍 Incoming data:", data)

    address = data.get("address")
    print("📬 Received address:", address)

    if not address:
        print("❌ No address provided.")
        return jsonify({"error": "Address is required"}), 400

    lat, lon = geocode_address(address)
    print(f"📍 Geocoded to: lat={lat}, lon={lon}")

    if lat is None:
        print("❌ Could not geocode address.")
        return jsonify({"error": "Could not geocode address"}), 400

    zones = find_school_zones(lat, lon, all_zones_gdf)
    print("🏫 Matched zones:", zones)

    return jsonify({"zones": zones}), 200

# @app.route("/test-all-schools", methods=["GET"])
# def test_all_schools():
#     results = []

#     # Go through each display name list
#     for level, display_dict in [
#         ("High", DISPLAY_NAMES_HIGH),
#         ("Middle", DISPLAY_NAMES_MIDDLE),
#         ("Elementary", DISPLAY_NAMES_ELEMENTARY),
#     ]:
#         school_names = list(display_dict.values())
#         linked = [f"[{name}]({SCHOOL_REPORT_LINKS.get(name.strip().lower(), 'NO LINK FOUND')})" for name in sorted(school_names)]
#         results.append(f"✅ {level} Schools: {', '.join(linked)}")

#     # Include Universal schools (already in the same display dicts)
#     for level, names in UNIVERSAL_SCHOOLS.items():
#         linked = [f"[{name}]({SCHOOL_REPORT_LINKS.get(name.strip().lower(), 'NO LINK FOUND')})" for name in sorted(names)]
#         results.append(f"🌐 Universal {level} Schools: {', '.join(linked)}")

#     return jsonify({"all_schools": results})



# --- Run the app ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

