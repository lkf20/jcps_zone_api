from flask import Flask, request, jsonify
from geopy.geocoders import Nominatim
import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
import os





app = Flask(__name__)
geolocator = Nominatim(user_agent="jcps_school_bot")

# Load and prepare the zone data
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "..", "data")
choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")

# Print resolved full path
print("🔍 Choice zone shapefile path:", os.path.abspath(choice_path))

# Check if the file exists
if not os.path.exists(choice_path):
    print("❌ ERROR: ChoiceZone.shp does not exist at that path!")
else:
    print("✅ Found ChoiceZone.shp file.")


high_path = os.path.join(DATA_DIR, "High", "High.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Middle.shp")

print("📁 Loading shapefiles from:", DATA_DIR)

choice_gdf = gpd.read_file(choice_path)
choice_gdf["zone_type"] = "Choice"

high_gdf = gpd.read_file(high_path)
high_gdf["zone_type"] = "High"

middle_gdf = gpd.read_file(middle_path)
middle_gdf["zone_type"] = "Middle"

all_zones_gdf = gpd.GeoDataFrame(
    pd.concat([choice_gdf, high_gdf, middle_gdf], ignore_index=True)
)

# Convert to lat/lon CRS
all_zones_gdf = all_zones_gdf.to_crs(epsg=4326)

# --- Helper Functions ---
def geocode_address(address):
    location = geolocator.geocode(address)
    if location:
        return location.latitude, location.longitude
    return None, None

def find_school_zones(lat, lon, gdf):
    point = Point(lon, lat)
    matches = gdf[gdf.geometry.contains(point)]

    if matches.empty:
        return []

    results = []
    for _, row in matches.iterrows():
        zone_type = row["zone_type"]
        if zone_type == "High":
            name = row["High"] if pd.notna(row["High"]) else row["Name"]
        elif zone_type == "Middle":
            name = row["Middle"] if pd.notna(row["Middle"]) else row["Name"]
        elif zone_type == "Choice":
            name = row["Name"]
        else:
            name = "Unknown"
        results.append(f"{zone_type} zone: {name}")
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


# --- Run the app ---
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)

