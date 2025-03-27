import pandas as pd
import geopandas as gpd
import os
from shapely.geometry import Point
from geopy.geocoders import Nominatim

# Base path to the data folder
DATA_DIR = "data"

# Define file paths
choice_path = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")
high_path = os.path.join(DATA_DIR, "High", "High.shp")
middle_path = os.path.join(DATA_DIR, "Middle", "Middle.shp")

# Load each shapefile and label its type
choice_gdf = gpd.read_file(choice_path)
choice_gdf["zone_type"] = "Choice"

high_gdf = gpd.read_file(high_path)
high_gdf["zone_type"] = "High"

middle_gdf = gpd.read_file(middle_path)
middle_gdf["zone_type"] = "Middle"

# Combine them all into one GeoDataFrame (optional but useful)
all_zones_gdf = gpd.GeoDataFrame(
    pd.concat([choice_gdf, high_gdf, middle_gdf], ignore_index=True)
)
# 🛠 FIX: Convert to lat/lon CRS so it matches the geocoded point
all_zones_gdf = all_zones_gdf.to_crs(epsg=4326)

# Quick preview
# print(all_zones_gdf.head())
# print("Columns:", all_zones_gdf.columns)
# print("Zone types:", all_zones_gdf['zone_type'].unique())

# --- Zone Finder Function ---
def find_school_zones(lat, lon, all_zones_gdf):
    point = Point(lon, lat)  # Note: lon = x, lat = y

    matches = all_zones_gdf[all_zones_gdf.geometry.contains(point)]

    if matches.empty:
        return "No school zones found for this location."
    
    result = []
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

        result.append(f"{zone_type} zone: {name}")

        print("Matched row:", row)
    
    return result

# --- Temporary Test Call (optional for now) ---
# lat, lon = 38.246, -85.759  # Just a placeholder for now
# print(find_school_zones(lat, lon, all_zones_gdf))


# --- Geocoding Function ---
geolocator = Nominatim(user_agent="jcps_school_bot")

def geocode_address(address):
    location = geolocator.geocode(address)
    if location:
        return location.latitude, location.longitude
    return None, None


# --- TEST ADDRESS ---
if __name__ == "__main__":
    # Test with a real address
    address = "4300 Breckenridge Ln, Louisville, KY 40218"
    lat, lon = geocode_address(address)

    print(f"Geocoded lat/lon: {lat}, {lon}")
    print("CRS:", all_zones_gdf.crs)


    if lat is None:
        print("Could not geocode the address.")
    else:
        print(f"Lat/Lon: {lat}, {lon}")
        print("Testing point:", Point(lon, lat))
        zones = find_school_zones(lat, lon, all_zones_gdf)
        print("Matching zones:", zones)
