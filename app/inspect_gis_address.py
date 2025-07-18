# app/inspect_gis_data.py (New version for targeted GIS checks)

import os
import geopandas as gpd
from shapely.geometry import Point
from geopy.geocoders import Nominatim
import warnings

# Suppress a common warning from geopy about synchronous geocoding
warnings.filterwarnings("ignore", category=UserWarning, module='geopy')

def check_address_in_shapefile(address, file_path, column_name, description):
    """
    Geocodes an address and checks which polygon it falls into within a specific shapefile.
    """
    print("-" * 60)
    print(f"🔍 Checking Address in: {description}")
    print(f"   File: {os.path.basename(file_path)}")
    print("-" * 60)

    # --- 1. Check if files exist ---
    if not os.path.exists(file_path):
        print(f"  ❌ ERROR: Shapefile not found at '{file_path}'.")
        return

    # --- 2. Geocode the address ---
    print(f"  Geocoding address: '{address}'...")
    geolocator = Nominatim(user_agent="jcps_diagnostic_tool/1.0")
    try:
        location = geolocator.geocode(address, timeout=10)
        if not location:
            print("  ❌ ERROR: Could not geocode address.")
            return
        point = Point(location.longitude, location.latitude)
        print(f"  ✅ Geocoded to: (Lat: {location.latitude:.5f}, Lon: {location.longitude:.5f})")
    except Exception as e:
        print(f"  ❌ ERROR during geocoding: {e}")
        return

    # --- 3. Load Shapefile and perform spatial query ---
    print("  Loading shapefile and checking for matches...")
    try:
        gdf = gpd.read_file(file_path)
        # Ensure the shapefile is in the same coordinate system as our point (WGS 84)
        gdf = gdf.to_crs(epsg=4326)

        if column_name not in gdf.columns:
            print(f"  ❌ ERROR: Column '{column_name}' not in shapefile. Available columns: {list(gdf.columns)}")
            return
            
        # Find which polygon contains the point
        matches = gdf[gdf.geometry.contains(point)]

        if matches.empty:
            print("  ✅ RESULT: Address is NOT inside any zone in this file.")
        else:
            print(f"  ❗️ FOUND MATCH(ES)! Address is inside the following zone(s):")
            for index, row in matches.iterrows():
                school_name = row[column_name]
                print(f"     - {school_name}")
                
    except Exception as e:
        print(f"  ❌ ERROR reading or processing the shapefile: {e}")
    finally:
        print("-" * 60 + "\n")


if __name__ == "__main__":
    # --- Configuration ---
    ADDRESS_TO_TEST = "4425 Preston Hwy, Louisville, KY"
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "..", "data")

    # --- Shapefile to Investigate ---
    mst_middle_file = os.path.join(DATA_DIR, "MagnetMiddle", "MST_MS_Bnds.shp")

    print(f"\n--- Running Targeted GIS Diagnostic for address: '{ADDRESS_TO_TEST}' ---")
    check_address_in_shapefile(
        address=ADDRESS_TO_TEST,
        file_path=mst_middle_file,
        column_name="MST", # The column containing school names in this file
        description="MST Magnet Middle Schools"
    )