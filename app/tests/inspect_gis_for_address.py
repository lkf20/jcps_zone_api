# app/tests/inspect_gis_for_address.py
import os
import geopandas as gpd
import googlemaps
from shapely.geometry import Point

def check_address_in_shapefile(address, gmaps_client, file_path, column_name, description):
    """
    Geocodes an address and checks which polygon it falls into within a specific shapefile.
    """
    print("-" * 60)
    print(f"🔍 Checking Address in: {description}")
    print(f"   File: {os.path.basename(file_path)}")
    print("-" * 60)

    if not os.path.exists(file_path):
        print(f"  ❌ ERROR: Shapefile not found at '{file_path}'.\n")
        return

    print(f"  Geocoding address: '{address}'...")
    try:
        results = gmaps_client.geocode(address)
        if not results:
            print("  ❌ ERROR: Could not geocode address.\n")
            return
        location = results[0]['geometry']['location']
        point = Point(location['lng'], location['lat'])
        print(f"  ✅ Geocoded to: (Lat: {location['lat']:.5f}, Lon: {location['lng']:.5f})")
    except Exception as e:
        print(f"  ❌ ERROR during geocoding: {e}\n")
        return

    print("  Loading shapefile and checking for matches...")
    try:
        gdf = gpd.read_file(file_path)
        gdf = gdf.to_crs(epsg=4326)

        if column_name not in gdf.columns:
            print(f"  ❌ ERROR: Column '{column_name}' not in shapefile. Available columns: {list(gdf.columns)}\n")
            return
            
        matches = gdf[gdf.geometry.contains(point)]

        if matches.empty:
            print("  ✅ RESULT: Address is NOT inside any zone in this file.\n")
        else:
            print(f"  ❗️ FOUND {len(matches)} MATCH(ES)! Address is inside the following zone(s):")
            for index, row in matches.iterrows():
                zone_name = row[column_name]
                print(f"     - {zone_name.strip().upper()}")
                
    except Exception as e:
        print(f"  ❌ ERROR reading or processing the shapefile: {e}")
    finally:
        print("-" * 60 + "\n")


if __name__ == "__main__":
    # --- Configuration ---
    # <<< START: CORRECTED CODE >>>
    ADDRESS_TO_TEST = "717 Gwendolyn St, Louisville, KY 40203"
    # <<< END: CORRECTED CODE >>>
    
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "..", "..", "data")

    # --- Initialize Google Maps Client ---
    GOOGLE_MAPS_API_KEY = os.environ.get('GOOGLE_MAPS_API_KEY')
    if not GOOGLE_MAPS_API_KEY:
        print("\n❌ FATAL ERROR: GOOGLE_MAPS_API_KEY environment variable not set.")
        exit()
    gmaps = googlemaps.Client(key=GOOGLE_MAPS_API_KEY)
    
    # --- Shapefile to Investigate ---
    mst_middle_file = os.path.join(DATA_DIR, "MagnetMiddle", "MST_MS_Bnds.shp")

    print(f"\n--- Running Targeted GIS Diagnostic for address: '{ADDRESS_TO_TEST}' ---")
    
    check_address_in_shapefile(
        address=ADDRESS_TO_TEST,
        gmaps_client=gmaps,
        file_path=mst_middle_file,
        column_name="MST",
        description="MST Magnet Middle Schools"
    )