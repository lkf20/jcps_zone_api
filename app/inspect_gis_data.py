# app/inspect_gis_data.py

# app/inspect_gis_data.py (Updated to include MST_MS_Bnds.shp)

import os
import geopandas as gpd
import pandas as pd

def inspect_shapefile(file_path, column_name, description):
    """
    Loads a shapefile and prints all unique, non-empty values from a specified column.
    """
    print("-" * 50)
    print(f"🔍 Inspecting: {description}")
    print(f"   File: {os.path.basename(file_path)}")
    print(f"   Column: '{column_name}'")
    print("-" * 50)

    if not os.path.exists(file_path):
        print("  ❌ ERROR: File not found.")
        print("\n")
        return

    try:
        gdf = gpd.read_file(file_path)
        
        if column_name not in gdf.columns:
            print(f"  ❌ ERROR: Column '{column_name}' does not exist in this file.")
            print(f"     Available columns are: {list(gdf.columns)}")
            print("\n")
            return

        # Get unique, non-empty school names and sort them
        unique_names = gdf[column_name].dropna().unique()
        sorted_names = sorted([str(name).strip() for name in unique_names if str(name).strip()])

        if not sorted_names:
            print("  ⚠️ No valid school names found in this column.")
        else:
            print(f"  ✅ Found {len(sorted_names)} unique school names:")
            for name in sorted_names:
                print(f"     - {name}")
        
        print("\n")

    except Exception as e:
        print(f"  ❌ ERROR: Could not read or process the shapefile. Details: {e}")
        print("\n")


if __name__ == "__main__":
    # Define the base path to the data directory
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, "..", "data")

    # --- List of shapefiles and columns to inspect ---
    files_to_inspect = [
        (
            "MST Magnet Middle Schools", # <-- NEW ENTRY
            os.path.join(DATA_DIR, "MagnetMiddle", "MST_MS_Bnds.shp"),
            "MST" # This is our best guess for the column name
        ),
        (
            "Geographical Magnet Middle Schools",
            os.path.join(DATA_DIR, "TraditionalMiddle", "Traditional_MS_Bnds.shp"),
            "Traditiona"
        ),
        (
            "Resides Middle Schools",
            os.path.join(DATA_DIR, "Middle", "Resides_MS_Boundaries.shp"),
            "Middle"
        )
    ]

    print("\n--- Starting GIS Data Inspection ---")
    for description, path, column in files_to_inspect:
        inspect_shapefile(path, column, description)