# app/tests/inspect_choice_zone.py (UPDATED)
import os
import geopandas as gpd

def inspect_shapefile_columns_and_data(file_path, description):
    """
    Loads a shapefile and prints its columns and a sample of its data.
    """
    print("-" * 60)
    print(f"🔍 Inspecting: {description}")
    print(f"   File: {os.path.basename(file_path)}")
    print("-" * 60)

    if not os.path.exists(file_path):
        print(f"  ❌ ERROR: File not found at '{file_path}'.\n")
        return

    try:
        gdf = gpd.read_file(file_path)
        
        # --- 1. Print all available columns ---
        print("  ✅ Available Columns in Shapefile:")
        print(f"     {list(gdf.columns)}\n")

        # --- 2. Print the first 5 rows of data (excluding the geometry) ---
        print("  ✅ Sample Data (first 5 rows):")
        # Use .drop() to hide the long 'geometry' column for cleaner output
        sample_data = gdf.drop(columns='geometry', errors='ignore').head()
        print(sample_data.to_string()) # .to_string() formats it nicely
        
        print("\n" + "-" * 60)
        print("  ACTION: Look at the sample data above to identify which column")
        print("          contains the school names (e.g., 'ATKINSON', 'HUDSON MIDDLE').")
        print("-" * 60 + "\n")


    except Exception as e:
        print(f"  ❌ ERROR: Could not read or process the shapefile. Details: {e}\n")


if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    # Go up two levels (from tests -> app -> project root) to find the data directory
    DATA_DIR = os.path.join(BASE_DIR, "..", "..", "data")
    
    choice_zone_file = os.path.join(DATA_DIR, "ChoiceZone", "ChoiceZone.shp")

    print("\n--- Starting Detailed Choice Zone GIS Data Inspection ---")
    inspect_shapefile_columns_and_data(
        file_path=choice_zone_file,
        description="Choice Zone Schools"
    )