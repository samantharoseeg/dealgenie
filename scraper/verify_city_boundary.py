import geopandas as gpd

# Path to the city boundary shapefile
CITY_BOUNDARY_PATH = "data/City_Boundaries.shp"  # Relative path

# Load the shapefile
print("📂 Loading city boundary shapefile...")
try:
    city_gdf = gpd.read_file(CITY_BOUNDARY_PATH)
except Exception as e:
    print(f"❌ Error loading shapefile: {e}")
    exit()

# Inspect the shapefile
print("🔍 Shapefile Info:")
print(f"Total number of features: {len(city_gdf)}")
print(f"Columns: {city_gdf.columns.tolist()}")
print(f"Coordinate Reference System (CRS): {city_gdf.crs}")

# Look for potential city name fields
possible_city_fields = [col for col in city_gdf.columns if "city" in col.lower() or "name" in col.lower()]
if not possible_city_fields:
    print("❌ No likely city name field found. Please check the columns manually.")
else:
    print(f"🔎 Possible city name fields: {possible_city_fields}")

    # Check each potential field for "Los Angeles"
    for field in possible_city_fields:
        print(f"\nInspecting field '{field}':")
        unique_values = city_gdf[field].unique()
        print(f"Unique values in '{field}': {unique_values[:10]}")  # Show first 10 for brevity

        # Look for "Los Angeles" (case-insensitive)
        la_matches = [val for val in unique_values if isinstance(val, str) and "los angeles" in val.lower()]
        if la_matches:
            print(f"✅ Found 'Los Angeles' in field '{field}': {la_matches}")
        else:
            print(f"❌ 'Los Angeles' not found in field '{field}'.")

# Display the first few rows for manual inspection
print("\n📋 First 5 rows of the shapefile:")
print(city_gdf.head())