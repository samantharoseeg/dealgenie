import geopandas as gpd

SHAPEFILE_PATH = "/Users/samanthagrant/Desktop/dealgenie/data/LACounty_Parcels.shp"
CITY_BOUNDARY_PATH = "/Users/samanthagrant/Desktop/dealgenie/data/City_Boundaries.shp"
OUTPUT_CSV = "/Users/samanthagrant/Desktop/dealgenie/data/lacity_apns.csv"

# Load shapefiles
print("📂 Loading parcel shapefile...")
gdf = gpd.read_file(SHAPEFILE_PATH)
print("📂 Loading City of LA boundary shapefile...")
city_boundary = gpd.read_file(CITY_BOUNDARY_PATH)

# Ensure both GeoDataFrames use the same CRS
print("🔧 Aligning coordinate reference systems...")
if gdf.crs != city_boundary.crs:
    city_boundary = city_boundary.to_crs(gdf.crs)

# Inspect columns
print("🔍 Parcel columns:", gdf.columns.tolist())
print(f"✅ Total number of parcels: {len(gdf)}")

# Filter for City of LA using a list of neighborhoods
la_neighborhoods = [
    "LOS ANGELES",
    "DEL REY",
    "MARINA DEL REY",
    "UNIVERSAL CITY",
    "SYLMAR ISLAND",
    "WEST LOS ANGELES (SAWTELLE VA)",
    "W ATHENS - WESTMONT",
    "WEST CARSON",
    "WEST CHATSWORTH",
    "WILLOWBROOK"
]
city_boundary_la = city_boundary[(city_boundary["CITYNAME_A"] == "LOS ANGELES") | (city_boundary["CITY_COMM_"].isin(la_neighborhoods))]

# Perform spatial join to find parcels within City of LA boundary
print("🔄 Performing spatial join...")
lacity_gdf = gpd.sjoin(gdf, city_boundary_la, how="inner", predicate="intersects")
print(f"✅ Found {len(lacity_gdf)} City of LA parcels")

# Save APNs
if "AIN" in lacity_gdf.columns:
    lacity_gdf["AIN"].to_csv(OUTPUT_CSV, index=False)
    print(f"📁 Saved to {OUTPUT_CSV}")
else:
    raise ValueError("❌ 'AIN' column not found in shapefile. Please check the column names.")