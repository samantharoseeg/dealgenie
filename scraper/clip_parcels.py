import geopandas as gpd
import os
import glob

# Base project directory (one level up from this script)
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Data folders
PARCELS_DIR  = os.path.join(BASE_DIR, "dealgenie_data", "parcels")
BOUNDARY_DIR = os.path.join(BASE_DIR, "dealgenie_data", "city_boundary")
OUTPUT_PATH  = os.path.join(BASE_DIR, "dealgenie_data", "la_city_parcels.gpkg")

# 1. Locate the parcel shapefile (recursively)
parcel_shps = glob.glob(os.path.join(PARCELS_DIR, "**", "*.shp"), recursive=True)
if not parcel_shps:
    raise FileNotFoundError(f"No .shp found under {PARCELS_DIR}")
parcel_shp = parcel_shps[0]

# 2. Locate the city boundary shapefile
boundary_shps = glob.glob(os.path.join(BOUNDARY_DIR, "**", "*.shp"), recursive=True)
if not boundary_shps:
    raise FileNotFoundError(f"No .shp found under {BOUNDARY_DIR}")
boundary_shp = boundary_shps[0]

# 3. Load data
parcels       = gpd.read_file(parcel_shp)
city_boundary = gpd.read_file(boundary_shp)

# 4. Reproject parcels to match city boundary CRS
parcels = parcels.to_crs(city_boundary.crs)

# 5. Spatial join – keep only parcels intersecting the city
la_city_parcels = gpd.sjoin(
    parcels,
    city_boundary[["geometry"]],
    how="inner",
    predicate="intersects"
)

# 6. Save the result
la_city_parcels.to_file(OUTPUT_PATH, driver="GPKG")
print(f"✅ Clipped parcels saved to {OUTPUT_PATH}")
