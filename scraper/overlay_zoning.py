# overlay_zoning.py
import geopandas as gpd
import os
import glob

# 1) Define base/project directories
BASE_DIR      = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
PARCELS_FILE  = os.path.join(BASE_DIR, "dealgenie_data", "la_city_parcels.gpkg")
ZONING_DIR    = os.path.join(BASE_DIR, "dealgenie_data", "zoning")
OUTPUT_FILE   = os.path.join(BASE_DIR, "dealgenie_data", "la_city_parcels_with_zoning.gpkg")

# 2) Load clipped LA City parcels
print(f"Loading parcels from {PARCELS_FILE}...")
parcels = gpd.read_file(PARCELS_FILE)

# 3) Find and load the zoning shapefile
zoning_shps = glob.glob(os.path.join(ZONING_DIR, "**", "*.shp"), recursive=True)
if not zoning_shps:
    raise FileNotFoundError(f"No .shp found under {ZONING_DIR}")
zoning_path = zoning_shps[0]
print(f"Found zoning shapefile: {zoning_path}")
zoning = gpd.read_file(zoning_path)

# 4) Inspect available zoning fields
print("Available zoning fields:", zoning.columns.tolist())

# 5) Clean up any leftover index columns from prior joins
for df in (parcels, zoning):
    for col in ("index_left", "index_right"):
        if col in df.columns:
            df.drop(columns=[col], inplace=True)

# 6) Choose the zoning attribute field
ZONING_FIELD = "ZONE_COMPL"  # <-- already identified in your layer

if ZONING_FIELD not in zoning.columns:
    raise ValueError(f"Field '{ZONING_FIELD}' not found in zoning layer.")

# 7) Reproject zoning to match parcels CRS
zoning = zoning.to_crs(parcels.crs)

# 8) Spatial join: tag parcels with zoning
print(f"Joining parcels to zoning on field '{ZONING_FIELD}'...")
parcels_with_zoning = gpd.sjoin(
    parcels,
    zoning[[ZONING_FIELD, "geometry"]],
    how="left",
    predicate="intersects"
)

# 9) Save the result
parcels_with_zoning.to_file(OUTPUT_FILE, driver="GPKG")
print(f"✅ Parcels with zoning saved to {OUTPUT_FILE}")
