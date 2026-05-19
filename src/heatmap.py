import os
import time
import duckdb
import matplotlib.pyplot as plt
import math

import datashader as ds
from datashader.utils import export_image
import datashader.transfer_functions as tf
import matplotlib.pyplot as plt

from db_setup.utils.db_utils import get_db_path_or_url

db_path = get_db_path_or_url("duckdb")

connection = duckdb.connect(db_path)
print("Connected to DuckDB")

ZOOM = 7

cache_file = f"heatmap_z{ZOOM}_full_cache.parquet"

img_name = f"denmark_ship_traffic_z{ZOOM}_full"

# 1. Caching Mechanism: Check if Parquet cache exists
if not os.path.exists(cache_file):
    print("Cache not found. Executing query and writing to Parquet...")
    query = f"""
    COPY (
        WITH distinct_vessel_cells AS (
            SELECT mmsi, CS_GetParentCell(cell_z21, 21, {ZOOM}) AS cell FROM p10_cs.trajectory_cs
            -- WHERE mmsi IN (219281000,219437000,248189000,220464000,219435000,636023946,219358000,219115000,636023944,220253000,477552100,219136000,636091995,563076300,219543000,245897000,636024484,266331000,220431000,256800000)
            UNION 
            SELECT mmsi, CS_GetParentCell(cell_z21, 21, {ZOOM}) AS cell FROM p10_cs.stop_cs
            -- WHERE mmsi IN (219281000,219437000,248189000,220464000,219435000,636023946,219358000,219115000,636023944,220253000,477552100,219136000,636091995,563076300,219543000,245897000,636024484,266331000,220431000,256800000)
        ),
        cell_counts AS (
            SELECT cell, COUNT(mmsi) AS mmsi_count
            FROM distinct_vessel_cells
            GROUP BY cell
        )
        SELECT
            cell,
            mmsi_count,
            (CS_CellToTileZXY(cell, {ZOOM})).x AS tile_x,
            (CS_CellToTileZXY(cell, {ZOOM})).y AS tile_y
        FROM cell_counts
    ) TO '{cache_file}' (FORMAT PARQUET);
    """
    query_start_time = time.perf_counter()

    connection.execute(query)

    query_end_time = time.perf_counter()
    print(
        f"Query executed in {query_end_time - query_start_time:.2f} seconds and results cached to '{cache_file}'."
    )
else:
    print(f"Found existing cache: {cache_file}")

# Load the data from the Parquet file into a Pandas DataFrame
print("Loading data for visualization...")
df = connection.execute(f"SELECT * FROM '{cache_file}'").df()


def lonlat_to_zxy(lon, lat, z):
    """Converts WGS84 lon/lat to Slippy Map XYZ coordinates."""
    n = 2.0**z
    x = (lon + 180.0) / 360.0 * n
    lat_rad = math.radians(lat)
    y = (1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n
    return int(x), int(y)


# Denmark's approximate bounding box
DK_LON_MIN, DK_LON_MAX = 1.5, 17.5
DK_LAT_MIN, DK_LAT_MAX = 52.5, 58.5

# Calculate Slippy Map bounds
x_min_dk, y_max_dk = lonlat_to_zxy(DK_LON_MIN, DK_LAT_MIN, ZOOM)
x_max_dk, y_min_dk = lonlat_to_zxy(DK_LON_MAX, DK_LAT_MAX, ZOOM)

# 1. Calculate spatial ranges in Web Mercator tile units
dx_tiles = x_max_dk - x_min_dk
dy_tiles = y_max_dk - y_min_dk

# 2. Render at high resolution, then spread each tile into a visible block
TARGET_WIDTH = 3840
pixels_per_tile = TARGET_WIDTH / max(dx_tiles, 1)
calculated_height = max(int(TARGET_WIDTH * (dy_tiles / max(dx_tiles, 1))), 1)

# 3. Create the Canvas using the high-resolution dimensions
cvs = ds.Canvas(
    plot_width=TARGET_WIDTH,
    plot_height=calculated_height,
    x_range=(x_min_dk, x_max_dk),
    y_range=(y_min_dk, y_max_dk),
)

# 4. Aggregate points onto the canvas
agg = cvs.points(df, "tile_x", "tile_y", ds.sum("mmsi_count"))

# 5. Invert the Y-axis on the aggregated DataArray
agg = agg[::-1, :]

# 6. Shade, spread, and export
img = tf.shade(agg, cmap=plt.get_cmap("inferno"), how="log")
img = tf.spread(img, px=max(1, int(round(pixels_per_tile / 2))), shape="square")
img = tf.set_background(img, "#1a1a1a")
export_image(img, img_name, background="#1a1a1a")
print(f"Heatmap generated and saved as '{img_name}.png'")
