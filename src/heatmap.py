import os
import time
import duckdb
import matplotlib.pyplot as plt

import datashader as ds
from datashader.utils import export_image
import datashader.transfer_functions as tf
import matplotlib.pyplot as plt

from db_setup.utils.db_utils import get_db_path_or_url

db_path = get_db_path_or_url("duckdb")

connection = duckdb.connect(db_path)
print("Connected to DuckDB")

cache_file = "heatmap_z19_cache.parquet"

# 1. Caching Mechanism: Check if Parquet cache exists
if not os.path.exists(cache_file):
    print("Cache not found. Executing query and writing to Parquet...")
    query = """
    COPY (
        WITH distinct_vessel_cells AS (
            SELECT mmsi, CS_GetParentCell(cell_z21, 21, 19) AS cell_z19 FROM p10_cs.trajectory_cs
            UNION 
            SELECT mmsi, CS_GetParentCell(cell_z21, 21, 19) AS cell_z19 FROM p10_cs.stop_cs
        ),
        cell_counts AS (
            SELECT cell_z19, COUNT(mmsi) AS mmsi_count
            FROM distinct_vessel_cells
            GROUP BY cell_z19
        )
        SELECT
            cell_z19,
            mmsi_count,
            (CS_CellToTileZXY(cell_z19, 19)).x AS tile_x,
            (CS_CellToTileZXY(cell_z19, 19)).y AS tile_y
        FROM cell_counts
    ) TO 'heatmap_z19_cache.parquet' (FORMAT PARQUET);
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

# 1. Define bounding box
x_min, x_max = df["tile_x"].min(), df["tile_x"].max()
y_min, y_max = df["tile_y"].min(), df["tile_y"].max()

# 2. Create the Canvas using standard (min, max) bounds
cvs = ds.Canvas(
    plot_width=3840,
    plot_height=3840,
    x_range=(x_min, x_max),
    y_range=(y_min, y_max),  # <-- FIXED: Must be strictly (min, max)
)

# 3. Aggregate points onto the canvas
agg = cvs.points(df, "tile_x", "tile_y", ds.sum("mmsi_count"))

# 3b. Invert the Y-axis on the aggregated DataArray
# Datashader natively places the minimum Y value at the bottom of the image.
# Reversing the array's Y-axis aligns it with the Slippy Map orientation (North at top).
agg = agg[::-1, :]

# 4. Shade the aggregation matrix
img = tf.shade(agg, cmap=plt.get_cmap("inferno"), how="log")

# 5. Set the background color
img = tf.set_background(img, "#1a1a1a")

# 6. Export
export_image(img, "denmark_ship_traffic_z19_datashader", background="#1a1a1a")
