import time
import duckdb
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

from db_setup.utils.db_utils import get_db_path_or_url

db_path = get_db_path_or_url("duckdb")

query = """
WITH distinct_vessel_cells AS (
    SELECT 
        mmsi, 
        CS_GetParentCell(cell_z21, 21, 19) AS cell_z19
    FROM p10_cs.trajectory_cs
    
    UNION 
    
    SELECT 
        mmsi, 
        CS_GetParentCell(cell_z21, 21, 19) AS cell_z19
    FROM p10_cs.stop_cs
),
cell_counts AS (
    SELECT
        cell_z19,
        COUNT(mmsi) AS mmsi_count
    FROM distinct_vessel_cells
    GROUP BY cell_z19
)
SELECT
    cell_z19,
    mmsi_count,
    (CS_CellToTileZXY(cell_z19, 19)).x AS tile_x,
    (CS_CellToTileZXY(cell_z19, 19)).y AS tile_y
FROM cell_counts;
"""

connection = duckdb.connect(db_path)
print("Connected to DuckDB")

print(f"Executing query to retrieve distinct MMSI counts per Z19 cell...")
query_start_time = time.perf_counter()
df = connection.execute(query).df()
query_end_time = time.perf_counter()
print(
    f"Query executed in {query_end_time - query_start_time:.2f} seconds, retrieved {len(df)} rows."
)
connection.close()

print("DuckDB connection closed.")
# 1. Determine bounding box
x_min, x_max = float(df["tile_x"].min()), float(df["tile_x"].max())
y_min, y_max = float(df["tile_y"].min()), float(df["tile_y"].max())

width = int(x_max - x_min + 1)
height = int(y_max - y_min + 1)

# 2. Allocate a dense 2D array
# Initializing with np.nan ensures empty cells are rendered as background (transparent)
# rather than mapping to the bottom of the colormap.
grid = np.full((height, width), np.nan)

# 3. Populate the grid
# Note: Matrix indexing is [row, column], which translates to [y, x]
y_indices = (df["tile_y"] - y_min).astype(int)
x_indices = (df["tile_x"] - x_min).astype(int)
grid[y_indices, x_indices] = df["mmsi_count"]

# 4. Render the heatmap
fig, ax = plt.subplots(figsize=(14, 12))

# origin='upper' aligns with Slippy Map tile logic
im = ax.imshow(
    grid,
    cmap="inferno",
    origin="upper",
    norm=LogNorm(),
    extent=(x_min - 0.5, x_max + 0.5, y_max + 0.5, y_min - 0.5),
)

# Formatting
ax.set_facecolor("#1a1a1a")  # Dark background for unpopulated water/land cells
plt.colorbar(im, label="Distinct MMSI Count (Log Scale)", fraction=0.046, pad=0.04)
plt.title("Ship Traffic Coverage in Denmark (Distinct MMSIs, Z19)", fontsize=14)
plt.xlabel("Tile X")
plt.ylabel("Tile Y")

plt.tight_layout()
plt.show()
