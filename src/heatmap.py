import os
import time
import duckdb
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm

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

# 2. Determine bounding box
x_min, x_max = float(df["tile_x"].min()), float(df["tile_x"].max())
y_min, y_max = float(df["tile_y"].min()), float(df["tile_y"].max())

width = int(x_max - x_min + 1)
height = int(y_max - y_min + 1)

# 3. Allocate a dense 2D array
grid = np.full((height, width), np.nan)

# 4. Populate the grid
y_indices = (df["tile_y"] - y_min).astype(int)
x_indices = (df["tile_x"] - x_min).astype(int)
grid[y_indices, x_indices] = df["mmsi_count"]

# 5. Render the heatmap
fig, ax = plt.subplots(figsize=(14, 12))

im = ax.imshow(
    grid,
    cmap="inferno",
    origin="upper",
    norm=LogNorm(),
    extent=(x_min - 0.5, x_max + 0.5, y_max + 0.5, y_min - 0.5),
)

# Formatting
ax.set_facecolor("#1a1a1a")
plt.colorbar(im, label="Distinct MMSI Count (Log Scale)", fraction=0.046, pad=0.04)
plt.title("Ship Traffic Coverage in Denmark (Distinct MMSIs, Z19)", fontsize=14)
plt.xlabel("Tile X")
plt.ylabel("Tile Y")

plt.tight_layout()

# 6. Export the plot (Must be called before plt.show())
# Save as PDF for lossless inclusion in LaTeX (ACM format)
pdf_path = "denmark_ship_traffic_z19.pdf"
plt.savefig(pdf_path, format="pdf", bbox_inches="tight")
print(f"Plot exported to {pdf_path}")

# Save as a high-DPI PNG for standard viewing/presentations
png_path = "denmark_ship_traffic_z19.png"
plt.savefig(
    png_path, format="png", dpi=300, bbox_inches="tight", facecolor=fig.get_facecolor()
)
print(f"Plot exported to {png_path}")

plt.show()
