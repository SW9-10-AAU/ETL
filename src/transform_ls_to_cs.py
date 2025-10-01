import mercantile
from psycopg import Connection
from shapely import convex_hull, from_wkb, Point, LineString, Polygon, MultiPoint

ZOOM = 21


def fetch_trajectories(conn: Connection):
    cur = conn.cursor()

    cur.execute("SELECT trajectory_id, ST_AsBinary(geom) FROM ls_experiment.trajectory_ls_naive;")
    rows = cur.fetchall()
    cur.close()
    return rows


def decode_trajectory(row) -> tuple[int, LineString]:
    traj_id = row[0]
    geom = from_wkb(row[1])
    return traj_id, geom

def encode_trajectory(cur: Connection, traj_id: int, geom: LineString):
    cur.execute("""--sql
        UPDATE ls_experiment.trajectory_ls_naive
        SET cell_id = %s
        WHERE trajectory_id = %s
    """, (encode_lonlat_to_cellid(geom.centroid.x, geom.centroid.y), traj_id))

# Tile and Cell ID conversion functions
def lonlat_to_tilexy(lon: float, lat: float, zoom: int = ZOOM) -> tuple[int, int]:
    """Convert (lon, lat) to MVT tile coordinates (x, y) at given zoom level (default = 21)."""
    tile = mercantile.tile(lon, lat, zoom)
    return tile.x, tile.y


def tilexy_to_cellid(x: int, y: int) -> int:
    """Construct cell ID as bigint: prefix(1) + x padded to 7 digits + y padded to 7 digits."""
    value = 100_000_000_000_000 + (x * 10_000_000) + y
    return value


def decode_cellid_to_tile_x_y(cell_id: int) -> tuple[int, int]:
    """Reverse from cellId to (x, y) tile coordinates."""
    raw = cell_id - 100_000_000_000_000
    x = raw // 10_000_000
    y = raw % 10_000_000
    return x, y


# Main converter
def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
    """Convert (lon, lat) → cellId."""
    x, y = lonlat_to_tilexy(lon, lat, zoom)
    return tilexy_to_cellid(x, y)
