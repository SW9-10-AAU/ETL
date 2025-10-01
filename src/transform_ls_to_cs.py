import mercantile
from shapely import from_wkb, LineString

ZOOM = 21

def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
    x, y = mercantile.tile(lon, lat, zoom).x, mercantile.tile(lon, lat, zoom).y
    return 100_000_000_000_000 + (x * 10_000_000) + y

def main():
    from dotenv import load_dotenv
    from connect import connect_to_db

    load_dotenv()
    connection = connect_to_db()
    cur = connection.cursor()

    cur.execute("SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom) FROM ls_experiment.trajectory_ls_naive;")
    rows = cur.fetchall()

    for row in rows:
        traj_id, mmsi, ts_start, ts_end, geom_wkb = row
        linestring: LineString = from_wkb(geom_wkb)
        cellids = [encode_lonlat_to_cellid(lon, lat) for lon, lat, *_ in linestring.coords]
        # Store as array (Postgres int[]), or as text (comma-separated)
        cur.execute("""
            INSERT INTO ls_experiment.trajectory_cs (mmsi, ts_start, ts_end, trajectory)
            VALUES (%s, %s, %s, %s)
        """, (mmsi, ts_start, ts_end, cellids))
    connection.commit()
    cur.close()
    connection.close()

if __name__ == "__main__":
    main()


# def fetch_trajectories(conn: Connection):
#     cur = conn.cursor()
#
#     cur.execute("SELECT trajectory_id, ST_AsBinary(geom) FROM ls_experiment.trajectory_ls_naive;")
#     rows = cur.fetchall()
#     cur.close()
#     return rows
#
#
# def decode_trajectory(row) -> dict[int, int, float, float, LineString]:
#     traj_id: int = row[0]
#     mmsi: int = row[1]
#     ts_start: float = row[2]
#     ts_end: float = row[3]
#     trajectory: LineString = from_wkb(row[4])
#     return traj_id, mmsi, ts_start, ts_end, trajectory
#
#
# def encode_trajectory(cur: Connection, traj_id: int, trajectory: LineString):
#     cur.execute("""--sql
#         INSERT INTO ls_experiment.trajectory_cs(mmsi, ts_start, ts_end, trajectory)
#         VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), %s)
#     """, (encode_lonlat_to_cellid(trajectory.x, trajectory.y), traj_id))
#
#
# # Tile and Cell ID conversion functions
# def lonlat_to_tilexy(lon: float, lat: float, zoom: int = ZOOM) -> tuple[int, int]:
#     """Convert (lon, lat) to MVT tile coordinates (x, y) at given zoom level (default = 21)."""
#     tile = mercantile.tile(lon, lat, zoom)
#     return tile.x, tile.y
#
#
# def tilexy_to_cellid(x: int, y: int) -> int:
#     """Construct cell ID as bigint: prefix(1) + x padded to 7 digits + y padded to 7 digits."""
#     value = 100_000_000_000_000 + (x * 10_000_000) + y
#     return value
#
#
# def decode_cellid_to_tile_x_y(cell_id: int) -> tuple[int, int]:
#     """Reverse from cellId to (x, y) tile coordinates."""
#     raw = cell_id - 100_000_000_000_000
#     x = raw // 10_000_000
#     y = raw % 10_000_000
#     return x, y
#
#
# # Main converter
# def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
#     """Convert (lon, lat) → cellId."""
#     x, y = lonlat_to_tilexy(lon, lat, zoom)
#     return tilexy_to_cellid(x, y)

if __name__ == "__main__":
    main()