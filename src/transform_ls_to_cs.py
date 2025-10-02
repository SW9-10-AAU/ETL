import mercantile
from shapely import from_wkb, LineString, Polygon, Point

ZOOM = 21

def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
    x, y = mercantile.tile(lon, lat, zoom).x, mercantile.tile(lon, lat, zoom).y
    return 100_000_000_000_000 + (x * 10_000_000) + y

def bresenham(x0, y0, x1, y1):
    dx = abs(x1 - x0)
    dy = abs(y1 - y0)
    x, y = x0, y0
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    if dx > dy:
        err = dx / 2.0
        while x != x1:
            yield x, y
            err -= dy
            if err < 0:
                y += sy
                err += dx
            x += sx
        yield x, y
    else:
        err = dy / 2.0
        while y != y1:
            yield x, y
            err -= dx
            if err < 0:
                x += sx
                err += dy
            y += sy
        yield x, y

def transform_ls_trajectories_to_cs(connection):
    cur = connection.cursor()
    cur.execute(
        "SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom) FROM ls_experiment.trajectory_ls ORDER BY trajectory_id;")
    rows = cur.fetchall()
    for row in rows:
        _, mmsi, ts_start, ts_end, geom_wkb = row
        linestring: LineString = from_wkb(geom_wkb)
        coords = list(linestring.coords)
        cellids = []
        for i in range(len(coords) - 1):
            lon0, lat0 = coords[i][:2]
            lon1, lat1 = coords[i + 1][:2]
            x0, y0 = mercantile.tile(lon0, lat0, ZOOM).x, mercantile.tile(lon0, lat0, ZOOM).y
            x1, y1 = mercantile.tile(lon1, lat1, ZOOM).x, mercantile.tile(lon1, lat1, ZOOM).y
            for x, y in bresenham(x0, y0, x1, y1):
                cellid = 100_000_000_000_000 + (x * 10_000_000) + y
                cellids.append(cellid)
        cur.execute("""
            INSERT INTO ls_experiment.trajectory_cs (mmsi, ts_start, ts_end, trajectory)
            VALUES (%s, %s, %s, %s)
        """, (mmsi, ts_start, ts_end, cellids))
    connection.commit()
    cur.close()

def transform_ls_stops_to_cs(connection):
    cur = connection.cursor()
    cur.execute(
        "SELECT stop_id, mmsi, ts_start, ts_end, ST_AsBinary(geom) FROM ls_experiment.stop_poly ORDER BY stop_id;")
    rows = cur.fetchall()
    for row in rows:
        _, mmsi, ts_start, ts_end, geom_wkb = row
        polygon: Polygon = from_wkb(geom_wkb)
        minx, miny, maxx, maxy = polygon.bounds
        cellids = []
        # Loop over all tiles in bounding box
        for tile in mercantile.tiles(minx, miny, maxx, maxy, ZOOM):
            tile_BBox = mercantile.bounds(tile)
            lon, lat = ((tile_BBox.west + tile_BBox.east) / 2), ((tile_BBox.north + tile_BBox.south) / 2)
            if polygon.contains(Point(lon, lat)):
                cellid = 100_000_000_000_000 + (tile.x * 10_000_000) + tile.y
                cellids.append(cellid)
        cur.execute("""
            INSERT INTO ls_experiment.stop_cs (mmsi, ts_start, ts_end, trajectory)
            VALUES (%s, %s, %s, %s)
        """, (mmsi, ts_start, ts_end, cellids))
    connection.commit()
    cur.close()

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