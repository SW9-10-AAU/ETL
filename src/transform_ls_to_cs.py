import mercantile
from concurrent.futures import ProcessPoolExecutor, as_completed
from psycopg import Connection
from shapely import from_wkb, LineString, Polygon, Point

# --- Constants ---

ZOOM = 21
ENCODE_OFFSET = 100_000_000_000_000
ENCODE_MULT = 10_000_000
BATCH_SIZE = 5_000


# --- Encoding Utilities ---

def encode_tile_xy_to_cellid(x: int, y: int) -> int:
    return ENCODE_OFFSET + (x * ENCODE_MULT) + y


def get_tile_xy(lon: float, lat: float, zoom: int = ZOOM) -> tuple[int, int]:
    t = mercantile.tile(lon, lat, zoom)
    return t.x, t.y


def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
    x, y = get_tile_xy(lon, lat, zoom)
    return encode_tile_xy_to_cellid(x, y)


# --- Bresenham ---

def bresenham(x0: int, y0: int, x1: int, y1: int) -> list[tuple[int, int]]:
    tiles = []
    dx, dy = abs(x1 - x0), abs(y1 - y0)
    sx, sy = (1, -1)[x0 > x1], (1, -1)[y0 > y1]
    err = dx - dy

    while True:
        tiles.append((x0, y0))
        if x0 == x1 and y0 == y1:
            break
        e2 = 2 * err
        if e2 > -dy:
            err -= dy
            x0 += sx
        if e2 < dx:
            err += dx
            y0 += sy
    return tiles


# --- Conversion Utilities ---

def convert_linestring_to_cellstring(ls: LineString) -> list[int]:
    if ls.is_empty:
        return []
    coords = ls.coords
    cellstring = []
    append = cellstring.append
    for c0, c1 in zip(coords[:-1], coords[1:]):
        lon0, lat0 = c0[:2]
        lon1, lat1 = c1[:2]
        x0, y0 = get_tile_xy(lon0, lat0)
        x1, y1 = get_tile_xy(lon1, lat1)
        for x, y in bresenham(x0, y0, x1, y1):
            append(encode_tile_xy_to_cellid(x, y))
    return cellstring


def convert_polygon_to_cellstring(poly: Polygon) -> list[int]:
    if poly.is_empty:
        return []
    minx, miny, maxx, maxy = poly.bounds
    tiles = mercantile.tiles(minx, miny, maxx, maxy, ZOOM)
    cellstring = []
    append = cellstring.append
    contains = poly.contains

    for tile in tiles:
        bounds = mercantile.bounds(tile)
        center = Point((bounds.west + bounds.east) / 2, (bounds.north + bounds.south) / 2)
        if contains(center):
            append(encode_tile_xy_to_cellid(tile.x, tile.y))
    return cellstring


# --- Worker Functions ---

def process_trajectory_row(row: tuple):
    trajectory_id, mmsi, ts_start, ts_end, geom_wkb = row
    linestring: LineString = from_wkb(geom_wkb)
    cellstring = convert_linestring_to_cellstring(linestring)
    is_unique = len(cellstring) == len(set(cellstring))
    return trajectory_id, mmsi, ts_start, ts_end, is_unique, cellstring


def process_stop_row(row: tuple):
    stop_id, mmsi, ts_start, ts_end, geom_wkb = row
    polygon: Polygon = from_wkb(geom_wkb)

    if polygon.is_empty:
        print(f"⚠️ Skipping empty stop {stop_id}")
        return None

    cellstring = convert_polygon_to_cellstring(polygon)
    return stop_id, mmsi, ts_start, ts_end, cellstring


# --- Streaming Batch Helpers ---

def stream_query(cur, query: str, batch_size: int):
    """Generator that yields rows in batches."""
    cur.execute(query)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows


# --- Main Transformation Functions ---

def transform_ls_trajectories_to_cs(connection: Connection, max_workers: int = 4, batch_size: int = BATCH_SIZE):
    total_processed = 0
    insert_query = """
                   INSERT INTO prototype1.trajectory_cs (trajectory_id, mmsi, ts_start, ts_end, unique_cells, cellstring)
                   VALUES (%s, %s, %s, %s, %s, %s) \
                   """

    with connection.cursor() as cur:
        query = """
                SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM prototype1.trajectory_ls
                ORDER BY trajectory_id; \
                """

        for batch in stream_query(cur, query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_trajectory_row, row) for row in batch]
                results = []
                for f in as_completed(futures):
                    try:
                        results.append(f.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_query, [(tid, m, s, e, u, c) for (tid, m, s, e, u, c) in results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} trajectories")

    print(f"Finished processing all trajectories ({total_processed:,} total)")


def transform_ls_stops_to_cs(connection: Connection, max_workers: int = 4, batch_size: int = BATCH_SIZE):
    total_processed = 0
    insert_query = """
                   INSERT INTO prototype1.stop_cs (stop_id, mmsi, ts_start, ts_end, cellstring)
                   VALUES (%s, %s, %s, %s, %s) \
                   """

    with connection.cursor() as cur:
        query = """
                SELECT stop_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM prototype1.stop_poly
                ORDER BY stop_id; \
                """

        for batch in stream_query(cur, query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = [executor.submit(process_stop_row, row) for row in batch]
                results = []
                for f in as_completed(futures):
                    try:
                        result = f.result()
                        if result is not None:
                            results.append(result)
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_query, [(sid, m, s, e, c) for (sid, m, s, e, c) in results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} stops")

    print(f"Finished processing all stops ({total_processed:,} total)")
