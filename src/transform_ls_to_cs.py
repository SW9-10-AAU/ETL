from typing import LiteralString, cast
import mercantile
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from psycopg import Connection, Cursor
from shapely import MultiLineString, Point, from_wkb, LineString, Polygon, MultiPolygon, box, unary_union

Row = tuple[int, int, int, int, bytes]  # (trajectory_id/stop_id, mmsi, ts_start, ts_end, geom_wkb)
ProcessResultTraj = tuple[int, int, int, int, bool, list[int], list[int], list[int]]  # trajectory_id, mmsi, ts_start, ts_end, is_unique, cellstring_z13, cellstring_z17, cellstring_z21
ProcessResultStop = tuple[int, int, int, int, list[int], list[int], list[int]]  # stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21
FutureResultTraj = Future[ProcessResultTraj]
FutureResultStop = Future[ProcessResultStop]

# --- Constants ---

DEFAULT_ZOOM = 21 # Default zoom level
ENCODE_OFFSET_Z21 = 100_000_000_000_000
ENCODE_OFFSET_Z17 = 1_000_000_000_000
ENCODE_OFFSET_Z13 = 100_000_000
ENCODE_MULT_Z21 = 10_000_000
ENCODE_MULT_Z17 = 1_000_000
ENCODE_MULT_Z13 = 10_000
BATCH_SIZE = 5_000
MAX_WORKERS = 4

# --- Encoding Utilities ---

def encode_tile_xy_to_cellid(x: int, y: int, zoom: int = DEFAULT_ZOOM) -> int:
    if (zoom == 13):
        return ENCODE_OFFSET_Z13 + (x * ENCODE_MULT_Z13) + y

    if (zoom == 17):
        return ENCODE_OFFSET_Z17 + (x * ENCODE_MULT_Z17) + y

    return ENCODE_OFFSET_Z21 + (x * ENCODE_MULT_Z21) + y

def get_tile_xy(lon: float, lat: float, zoom: int = DEFAULT_ZOOM) -> tuple[int, int]:
    tile = mercantile.tile(lon, lat, zoom)
    return tile.x, tile.y

def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = DEFAULT_ZOOM) -> int:
    x, y = get_tile_xy(lon, lat, zoom)
    return encode_tile_xy_to_cellid(x, y)

# --- Bresenham --- TODO: remove bresenham

def bresenham(x0: int, y0: int, x1: int, y1: int) -> list[tuple[int, int]]:
    tiles: list[tuple[int, int]] = []
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

# ---- supercover bresenham ----   from https://dedu.fr/projects/bresenham/ 

def supercover_bresenham(x1: int, y1: int, x2: int, y2: int) -> list[tuple[int, int]]:
    cells: list[tuple[int, int]] = []
    dx, dy = x2 - x1, y2 - y1
    x, y = x1, y1

    xstep = 1 if dx >= 0 else -1
    ystep = 1 if dy >= 0 else -1
    dx, dy = abs(dx), abs(dy)
    ddx, ddy = 2 * dx, 2 * dy

    cells.append((x, y))

    if ddx >= ddy:
        errorprev = error = dx
        for _ in range(dx):
            x += xstep
            error += ddy
            if error > ddx:
                y += ystep
                error -= ddx
                # check for extra cells
                if error + errorprev < ddx:
                    cells.append((x, y - ystep))
                elif error + errorprev > ddx:
                    cells.append((x - xstep, y))
                else:
                    cells.append((x, y - ystep))
                    cells.append((x - xstep, y))
            cells.append((x, y))
            errorprev = error
    else:
        errorprev = error = dy
        for _ in range(dy):
            y += ystep
            error += ddx
            if error > ddy:
                x += xstep
                error -= ddy
                if error + errorprev < ddy:
                    cells.append((x - xstep, y))
                elif error + errorprev > ddy:
                    cells.append((x, y - ystep))
                else:
                    cells.append((x - xstep, y))
                    cells.append((x, y - ystep))
            cells.append((x, y))
            errorprev = error

    return cells

def create_cellstring_multipoly(cell_tiles: list[tuple[int, int]], zoom: int = DEFAULT_ZOOM) -> MultiPolygon:
    unique_tiles = set(cell_tiles)  # Remove duplicates to avoid redundant geometry creation
    return unary_union([
        box(*mercantile.bounds(x, y, zoom))
        for x, y in unique_tiles
    ])

def find_noncontained_ls_segments(ls: LineString,tile_multipolygon: MultiPolygon) -> list[LineString]:
    # shrink the multipolygon slightly to avoid edge cases where the line just touches the tile boundary but isn't actually covered by it
    eps = 1e-9
    shrunk_poly = tile_multipolygon.buffer(-eps)
    noncovered = ls.difference(shrunk_poly)
    if noncovered.is_empty:
        return []

    if isinstance(noncovered, LineString):
        return [noncovered]

    if isinstance(noncovered, MultiLineString):
        return list(noncovered.geoms)

    return []

def buffer_point_for_tile_edge_cases(point: Point) -> Polygon:
    """Buffer a point by a small amount to ensure we capture edge cases where the line just touches the tile boundary."""
    point_buf = 1e-9
    return point.buffer(point_buf).envelope

def point_to_all_candidate_tiles(lon: float, lat: float, zoom: int) -> list[tuple[int, int]]:
    """Return all tiles a point could touch, handling edges/corners."""
    minx, miny, maxx, maxy = buffer_point_for_tile_edge_cases(Point(lon, lat)).bounds
    return [(t.x, t.y) for t in mercantile.tiles(minx, miny, maxx, maxy, zoom)]

# --- Conversion Utilities ---

def convert_linestring_to_cellstring(ls: LineString, zoom: int = DEFAULT_ZOOM, use_supercover: bool = False) -> list[int]:
    if ls.is_empty:
        return []
    
    coords = list(ls.coords)
    cellstring: list[int] = []
    
    # Process each segment independently to preserve temporal order
    for i in range(len(coords) - 1):
        c0, c1 = coords[i], coords[i + 1]
        lon0, lat0 = c0[:2]
        lon1, lat1 = c1[:2]
        x0, y0 = get_tile_xy(lon0, lat0, zoom)
        x1, y1 = get_tile_xy(lon1, lat1, zoom)
        
        # Get initial tiles for this segment
        tiles = bresenham(x0, y0, x1, y1) if not use_supercover else supercover_bresenham(x0, y0, x1, y1)
        
        # Create this segment's LineString and check coverage
        segment_ls = LineString([c0, c1])
        tile_multipolygon = create_cellstring_multipoly(tiles, zoom)
        non_covere_ls_segments = find_noncontained_ls_segments(segment_ls, tile_multipolygon)
        
        # Iterative gap-filling for THIS segment only
        max_iterations = 10  # Safety limit
        iteration = 0
        while non_covere_ls_segments and iteration < max_iterations:
            iteration += 1
            if iteration == 10:
                raise Exception(f"Exceeded max iterations for segment {i} - possible infinite loop in coverage filling")
            
            for segment in non_covere_ls_segments:
                seg_coords = list(segment.coords)
                
                for sc0, sc1 in zip(seg_coords[:-1], seg_coords[1:]):
                    
                    # Get all candidate tiles for both endpoints
                    tiles_c0 = [
                        (x, y)
                        for x, y in point_to_all_candidate_tiles(sc0[0], sc0[1], zoom)
                        if box(*mercantile.bounds(x, y, zoom)).intersects(buffer_point_for_tile_edge_cases(Point(sc0[:2])))
                    ]
                    
                    tiles_c1 = [
                        (x, y)
                        for x, y in point_to_all_candidate_tiles(sc1[0], sc1[1], zoom)
                        if box(*mercantile.bounds(x, y, zoom)).intersects(buffer_point_for_tile_edge_cases(Point(sc1[:2])))
                    ]
                    
                    # Add supercover tiles between all candidate pairs.
                    for x0_c, y0_c in tiles_c0:
                        for x1_c, y1_c in tiles_c1:
                            tiles.extend(supercover_bresenham(x0_c, y0_c, x1_c, y1_c))
            
            # Re-check coverage with updated tiles
            tile_multipolygon = create_cellstring_multipoly(tiles, zoom)
            non_covere_ls_segments = find_noncontained_ls_segments(segment_ls, tile_multipolygon)
        
        # Convert segment tiles to cell IDs and append to cellstring
        for x, y in tiles:
            cellstring.append(encode_tile_xy_to_cellid(x, y, zoom))
    
    deduplicated_cellstring = list(dict.fromkeys(cellstring))
    return deduplicated_cellstring

def convert_polygon_to_cellstring(poly: Polygon | MultiPolygon, zoom: int = DEFAULT_ZOOM) -> list[int]:
    """
    Converts a Polygon or MultiPolygon to a cellstring (list of tile IDs).

    Uses tile-based intersection testing: generates all tiles covering the bounding box,
    then filters to only tiles that actually intersect the geometry.

    Args:
        poly: A Shapely Polygon or MultiPolygon to convert
        zoom: Zoom level for tiles (default: 21)

    Returns:
        List of integer cell IDs representing tiles that intersect the geometry
    """
    if poly.is_empty:
        return []
    minx, miny, maxx, maxy = poly.bounds
    tiles = mercantile.tiles(minx, miny, maxx, maxy, zoom)
    cellstring: list[int] = []

    for tile in tiles:
        bounds = mercantile.bounds(tile)
        tile_poly  = box(bounds.west, bounds.south, bounds.east, bounds.north)
        if poly.intersects(tile_poly):
            cellstring.append(encode_tile_xy_to_cellid(tile.x, tile.y, zoom))
    return cellstring

# --- Worker Functions ---

def process_trajectory_row(row: Row, use_supercover: bool) -> ProcessResultTraj:
    trajectory_id, mmsi, ts_start, ts_end, geom_wkb = row
    linestring = cast(LineString, from_wkb(geom_wkb))
    cellstring_z13 = convert_linestring_to_cellstring(linestring, 13, use_supercover)
    cellstring_z17 = convert_linestring_to_cellstring(linestring, 17, use_supercover)
    cellstring_z21 = convert_linestring_to_cellstring(linestring, 21, use_supercover)
    is_unique : bool = True #TODO: remove unique
    return (trajectory_id, mmsi, ts_start, ts_end, is_unique, cellstring_z13, cellstring_z17, cellstring_z21)

def process_stop_row(row: Row) -> ProcessResultStop:
    stop_id, mmsi, ts_start, ts_end, geom_wkb = row
    polygon = cast(Polygon, from_wkb(geom_wkb))

    cellstring_z13 = convert_polygon_to_cellstring(polygon, 13)
    cellstring_z17 = convert_polygon_to_cellstring(polygon, 17)
    cellstring_z21 = convert_polygon_to_cellstring(polygon, 21)
    return stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21

# --- Batch Helper ---

def get_batches(cur: Cursor, query: LiteralString, batch_size: int):
    """Generator that yields rows in batches."""
    print(f"Fetching rows...")
    cur.execute(query)
    print(f"Fetched rows, processing in batches of {batch_size}...")
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        yield rows

# --- Main Transformation Functions ---

def transform_ls_trajectories_to_cs(connection: Connection, max_workers: int = MAX_WORKERS,
                                    batch_size: int = BATCH_SIZE, use_supercover: bool = False):
    print(f"--- Processing trajectories with {'Supercover' if use_supercover else 'Bresenham'} (using {max_workers} workers) ---")
    total_processed = 0
    table_name = "trajectory_contained_supercover_cs" if use_supercover else "trajectory_cs"
    insert_query = f"""
                INSERT INTO prototype2.{table_name} (trajectory_id, mmsi, ts_start, ts_end, unique_cells, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
    with connection.cursor() as cur:
        query = """
                SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM prototype2.trajectory_ls
                WHERE trajectory_id
                ORDER BY trajectory_id;
                """

        for batch in get_batches(cur, query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: list[FutureResultTraj] = [executor.submit(process_trajectory_row, row, use_supercover) for row in batch]
                results: list[ProcessResultTraj] = []
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_query,
                                       [(trajectory_id, mmsi, start_time, end_time, is_unique, cellstring_z13, cellstring_z17, cellstring_z21) for
                                        (trajectory_id, mmsi, start_time, end_time, is_unique, cellstring_z13, cellstring_z17, cellstring_z21) in
                                        results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} trajectories")

    print(f"Finished processing all trajectories ({total_processed:,} total)")

def transform_ls_stops_to_cs(connection: Connection, max_workers: int = MAX_WORKERS, batch_size: int = BATCH_SIZE):
    print(f"--- Processing stops (using {max_workers} workers) ---")
    total_processed = 0
    insert_query = """
                   INSERT INTO prototype2.stop_cs (stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """

    with connection.cursor() as cur:
        query = """
                SELECT stop_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM prototype2.stop_poly
                ORDER BY stop_id;
                """

        for batch in get_batches(cur, query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: list[FutureResultStop] = [executor.submit(process_stop_row, row) for row in batch]
                results: list[ProcessResultStop] = []
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_query, [(stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) for
                                                      (stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) in results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} stops")

    print(f"Finished processing all stops ({total_processed:,} total)")