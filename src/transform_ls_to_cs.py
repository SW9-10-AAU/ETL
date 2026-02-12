from enum import Enum
from typing import LiteralString, cast
import mercantile
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from psycopg import Connection, Cursor
from shapely import from_wkb, LineString, Polygon, MultiPolygon, box

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

# --- Bresenham ---

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

# --- Conversion Utilities ---

def convert_linestring_to_cellstring(ls: LineString, zoom: int = DEFAULT_ZOOM, use_supercover: bool = False) -> list[int]:
    if ls.is_empty:
        return []
    coords = ls.coords
    cellstring: list[int] = []
    for c0, c1 in zip(coords[:-1], coords[1:]):
        lon0, lat0 = c0[:2]
        lon1, lat1 = c1[:2]
        x0, y0 = get_tile_xy(lon0, lat0, zoom)
        x1, y1 = get_tile_xy(lon1, lat1, zoom)
        
        cellstring_tiles = bresenham(x0, y0, x1, y1) if not use_supercover else supercover_bresenham(x0, y0, x1, y1)
        for x, y in cellstring_tiles:
            cellstring.append(encode_tile_xy_to_cellid(x, y, zoom))
    return cellstring


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

class Classification(Enum):
    FULLY_CONTAINED = 1
    PARTIALLY_CONTAINED = 2
    NO_INTERSECTION = 3

def classify_tile_containment(poly: Polygon | MultiPolygon, tile: mercantile.Tile) -> Classification:
    """
    Classify a tile's relationship to the polygon.

    Args:
        poly: A Shapely Polygon or MultiPolygon
        tile: A mercantile Tile

    Returns:
        Classification.FULLY_CONTAINED if the polygon completely contains the tile
        Classification.PARTIALLY_CONTAINED if the tile intersects but is not fully contained
        Classification.NO_INTERSECTION if there is no overlap
    """
    bounds = mercantile.bounds(tile)
    tile_poly = box(bounds.west, bounds.south, bounds.east, bounds.north)

    if poly.contains(tile_poly):
        return Classification.FULLY_CONTAINED
    elif poly.intersects(tile_poly):
        return Classification.PARTIALLY_CONTAINED
    else:
        return Classification.NO_INTERSECTION

def get_all_children_at_zoom(tile: mercantile.Tile, target_zoom: int) -> list[mercantile.Tile]:
    """
    Get all descendant tiles at target_zoom from the given tile.

    Args:
        tile: Parent tile
        target_zoom: Target zoom level (must be > tile.z)

    Returns:
        List of all descendant tiles at target_zoom
    """
    if tile.z >= target_zoom:
        return [tile]

    # Get direct children
    children = list(mercantile.children(tile))

    # If children are at target zoom, return them
    if children[0].z == target_zoom:
        return children

    # Otherwise, recursively get children of children
    all_descendants = []
    for child in children:
        all_descendants.extend(get_all_children_at_zoom(child, target_zoom))

    return all_descendants


def convert_polygon_to_cellstring_hierarchical(poly: Polygon | MultiPolygon) -> tuple[list[int], list[int], list[int]]:
    """
    Convert polygon to cellstrings at Z13, Z17, and Z21 using hierarchical subdivision.

    This algorithm optimizes polygon-to-cellstring conversion by leveraging the tile hierarchy:
    - Tiles fully contained at a coarse zoom level have all their children included at finer levels
    - Only tiles partially intersecting the polygon boundary require testing at finer levels

    Args:
        poly: A Shapely Polygon or MultiPolygon to convert

    Returns:
        Tuple of (cellstring_z13, cellstring_z17, cellstring_z21)
    """
    if poly.is_empty:
        return ([], [], [])

    # --- Phase 1: Z13 Processing (Base Level) ---
    minx, miny, maxx, maxy = poly.bounds
    z13_tiles = mercantile.tiles(minx, miny, maxx, maxy, 13)

    cellstring_z13: list[int] = []
    fully_contained_z13: list[mercantile.Tile] = []
    partially_contained_z13: list[mercantile.Tile] = []

    for tile in z13_tiles:
        classification = classify_tile_containment(poly, tile)

        if classification == Classification.FULLY_CONTAINED:
            cellstring_z13.append(encode_tile_xy_to_cellid(tile.x, tile.y, 13))
            fully_contained_z13.append(tile)
        elif classification == Classification.PARTIALLY_CONTAINED:
            cellstring_z13.append(encode_tile_xy_to_cellid(tile.x, tile.y, 13))
            partially_contained_z13.append(tile)
        # no_intersection tiles are skipped

    print(f"Z13: {len(fully_contained_z13)} fully contained tiles, {len(partially_contained_z13)} partially contained tiles, total {len(cellstring_z13)} tiles")
    
    # --- Phase 2: Z17 Processing (Hierarchical Subdivision) ---
    cellstring_z17: list[int] = []
    fully_contained_z17: list[mercantile.Tile] = []
    partially_contained_z17: list[mercantile.Tile] = []

    # Process fully-contained Z13 tiles
    for tile in fully_contained_z13:
        children_z17 = get_all_children_at_zoom(tile, 17)
        for child in children_z17:
            cellstring_z17.append(encode_tile_xy_to_cellid(child.x, child.y, 17))
            fully_contained_z17.append(child)

    # Process partially-contained Z13 tiles
    for tile in partially_contained_z13:
        children_z17 = get_all_children_at_zoom(tile, 17)
        for child in children_z17:
            classification = classify_tile_containment(poly, child)

            if classification == Classification.FULLY_CONTAINED:
                cellstring_z17.append(encode_tile_xy_to_cellid(child.x, child.y, 17))
                fully_contained_z17.append(child)
            elif classification == Classification.PARTIALLY_CONTAINED:
                cellstring_z17.append(encode_tile_xy_to_cellid(child.x, child.y, 17))
                partially_contained_z17.append(child)

    print(f"Z17: {len(fully_contained_z17)} fully contained tiles, {len(partially_contained_z17)} partially contained tiles, total {len(cellstring_z17)} tiles")

    # --- Phase 3: Z21 Processing (Final Subdivision) ---
    cellstring_z21: list[int] = []

    # Process fully-contained Z17 tiles
    for tile in fully_contained_z17:
        children_z21 = get_all_children_at_zoom(tile, 21)
        for child in children_z21:
            cellstring_z21.append(encode_tile_xy_to_cellid(child.x, child.y, 21))

    # Process partially-contained Z17 tiles
    for tile in partially_contained_z17:
        children_z21 = get_all_children_at_zoom(tile, 21)
        for child in children_z21:
            classification = classify_tile_containment(poly, child)

            if classification in (Classification.FULLY_CONTAINED, Classification.PARTIALLY_CONTAINED):
                cellstring_z21.append(encode_tile_xy_to_cellid(child.x, child.y, 21))

    print(f"Z21: Total {len(cellstring_z21)} tiles")

    return (cellstring_z13, cellstring_z17, cellstring_z21)


# --- Worker Functions ---

def process_trajectory_row(row: Row, use_supercover: bool) -> ProcessResultTraj:
    trajectory_id, mmsi, ts_start, ts_end, geom_wkb = row
    linestring = cast(LineString, from_wkb(geom_wkb))
    raw_cellstring_z13 = convert_linestring_to_cellstring(linestring, 13, use_supercover)
    raw_cellstring_z17 = convert_linestring_to_cellstring(linestring, 17, use_supercover)
    raw_cellstring_z21 = convert_linestring_to_cellstring(linestring, 21, use_supercover)
    cellstring_z13 = list(dict.fromkeys(raw_cellstring_z13)) # Deduplicate
    cellstring_z17 = list(dict.fromkeys(raw_cellstring_z17)) # Deduplicate
    cellstring_z21 = list(dict.fromkeys(raw_cellstring_z21)) # Deduplicate
    is_unique : bool = True
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
    table_name = "trajectory_supercover_cs" if use_supercover else "trajectory_cs"
    insert_query = f"""
                INSERT INTO prototype2.{table_name} (trajectory_id, mmsi, ts_start, ts_end, unique_cells, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """
                
    with connection.cursor() as cur:
        query = """
                SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM prototype2.trajectory_ls
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
