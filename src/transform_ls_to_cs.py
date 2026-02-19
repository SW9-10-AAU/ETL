from enum import Enum
from typing import LiteralString, cast
import mercantile
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from psycopg import Connection, Cursor
from shapely import MultiLineString, Point, from_wkb, LineString, Polygon, MultiPolygon, box, unary_union

Row = tuple[int, int, int, int, bytes]  # (trajectory_id/stop_id, mmsi, ts_start, ts_end, geom_wkb)
ProcessResultTraj = tuple[int, int, int, int, list[int], list[int], list[int]]  # trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21
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

# Supercover line drawing algorithm from https://dedu.fr/projects/bresenham/ 
def supercover(x1: int, y1: int, x2: int, y2: int) -> list[tuple[int, int]]:
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

def convert_tiles_to_shapely_polygon(segment_tiles: list[tuple[int, int]], zoom: int = DEFAULT_ZOOM) -> Polygon:
    unique_tiles = set(segment_tiles)  # Remove duplicates to avoid redundant geometry creation
    return cast(Polygon, unary_union([
        box(*mercantile.bounds(x, y, zoom))
        for x, y in unique_tiles
    ]))

def find_noncontained_ls_segments(ls: LineString, cellstring_poly: Polygon) -> list[LineString]:
    # shrink the polygon slightly to avoid edge cases where the line just touches the cellstring boundary but isn't actually contained by it
    eps = 1e-9
    shrunk_cellstring_poly = cellstring_poly.buffer(-eps)
    noncontained_ls_segments = ls.difference(shrunk_cellstring_poly)
    
    # LineString is fully contained
    if noncontained_ls_segments.is_empty:
        return []

    # A single noncontained LineString segment
    if isinstance(noncontained_ls_segments, LineString):
        return [noncontained_ls_segments]

    # Multiple noncontained LineString segments
    if isinstance(noncontained_ls_segments, MultiLineString):
        return list(noncontained_ls_segments.geoms)

    return []

def buffer_point_to_poly(point: Point, buffer_distance = 1e-9) -> Polygon:
    """Buffer a point by a small amount to ensure we capture edge cases where the line just touches the tile boundary."""
    return cast(Polygon, point.buffer(buffer_distance).envelope)

def get_intersecting_tiles_for_point(lon: float, lat: float, zoom: int) -> list[tuple[int, int]]:
    """Return all tiles a point could touch, handling edges/corners."""
    minx, miny, maxx, maxy = buffer_point_to_poly(Point(lon, lat)).bounds
    return [(t.x, t.y) for t in mercantile.tiles(minx, miny, maxx, maxy, zoom)]

def get_tiles_for_endpoints(start_coord: tuple[float, float], end_coord: tuple[float, float], zoom: int) -> tuple[list[tuple[int, int]], list[tuple[int, int]]]:  
    start_tiles = [
        (x, y)
        for x, y in get_intersecting_tiles_for_point(start_coord[0], start_coord[1], zoom)
        if box(*mercantile.bounds(x, y, zoom)).intersects(buffer_point_to_poly(Point(start_coord[:2])))
    ]
    
    end_tiles = [
        (x, y)
        for x, y in get_intersecting_tiles_for_point(end_coord[0], end_coord[1], zoom)
        if box(*mercantile.bounds(x, y, zoom)).intersects(buffer_point_to_poly(Point(end_coord[:2])))
    ]
    
    return start_tiles, end_tiles

# --- Conversion Utilities ---

def convert_linestring_to_cellstrings(ls: LineString) -> tuple[list[int], list[int], list[int]]:
    cellstring_z13 = convert_linestring_to_cellstring(ls, 13)
    cellstring_z17 = convert_linestring_to_cellstring(ls, 17)
    cellstring_z21 = convert_linestring_to_cellstring(ls, 21)
    return cellstring_z13, cellstring_z17, cellstring_z21

def convert_linestring_to_cellstring(ls: LineString, zoom: int = DEFAULT_ZOOM) -> list[int]:
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
        segment_ls = LineString([c0, c1])
        segment_tiles = supercover(x0, y0, x1, y1)
        segment_tiles_poly = convert_tiles_to_shapely_polygon(segment_tiles, zoom)
        
        # Check for any segments of the LineString not contained in the polygon constructed from the tiles
        noncontained_ls_segments = find_noncontained_ls_segments(segment_ls, segment_tiles_poly)
        
        # Add cells for any non-contained LineString segments
        while noncontained_ls_segments:
            for segment in noncontained_ls_segments:
                seg_coords = list(segment.coords)
                
                for start_coord, end_coord in zip(seg_coords[:-1], seg_coords[1:]):
                    
                    # Get all intersecting tiles for both endpoints of the non-contained LineString segment
                    start_tiles, end_tiles = get_tiles_for_endpoints(start_coord, end_coord, zoom)
                    
                    # Add supercover tiles between all candidate pairs.
                    for x0_c, y0_c in start_tiles:
                        for x1_c, y1_c in end_tiles:
                            segment_tiles.extend(supercover(x0_c, y0_c, x1_c, y1_c))
            
            # Check containment with updated tiles
            segment_tiles_poly = convert_tiles_to_shapely_polygon(segment_tiles, zoom)
            noncontained_ls_segments = find_noncontained_ls_segments(segment_ls, segment_tiles_poly)
        
        # Convert segment tiles to cell IDs and append to cellstring
        for x, y in segment_tiles:
            cellstring.append(encode_tile_xy_to_cellid(x, y, zoom))
    
    deduplicated_cellstring = list(dict.fromkeys(cellstring))
    return deduplicated_cellstring

def convert_polygon_to_cellstring(poly: Polygon | Polygon, zoom: int = DEFAULT_ZOOM) -> list[int]:
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
        tile_poly: Polygon  = box(bounds.west, bounds.south, bounds.east, bounds.north)
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


def process_z13_tiles(poly: Polygon | MultiPolygon) -> tuple[list[int], list[mercantile.Tile], list[mercantile.Tile]]:
    minx, miny, maxx, maxy = poly.bounds
    z13_tiles = mercantile.tiles(minx, miny, maxx, maxy, 13)

    cellstring_z13: list[int] = []
    fully_contained_z13: list[mercantile.Tile] = []
    partially_contained_z13: list[mercantile.Tile] = []

    for tile in z13_tiles:
        classification = classify_tile_containment(poly, tile)

        match classification:
            case Classification.FULLY_CONTAINED:
                fully_contained_z13.append(tile)
            case Classification.PARTIALLY_CONTAINED:
                partially_contained_z13.append(tile)
            case Classification.NO_INTERSECTION:
                continue
            
        cellstring_z13.append(encode_tile_xy_to_cellid(tile.x, tile.y, 13))

    return cellstring_z13, fully_contained_z13, partially_contained_z13


def process_z17_tiles(
    poly: Polygon | MultiPolygon,
    fully_contained_z13: list[mercantile.Tile],
    partially_contained_z13: list[mercantile.Tile],
) -> tuple[list[int], list[mercantile.Tile], list[mercantile.Tile]]:
    cellstring_z17: list[int] = []
    fully_contained_z17: list[mercantile.Tile] = []
    partially_contained_z17: list[mercantile.Tile] = []

    for tile in fully_contained_z13:
        children_z17 = get_all_children_at_zoom(tile, 17)
        for child in children_z17:
            cellstring_z17.append(encode_tile_xy_to_cellid(child.x, child.y, 17))
            fully_contained_z17.append(child)

    for tile in partially_contained_z13:
        children_z17 = get_all_children_at_zoom(tile, 17)
        for child in children_z17:
            classification = classify_tile_containment(poly, child)

            match classification:
                case Classification.FULLY_CONTAINED:
                    fully_contained_z17.append(child)
                case Classification.PARTIALLY_CONTAINED:
                    partially_contained_z17.append(child)
                case Classification.NO_INTERSECTION:
                    continue

            cellstring_z17.append(encode_tile_xy_to_cellid(child.x, child.y, 17))
            
    return cellstring_z17, fully_contained_z17, partially_contained_z17


def process_z21_tiles(
    poly: Polygon | MultiPolygon,
    fully_contained_z17: list[mercantile.Tile],
    partially_contained_z17: list[mercantile.Tile],
) -> list[int]:
    cellstring_z21: list[int] = []

    for tile in fully_contained_z17:
        children_z21 = get_all_children_at_zoom(tile, 21)
        for child in children_z21:
            cellstring_z21.append(encode_tile_xy_to_cellid(child.x, child.y, 21))

    for tile in partially_contained_z17:
        children_z21 = get_all_children_at_zoom(tile, 21)
        for child in children_z21:
            classification = classify_tile_containment(poly, child)

            match classification:
                case Classification.FULLY_CONTAINED | Classification.PARTIALLY_CONTAINED:
                    cellstring_z21.append(encode_tile_xy_to_cellid(child.x, child.y, 21))
                case Classification.NO_INTERSECTION:
                    continue

    return cellstring_z21


def convert_polygon_to_cellstring_hierarchical(poly: Polygon | MultiPolygon) -> tuple[list[int], list[int], list[int]]:
    """
    Convert polygon to cellstrings at Z13, Z17, and Z21.

    Args:
        poly: A Shapely Polygon or MultiPolygon to convert

    Returns:
        Tuple of (cellstring_z13, cellstring_z17, cellstring_z21)
    """
    if poly.is_empty:
        return ([], [], [])

    cellstring_z13, fully_contained_z13, partially_contained_z13 = process_z13_tiles(poly)
    cellstring_z17, fully_contained_z17, partially_contained_z17 = process_z17_tiles(poly, fully_contained_z13, partially_contained_z13)
    cellstring_z21 = process_z21_tiles(poly, fully_contained_z17, partially_contained_z17)

    return cellstring_z13, cellstring_z17, cellstring_z21


# --- Worker Functions ---

def process_trajectory_row(row: Row) -> ProcessResultTraj:
    trajectory_id, mmsi, ts_start, ts_end, geom_wkb = row
    linestring = cast(LineString, from_wkb(geom_wkb))
    cellstring_z13, cellstring_z17, cellstring_z21= convert_linestring_to_cellstrings(linestring)
  
    return (trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)

def process_stop_row(row: Row) -> ProcessResultStop:
    stop_id, mmsi, ts_start, ts_end, geom_wkb = row
    polygon = cast(Polygon, from_wkb(geom_wkb))

    cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstring_hierarchical(polygon)
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

def transform_ls_trajectories_to_cs(connection: Connection, db_schema: str, max_workers: int = MAX_WORKERS,
                                    batch_size: int = BATCH_SIZE):
    print(f"--- Processing trajectories (using {max_workers} workers) ---")
    total_processed = 0
    insert_traj_query = f"""
                INSERT INTO {db_schema}.trajectory_cs (trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                
    with connection.cursor() as cur:
        get_trajs_query = f"""
                SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM {db_schema}.trajectory_ls
                ORDER BY trajectory_id;
                """

        for batch in get_batches(cur, get_trajs_query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: list[FutureResultTraj] = [executor.submit(process_trajectory_row, row) for row in batch]
                results: list[ProcessResultTraj] = []
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_traj_query,
                                       [(trajectory_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) for
                                        (trajectory_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) in
                                        results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} trajectories")

    print(f"Finished processing all trajectories ({total_processed:,} total)")

def transform_poly_stops_to_cs(connection: Connection, db_schema: str, max_workers: int = MAX_WORKERS, batch_size: int = BATCH_SIZE):
    print(f"--- Processing stops (using {max_workers} workers) ---")
    total_processed = 0
    insert_stop_query = f"""
                   INSERT INTO {db_schema}.stop_cs (stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)
                   """

    with connection.cursor() as cur:
        get_stops_query = f"""
                SELECT stop_id, mmsi, ts_start, ts_end, ST_AsBinary(geom)
                FROM {db_schema}.stop_poly
                ORDER BY stop_id;
                """

        for batch in get_batches(cur, get_stops_query, batch_size):
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: list[FutureResultStop] = [executor.submit(process_stop_row, row) for row in batch]
                results: list[ProcessResultStop] = []
                for future in as_completed(futures):
                    try:
                        results.append(future.result())
                    except Exception as e:
                        print(f"Worker error: {e}")

            with connection.cursor() as insert_cur:
                insert_cur.executemany(insert_stop_query, [(stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) for
                                                      (stop_id, mmsi, start_time, end_time, cellstring_z13, cellstring_z17, cellstring_z21) in results])
            connection.commit()

            total_processed += len(results)
            print(f"Processed total: {total_processed:,} stops")

    print(f"Finished processing all stops ({total_processed:,} total)")