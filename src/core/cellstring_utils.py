from enum import Enum
from typing import cast

import mercantile
from shapely import MultiLineString, Point, LineString, Polygon, MultiPolygon, box, unary_union
from ukc_core.quadkey_utils import zxy_to_quadkey

class Classification(Enum):
    """Enum for classifying tile containment in a Polygon or MultiPolygon."""
    FULLY_CONTAINED = 1
    PARTIALLY_CONTAINED = 2
    NO_INTERSECTION = 3

# --- Constants ---
DEFAULT_ZOOM = 21 # Default zoom level
ENCODE_OFFSET_Z21 = 100_000_000_000_000
ENCODE_OFFSET_Z17 = 1_000_000_000_000
ENCODE_OFFSET_Z13 = 100_000_000
ENCODE_MULT_Z21 = 10_000_000
ENCODE_MULT_Z17 = 1_000_000
ENCODE_MULT_Z13 = 10_000


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
    return encode_tile_xy_to_cellid(x, y, zoom)

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

def buffer_point_to_poly(point: Point, buffer_distance = 1e-5) -> Polygon:
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
            
        cellstring_z13.append(zxy_to_quadkey(13, tile.x, tile.y))

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
            cellstring_z17.append(zxy_to_quadkey(17, child.x, child.y))
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

            cellstring_z17.append(zxy_to_quadkey(17, child.x, child.y))
            
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
            cellstring_z21.append(zxy_to_quadkey(21, child.x, child.y))

    for tile in partially_contained_z17:
        children_z21 = get_all_children_at_zoom(tile, 21)
        for child in children_z21:
            classification = classify_tile_containment(poly, child)

            match classification:
                case Classification.FULLY_CONTAINED | Classification.PARTIALLY_CONTAINED:
                    cellstring_z21.append(zxy_to_quadkey(21, child.x, child.y))
                case Classification.NO_INTERSECTION:
                    continue

    return cellstring_z21
