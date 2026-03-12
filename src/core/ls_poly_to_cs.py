from typing import cast
import mercantile
from shapely import LineString, MultiPolygon, Polygon, box, from_wkb

from core.cellstring_utils import DEFAULT_ZOOM, encode_tile_xy_to_cellid, linecover, process_z13_tiles, \
    process_z17_tiles, process_z21_tiles

Row = tuple[int, int, int, int, bytes]  # (trajectory_id/stop_id, mmsi, ts_start, ts_end, geom_wkb)
ProcessResultTraj = tuple[int, int, int, int, list[int], list[int], list[
    int]]  # trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21
ProcessResultStop = tuple[int, int, int, int, list[int], list[int], list[
    int]]  # stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21


# --- Conversion Utilities ---

def convert_linestring_to_cellstrings(ls: LineString) -> tuple[list[int], list[int], list[int]]:
    """Convert a LineString to CellStrings at z13, z17, and z21."""
    cellstring_z13 = convert_linestring_to_cellstring(ls, 13)
    cellstring_z17 = convert_linestring_to_cellstring(ls, 17)
    cellstring_z21 = convert_linestring_to_cellstring(ls, 21)
    return cellstring_z13, cellstring_z17, cellstring_z21


def convert_linestring_to_cellstring(ls: LineString, zoom: int = DEFAULT_ZOOM) -> list[int]:
    """Convert a LineString to CellString at the specified zoom level.

    Uses the Amanatides & Woo grid-traversal algorithm (via ``linecover``)
    which guarantees full coverage of every segment without gap-checking.
    Every cell is preserved in traversal order — no deduplication is
    performed — so that the full temporal sequence of tile visits is kept.
    """
    if ls.is_empty:
        return []

    coords = list(ls.coords)
    tiles = linecover(coords, zoom)

    return [encode_tile_xy_to_cellid(x, y, zoom) for x, y in tiles]


def convert_polygon_to_cellstrings(poly: Polygon | MultiPolygon, skip_z21: bool = False) -> tuple[
    list[int], list[int], list[int]]:
    """
    Convert Polygon or MultiPolygon to CellStrings at z13, z17, and z21.

    Args:
        poly: A Shapely Polygon or MultiPolygon to convert

    Returns:
        Tuple of (cellstring_z13, cellstring_z17, cellstring_z21)
    """
    if poly.is_empty:
        return ([], [], [])

    cellstring_z13, fully_contained_z13, partially_contained_z13 = process_z13_tiles(poly)
    cellstring_z17, fully_contained_z17, partially_contained_z17 = process_z17_tiles(poly, fully_contained_z13,
                                                                                     partially_contained_z13)
    cellstring_z21 = [] if skip_z21 else process_z21_tiles(poly, fully_contained_z17, partially_contained_z17)

    return cellstring_z13, cellstring_z17, cellstring_z21


def deprecated_convert_polygon_to_cellstring(poly: Polygon | MultiPolygon, zoom: int = DEFAULT_ZOOM) -> list[int]:
    """
    --- DEPRECATED ---
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
        tile_poly: Polygon = box(bounds.west, bounds.south, bounds.east, bounds.north)
        if poly.intersects(tile_poly):
            cellstring.append(encode_tile_xy_to_cellid(tile.x, tile.y, zoom))
    return cellstring


# --- Worker Functions ---

def process_trajectory_row(row: Row) -> ProcessResultTraj:
    trajectory_id, mmsi, ts_start, ts_end, geom_wkb = row
    linestring = cast(LineString, from_wkb(geom_wkb))
    cellstring_z13, cellstring_z17, cellstring_z21 = convert_linestring_to_cellstrings(linestring)
    print(
        f"Processed trajectory_id {trajectory_id} with {len(cellstring_z13)} z13 cells, {len(cellstring_z17)} z17 cells, and {len(cellstring_z21)} z21 cells.")

    return (trajectory_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21)


def process_stop_row(row: Row) -> ProcessResultStop:
    stop_id, mmsi, ts_start, ts_end, geom_wkb = row
    polygon = cast(Polygon, from_wkb(geom_wkb))

    cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstrings(polygon)
    return stop_id, mmsi, ts_start, ts_end, cellstring_z13, cellstring_z17, cellstring_z21
