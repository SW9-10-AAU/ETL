from typing import cast
import mercantile
from shapely import LineString, MultiPolygon, Polygon, box, from_wkb

from core.cellstring_utils import (
    DEFAULT_ZOOM,
    linecover,
    process_tiles_at_zoom,
    process_child_tiles,
    xyz_to_quadkey_int,
)

TrajRow = tuple[
    int, int, int, int, bytes
]  # (trajectory_id, mmsi, ts_start, ts_end, geom_wkb)
StopRow = tuple[
    int, int, int, int, bytes
]  # (stop_id, mmsi, ts_start, ts_end, geom_wkb)
ProcessResultTraj = tuple[
    int, int, list[tuple[int, int]]
]  # trajectory_id, mmsi, [(cell_z21, ts)]
ProcessResultStop = tuple[
    int, int, int, int, list[int]
]  # stop_id, mmsi, ts_start, ts_end, cell_z21

# --- Conversion Utilities ---


def convert_linestring_to_cellstring(
    ls: LineString, zoom: int = DEFAULT_ZOOM
) -> list[tuple[int, int]]:
    """Convert a LineString to CellString (list of tuple(cell_id, timestamp)) at the specified zoom level with interpolated timestamps.

    Args:
        ls: A Shapely LineString to convert
        zoom: Zoom level for tiles (default: 21)

    Returns:
        List of (cell_id, epoch_timestamp) tuples, temporally ordered.
    """
    if ls.is_empty:
        return []

    return linecover(ls, zoom)


def convert_linestring_to_cellids(
    ls: LineString, zoom: int = DEFAULT_ZOOM
) -> list[int]:
    """Convert a LineString to CellString (list of cell IDs) at the specified zoom level without timestamps.

    Args:
        ls: A Shapely LineString to convert
        zoom: Zoom level for tiles (default: 21)

    Returns:
        List of integer cell IDs representing tiles that intersect the geometry, in no particular order.
    """
    cellstring_with_timestamps = convert_linestring_to_cellstring(ls, zoom)

    cell_ids = [cell_id for cell_id, _ in cellstring_with_timestamps]
    # Deduplicate cell IDs (spatial deduplication, ignoring timestamps)
    deduplicated_cell_ids = list(dict.fromkeys(cell_ids))
    return  deduplicated_cell_ids 



def convert_polygon_to_cellstrings(
    poly: Polygon | MultiPolygon,
    skip_finest_zoom: bool = False,
    zoom_levels: tuple[int, int, int] = (13, 17, 21)
) -> tuple[list[int], list[int], list[int]]:
    """
    Convert Polygon or MultiPolygon to CellStrings at configurable zoom levels.

    Args:
        poly: A Shapely Polygon or MultiPolygon to convert
        skip_finest_zoom: If True, skip the finest zoom level (for backward compatibility)
        zoom_levels: Tuple of (zoom1, zoom2, zoom3) where zoom1 < zoom2 < zoom3.
                     Defaults to (13, 17, 21) for backward compatibility.

    Returns:
        Tuple of (cellstring_zoom1, cellstring_zoom2, cellstring_zoom3)
    """
    if poly.is_empty:
        return ([], [], [])

    zoom1, zoom2, zoom3 = zoom_levels

    # Validate zoom levels
    if not (zoom1 < zoom2 < zoom3):
        raise ValueError(f"Zoom levels must be strictly increasing: {zoom1} < {zoom2} < {zoom3}")

    # Process first zoom level
    cellstring_z1, fully_contained_z1, partially_contained_z1 = process_tiles_at_zoom(
        poly, zoom1
    )

    # Process second zoom level
    cellstring_z2, fully_contained_z2, partially_contained_z2 = process_child_tiles(
        poly, fully_contained_z1, partially_contained_z1, zoom2
    )

    # Process third zoom level (if not skipped)
    cellstring_z3 = (
        []
        if skip_finest_zoom
        else process_child_tiles(poly, fully_contained_z2, partially_contained_z2, zoom3)[0]
    )

    return cellstring_z1, cellstring_z2, cellstring_z3


def deprecated_convert_polygon_to_cellstring(
    poly: Polygon | MultiPolygon, zoom: int = DEFAULT_ZOOM
) -> list[int]:
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
            cellstring.append(xyz_to_quadkey_int(zoom, tile.x, tile.y))
    return cellstring


# --- Worker Functions ---


def process_trajectory_row(row: TrajRow) -> ProcessResultTraj:
    trajectory_id, mmsi, _, _, geom_wkb = row
    linestring = cast(LineString, from_wkb(geom_wkb))
    cells_with_timestamps = convert_linestring_to_cellstring(linestring, 21)
    return (trajectory_id, mmsi, cells_with_timestamps)


def process_stop_row(row: StopRow) -> ProcessResultStop:
    stop_id, mmsi, ts_start, ts_end, geom_wkb = row
    polygon = cast(Polygon, from_wkb(geom_wkb))

    _, _, cellstring_z21 = convert_polygon_to_cellstrings(polygon)

    return stop_id, mmsi, ts_start, ts_end, cellstring_z21
