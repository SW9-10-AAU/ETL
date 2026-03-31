import math
from enum import Enum

import mercantile
from shapely import LineString, Polygon, MultiPolygon, box

from ukc_core.quadkey_utils import quadkey_to_int, zxy_to_quadkey


class Classification(Enum):
    """Enum for classifying tile containment in a Polygon or MultiPolygon."""

    FULLY_CONTAINED = 1
    PARTIALLY_CONTAINED = 2
    NO_INTERSECTION = 3


# --- Constants ---
DEFAULT_ZOOM = 21  # Default zoom level

# --- Encoding Utilities ---


def xyz_to_quadkey_int(zoom: int, x: int, y: int) -> int:
    return quadkey_to_int(zxy_to_quadkey(zoom, x, y))


def _point_to_tile_fraction(lon: float, lat: float, zoom: int) -> tuple[float, float]:
    """Convert lon/lat to fractional tile coordinates at the given zoom level.

    Adapted from Carto's quadbin ``point_to_tile_fraction``.
    Returns (x_frac, y_frac).
    """
    z2 = 1 << zoom
    sinlat = math.sin(lat * math.pi / 180.0)
    x = z2 * (lon / 360.0 + 0.5)
    y_fraction = 0.5 - 0.25 * math.log((1 + sinlat) / (1 - sinlat)) / math.pi
    y = max(0.0, min(z2 - 1, z2 * y_fraction))
    # Wrap x into [0, z2)
    x = x % z2
    if x < 0:
        x += z2
    return x, y


def linecover(
    ls: LineString,
    zoom: int = DEFAULT_ZOOM,
) -> list[tuple[int, int]]:
    """Return an list of (cell_id, epoch_timestamp) tuples that fully cover a LineString.

    This is an adaptation of Carto's Quadbin ``line_cover`` algorithm
    (https://github.com/CartoDB/quadbin-py) that uses Amanatides & Woo
    style grid traversal. It naturally preserves the temporal order of
    tiles as the line is walked from coordinate to coordinate.

    Args:
        ls:     A Shapely LineString.
        zoom:   Tile zoom level.

    Returns:
        List of (cell_id, epoch_timestamp) tuples, temporally ordered.
        Consecutive duplicates are suppressed so the caller only sees
        each tile once per contiguous run.
    """
    cells_with_time: list[tuple[int, int]] = []
    prev_cell_id = None
    
    coords = list(ls.coords)
    for i in range(len(coords) - 1):
        x0_f, y0_f = _point_to_tile_fraction(coords[i][0], coords[i][1], zoom)
        x1_f, y1_f = _point_to_tile_fraction(coords[i + 1][0], coords[i + 1][1], zoom)

        segment_cells: list[int] = []

        has_z = len(coords[i]) > 2 and len(coords[i + 1]) > 2
        ts_segment_start = int(coords[i][2]) if has_z else 0
        ts_segment_end = int(coords[i + 1][2]) if has_z else 0

        dx = x1_f - x0_f
        dy = y1_f - y0_f

        if dx == 0 and dy == 0:
            continue

        sx = 1 if dx > 0 else -1
        sy = 1 if dy > 0 else -1

        x = math.floor(x0_f)
        y = math.floor(y0_f)

        t_max_x = (
            float("inf") if dx == 0 else abs(((1 if dx > 0 else 0) + x - x0_f) / dx)
        )
        t_max_y = (
            float("inf") if dy == 0 else abs(((1 if dy > 0 else 0) + y - y0_f) / dy)
        )
        tdx = float("inf") if dx == 0 else abs(sx / dx)
        tdy = float("inf") if dy == 0 else abs(sy / dy)

        # Emit the first cell
        segment_cells.append(xyz_to_quadkey_int(zoom, x, y))

        while t_max_x < 1 or t_max_y < 1:
            if t_max_x < t_max_y:
                t_max_x += tdx
                x += sx
            else:
                t_max_y += tdy
                y += sy

            segment_cells.append(xyz_to_quadkey_int(zoom, x, y))

        # Linear interpolation of timestamps across all cells in this segment
        num_cells = len(segment_cells)
        for idx, cell_id in enumerate(segment_cells):
            if (num_cells == 1):  # If the start and end points are in the same cell, we duplicate the cell, but with different timestamps
                if cell_id != prev_cell_id:
                    cells_with_time.append((cell_id, ts_segment_start))
                    prev_cell_id = cell_id
                continue  # Important to skip the rest of the loop
            else:
                # Linear interpolation: first cell gets ts0, last cell gets ts1
                progress = idx / (num_cells - 1)
                interpolated_ts = round(
                    ts_segment_start + progress * (ts_segment_end - ts_segment_start)
                )
            if cell_id != prev_cell_id:
                cells_with_time.append((cell_id, interpolated_ts))
                prev_cell_id = cell_id
                     
    return cells_with_time


def classify_tile_containment(
    poly: Polygon | MultiPolygon, tile: mercantile.Tile
) -> Classification:
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


def get_all_children_at_zoom(
    tile: mercantile.Tile, target_zoom: int
) -> list[mercantile.Tile]:
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


def process_z13_tiles(
    poly: Polygon | MultiPolygon,
) -> tuple[list[int], list[mercantile.Tile], list[mercantile.Tile]]:
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

        cellstring_z13.append(xyz_to_quadkey_int(13, tile.x, tile.y))

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
            cellstring_z17.append(xyz_to_quadkey_int(17, child.x, child.y))
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

            cellstring_z17.append(xyz_to_quadkey_int(17, child.x, child.y))

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
            cellstring_z21.append(xyz_to_quadkey_int(21, child.x, child.y))

    for tile in partially_contained_z17:
        children_z21 = get_all_children_at_zoom(tile, 21)
        for child in children_z21:
            classification = classify_tile_containment(poly, child)

            match classification:
                case (
                    Classification.FULLY_CONTAINED | Classification.PARTIALLY_CONTAINED
                ):
                    cellstring_z21.append(xyz_to_quadkey_int(21, child.x, child.y))
                case Classification.NO_INTERSECTION:
                    continue

    return cellstring_z21


# ---- Deprecated encoding functions ----
ENCODE_OFFSET_Z21 = 100_000_000_000_000
ENCODE_OFFSET_Z17 = 1_000_000_000_000
ENCODE_OFFSET_Z13 = 100_000_000
ENCODE_MULT_Z21 = 10_000_000
ENCODE_MULT_Z17 = 1_000_000
ENCODE_MULT_Z13 = 10_000


def deprecated_get_tile_xy(
    lon: float, lat: float, zoom: int = DEFAULT_ZOOM
) -> tuple[int, int]:
    tile = mercantile.tile(lon, lat, zoom)
    return tile.x, tile.y


def deprecated_encode_tile_xy_to_cellid(
    x: int, y: int, zoom: int = DEFAULT_ZOOM
) -> int:
    if zoom == 13:
        return ENCODE_OFFSET_Z13 + (x * ENCODE_MULT_Z13) + y

    if zoom == 17:
        return ENCODE_OFFSET_Z17 + (x * ENCODE_MULT_Z17) + y

    return ENCODE_OFFSET_Z21 + (x * ENCODE_MULT_Z21) + y


def deprecated_encode_lonlat_to_cellid(
    lon: float, lat: float, zoom: int = DEFAULT_ZOOM
) -> int:
    x, y = deprecated_get_tile_xy(lon, lat, zoom)
    return deprecated_encode_tile_xy_to_cellid(x, y, zoom)


def deprecated_decode_cellid_to_tile(
    cellid: int, zoom: int = DEFAULT_ZOOM
) -> tuple[int, int]:
    """Decode a cell ID back to tile (x, y) coordinates."""
    if zoom == 13:
        offset = ENCODE_OFFSET_Z13
        mult = ENCODE_MULT_Z13
    elif zoom == 17:
        offset = ENCODE_OFFSET_Z17
        mult = ENCODE_MULT_Z17
    else:
        offset = ENCODE_OFFSET_Z21
        mult = ENCODE_MULT_Z21

    cellid_adjusted = cellid - offset
    x = cellid_adjusted // mult
    y = cellid_adjusted % mult
    return (x, y)
