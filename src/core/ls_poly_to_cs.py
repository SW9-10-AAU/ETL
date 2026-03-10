from typing import cast
import mercantile
from shapely import LineString, MultiPolygon, Polygon, box, from_wkb
from ukc_core.quadkey_utils import zxy_to_quadkey

from core.cellstring_utils import DEFAULT_ZOOM, convert_tiles_to_shapely_polygon, encode_tile_xy_to_cellid, find_noncontained_ls_segments, get_tile_xy, get_tiles_for_endpoints, process_z13_tiles, process_z17_tiles, process_z21_tiles, supercover

Row = tuple[int, int, int, int, bytes]  # (trajectory_id/stop_id, mmsi, ts_start, ts_end, geom_wkb)
ProcessResultTraj = tuple[int, int, int, list[str]]  # trajectory_id, mmsi, ts, cell_z21(bitencoded)
ProcessResultStop = tuple[int, int, int, int, list[str]]  # stop_id, mmsi, ts_start, ts_end, cell_z21(bitencoded)

# --- Conversion Utilities ---

def convert_linestring_to_cellstrings(ls: LineString) -> list[int]:
    """Convert a LineString to CellStrings at z21."""
    cell_z21 = convert_linestring_to_cellstring(ls, 21)
    return cell_z21

def convert_linestring_to_cellstring(ls: LineString, zoom: int = DEFAULT_ZOOM) -> list[int]:
    """Convert a LineString to CellString at the specified zoom level."""
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
            for ls_segment in noncontained_ls_segments:
                seg_coords = list(ls_segment.coords)

                for start_coord, end_coord in zip(seg_coords[:-1], seg_coords[1:]):
                    # Get all intersecting tiles for both endpoints of the non-contained LineString segment
                    start_tiles, end_tiles = get_tiles_for_endpoints((start_coord[0], start_coord[1]), (end_coord[0], end_coord[1]), zoom)
                    
                    # Add supercover tiles between all candidate pairs.
                    for x0_c, y0_c in start_tiles:
                        for x1_c, y1_c in end_tiles:
                            segment_tiles.extend(supercover(x0_c, y0_c, x1_c, y1_c))
                            
            # Check containment with updated tiles
            segment_tiles_poly = convert_tiles_to_shapely_polygon(segment_tiles, zoom)
            noncontained_ls_segments = find_noncontained_ls_segments(segment_ls, segment_tiles_poly)

        # Convert segment tiles to bitencoded cell IDs and add to cellstring
        for x, y in segment_tiles:
            cellstring.append(zxy_to_quadkey(zoom, x, y))
    
    deduplicated_cellstring = list(dict.fromkeys(cellstring))
    return deduplicated_cellstring

def convert_polygon_to_cellstrings(poly: Polygon | MultiPolygon, skip_z21: bool = False) -> tuple[list[int], list[int], list[int]]:
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
    cellstring_z17, fully_contained_z17, partially_contained_z17 = process_z17_tiles(poly, fully_contained_z13, partially_contained_z13)
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
        tile_poly: Polygon  = box(bounds.west, bounds.south, bounds.east, bounds.north)
        if poly.intersects(tile_poly):
            cellstring.append(encode_tile_xy_to_cellid(tile.x, tile.y, zoom))
    return cellstring


# --- Worker Functions ---

def process_trajectory_row(row: Row) -> ProcessResultTraj:
    trajectory_id, mmsi, ts, _, geom_wkb = row
    linestring = cast(LineString, from_wkb(geom_wkb))
    cellstring_z21= convert_linestring_to_cellstrings(linestring)
    print(f"Processed trajectory_id {trajectory_id} with {len(cellstring_z21)} z21 cells.")
  
    return (trajectory_id, mmsi, ts, cellstring_z21)

def process_stop_row(row: Row) -> ProcessResultStop:
    stop_id, mmsi, ts_start, ts_end, geom_wkb = row
    polygon = cast(Polygon, from_wkb(geom_wkb))

    _, _, cellstring_z21 = convert_polygon_to_cellstrings(polygon)
   
    return stop_id, mmsi, ts_start, ts_end, cellstring_z21