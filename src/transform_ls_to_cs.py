from psycopg import Connection
import mercantile
from mercantile import Tile
from shapely import from_wkb, LineString, Polygon, Point

ZOOM = 21

def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
    x, y = get_tile_xy(lon, lat)
    return encode_tile_xy_to_cellid(x,y)

def encode_tile_xy_to_cellid(x: int, y: int) -> int:
    return 100_000_000_000_000 + (x * 10_000_000) + y

def get_tile_xy(lon: float, lat: float, zoom: int = ZOOM) -> tuple[int,int]:
    tile = mercantile.tile(lon, lat, zoom)
    return tile.x, tile.y

def bresenham(x0 : int, y0 : int, x1 : int, y1 : int) -> list[tuple[int, int]]:
    tiles: list[tuple[int, int]] = []
    
    dx = abs(x1 - x0)
    dy = abs(y1 - y0)
    x, y = x0, y0
    sx = 1 if x0 < x1 else -1
    sy = 1 if y0 < y1 else -1
    
    if dx > dy:
        err = dx / 2.0
        while x != x1:
            tiles.append((x, y))
            err -= dy
            if err < 0:
                y += sy
                err += dx
            x += sx
        tiles.append((x, y))
    else:
        err = dy / 2.0
        while y != y1:
            tiles.append((x, y))
            err -= dx
            if err < 0:
                x += sx
                err += dy
            y += sy
        tiles.append((x, y))
        
    return tiles

def transform_ls_trajectories_to_cs(connection : Connection):
    cur = connection.cursor()
    cur.execute("""
            SELECT trajectory_id, mmsi, ts_start, ts_end, ST_AsBinary(geom) 
            FROM prototype1.trajectory_ls 
            ORDER BY trajectory_id;
        """)
    rows = cur.fetchall()

    for row in rows:
        _, mmsi, ts_start, ts_end, geom_wkb = row
        linestring: LineString = from_wkb(geom_wkb)
        cellstring = convert_linestring_to_cellstring(linestring)

        unique_cells: bool = is_unique_cells(cellstring)

        cur.execute("""
                INSERT INTO prototype1.trajectory_cs (mmsi, ts_start, ts_end, unique_cells, cellstring)
                VALUES (%s, %s, %s, %s, %s)
            """, (mmsi, ts_start, ts_end, unique_cells, cellstring))
    connection.commit()
    cur.close()
    
def convert_linestring_to_cellstring(linestring : LineString) -> list[int]:
    coords = list(linestring.coords)
    cellstring : list[int] = []
    for i in range(len(coords) - 1):
        lon0, lat0 = coords[i][:2]
        lon1, lat1 = coords[i + 1][:2]
        x0, y0 = get_tile_xy(lon0, lat0)
        x1, y1 = get_tile_xy(lon1, lat1)
        for x, y in bresenham(x0, y0, x1, y1):
            cellid = encode_tile_xy_to_cellid(x,y)
            cellstring.append(cellid)
    
    return cellstring

def transform_ls_stops_to_cs(connection : Connection):
    cur = connection.cursor()
    cur.execute("""
            SELECT stop_id, mmsi, ts_start, ts_end, ST_AsBinary(geom) 
            FROM prototype1.stop_poly 
            ORDER BY stop_id;
        """)
    rows = cur.fetchall()
    for row in rows:
        _, mmsi, ts_start, ts_end, geom_wkb = row
        polygon: Polygon = from_wkb(geom_wkb)
        cellstring = convert_polygon_to_cellstring(polygon)
        cur.execute("""
                INSERT INTO prototype1.stop_cs (mmsi, ts_start, ts_end, cellstring)
                VALUES (%s, %s, %s, %s)
            """, (mmsi, ts_start, ts_end, cellstring))
    connection.commit()
    cur.close()
    
def convert_polygon_to_cellstring(polygon : Polygon) -> list[int]:
    tiles = get_tiles_in_polygon_bbox(polygon)
    cellstring : list[int] = []
    
    # Loop over all tiles in bounding box
    for tile in tiles:
        tile_BBox = mercantile.bounds(tile)
        tile_center_lon, tile_center_lat = ((tile_BBox.west + tile_BBox.east) / 2), ((tile_BBox.north + tile_BBox.south) / 2)
        if polygon.contains(Point(tile_center_lon, tile_center_lat)):
            cellid = encode_tile_xy_to_cellid(tile.x,tile.y)
            cellstring.append(cellid)
    
    return cellstring

def get_tiles_in_polygon_bbox(polygon : Polygon) -> list[Tile]:
    minx, miny, maxx, maxy = polygon.bounds
    tiles = list(mercantile.tiles(minx, miny, maxx, maxy, ZOOM))
    return tiles

def is_unique_cells(cellstring : list[int]) -> bool:
    return len(cellstring) == len(set(cellstring))