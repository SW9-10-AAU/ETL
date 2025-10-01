import unittest
import mercantile

ZOOM = 21
MAX_TILE = (2 ** ZOOM) - 1  # 0 .. 2097151 at z=21

def lonlat_to_tilexy(lon: float, lat: float, zoom: int = ZOOM) -> tuple[int, int]:
    """Convert (lon, lat) to MVT tile coordinates (x, y) at given zoom level (default = 21)."""
    tile = mercantile.tile(lon, lat, zoom)
    return tile.x, tile.y

def tilexy_to_cellid(x: int, y: int) -> int:
    """Construct cell ID as bigint: prefix(1) + x padded to 7 digits + y padded to 7 digits."""
    value = 100_000_000_000_000 + (x * 10_000_000) + y
    return value

def decode_cellid_to_tile_x_y(cell_id: int) -> tuple[int, int]:
    """Reverse from cellId to (x, y) tile coordinates."""
    raw = cell_id - 100_000_000_000_000
    x = raw // 10_000_000
    y = raw % 10_000_000
    return x, y

# Main converter
def encode_lonlat_to_cellid(lon: float, lat: float, zoom: int = ZOOM) -> int:
    """Convert (lon, lat) → cellId."""
    x, y = lonlat_to_tilexy(lon, lat, zoom)
    return tilexy_to_cellid(x, y)

# ============================
# Unit tests
# ============================

class TestMvtCellId(unittest.TestCase):
    def test_HouHavn(self):
        lon, lat = 10.383365, 57.056374
        x, y = lonlat_to_tilexy(lon, lat)
        self.assertEqual((x, y), (1109063, 641880))

        cell_id = tilexy_to_cellid(x, y)
        self.assertEqual(cell_id, 111090630641880)

        x2, y2 = decode_cellid_to_tile_x_y(cell_id)
        self.assertEqual((x2, y2), (x, y))

        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 111090630641880)

    def test_toprightquadrant_VenoeHavn(self):
        lon, lat = 8.614294, 56.550693

        x, y = lonlat_to_tilexy(lon, lat)
        self.assertEqual((x, y), (1098757, 647260))

        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 110987570647260)

        x2, y2 = decode_cellid_to_tile_x_y(cell_id)
        self.assertEqual((x2, y2), (x, y))

    def test_topleftquadrant_Canada(self):
        lon, lat = -123.120231, 49.290563

        x, y = lonlat_to_tilexy(lon, lat)
        self.assertEqual((x, y), (331348, 717620))

        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 103313480717620)

        x2, y2 = decode_cellid_to_tile_x_y(cell_id)
        self.assertEqual((x2, y2), (x, y))

    def test_bottomleftquadrant_BuenosAires(self):
        lon, lat = -57.853151, -34.469250

        x, y = lonlat_to_tilexy(lon, lat)
        self.assertEqual((x, y), (711556, 1262712))

        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 107115561262712)

        x2, y2 = decode_cellid_to_tile_x_y(cell_id)
        self.assertEqual((x2, y2), (x, y))

    def test_bottomrightquadrant_Melbourne(self):
        lon, lat = 144.944281, -37.815050

        x, y = lonlat_to_tilexy(lon, lat)
        self.assertEqual((x, y), (1892937, 1286854))

        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 118929371286854)

        x2, y2 = decode_cellid_to_tile_x_y(cell_id)
        self.assertEqual((x2, y2), (x, y))

    def test_bounds(self):
        for lon, lat in [(-180, -85), (180, 85), (0, 0)]:
            x, y = lonlat_to_tilexy(lon, lat)
            self.assertGreaterEqual(x, 0)
            self.assertLessEqual(x, MAX_TILE)
            self.assertGreaterEqual(y, 0)
            self.assertLessEqual(y, MAX_TILE)

    def test_bounds_edges(self):
        # Explicitly test world edges
        edges = [
            (-180, -85),  # bottom-left
            (180, -85),  # bottom-right
            (-180, 85),  # top-left
            (180, 85),  # top-right
        ]
        for lon, lat in edges:
            with self.subTest(lon=lon, lat=lat):
                x, y = lonlat_to_tilexy(lon, lat)
                self.assertGreaterEqual(x, 0)
                self.assertLessEqual(x, MAX_TILE)
                self.assertGreaterEqual(y, 0)
                self.assertLessEqual(y, MAX_TILE)

if __name__ == "__main__":
    unittest.main()