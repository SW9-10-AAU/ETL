import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

import unittest
import mercantile
from shapely import LineString, Point, Polygon
from shapely.wkb import dumps

from core.cellstring_utils import ENCODE_MULT_Z13, ENCODE_MULT_Z17, ENCODE_MULT_Z21, ENCODE_OFFSET_Z13, ENCODE_OFFSET_Z17, ENCODE_OFFSET_Z21, Classification, classify_tile_containment, encode_lonlat_to_cellid
from core.ls_poly_to_cs import Row, convert_linestring_to_cellstring, convert_polygon_to_cellstrings, deprecated_convert_polygon_to_cellstring, process_trajectory_row
from core.points_to_ls_poly import process_single_mmsi


class TestEncodeLonLatToMVTCellId(unittest.TestCase):

    def test_HouHavn(self):
        lon, lat = 10.383365, 57.056374
        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1109063_0641880)

    def test_toprightquadrant_VenoeHavn(self):
        lon, lat = 8.614294, 56.550693
        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1098757_0647260)

    def test_topleftquadrant_Canada(self):
        lon, lat = -123.120231, 49.290563
        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_0331348_0717620)

    def test_bottomleftquadrant_BuenosAires(self):
        lon, lat = -57.853151, -34.469250
        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_0711556_1262712)

    def test_bottomrightquadrant_Melbourne(self):
        lon, lat = 144.944281, -37.815050
        cell_id = encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1892937_1286854)


class TestLineStringToCellStringTransformation(unittest.TestCase):
    """Tests for convert_linestring_to_cellstring transformation logic."""

    def _decode_cellid_to_tile(self, cellid: int, zoom: int) -> tuple[int, int]:
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

    def test_linestring_coverage_simple_east(self):
        """Test: simple east-moving trajectory produces coverage."""
        linestring = LineString([
            (10.0, 55.0),
            (10.1, 55.0),
            (10.2, 55.0),
        ])
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0, "Should produce cells for trajectory")

        # All cells should be decodable
        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        self.assertEqual(len(tile_coords), len(cellstring))

        # Verify no duplicates (deduplication at end of function)
        self.assertEqual(len(cellstring), len(set(cellstring)),
                         "Cellstring should have no duplicates after deduplication")

    def test_linestring_coverage_simple_north(self):
        """Test: simple north-moving trajectory produces coverage."""
        linestring = LineString([
            (10.0, 55.0),
            (10.0, 55.1),
            (10.0, 55.2),
        ])
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        self.assertEqual(len(tile_coords), len(cellstring))

        # Verify no duplicates
        self.assertEqual(len(cellstring), len(set(cellstring)))

    def test_linestring_two_segments_produces_cells(self):
        """Test: two-segment trajectory produces cells for both segments."""
        linestring = LineString([
            [10.836495399475098, 57.36823654174805],
            [10.83551025390625, 57.368526458740234]
        ])
        cellstring = convert_linestring_to_cellstring(linestring)

        self.assertGreater(len(cellstring), 0, "Two-segment trajectory should produce cells")

        # All cells should be decodable
        for cell in cellstring:
            tile_coords = self._decode_cellid_to_tile(cell, 21)
            self.assertIsInstance(tile_coords, tuple)
            self.assertEqual(len(tile_coords), 2)

        # No duplicates
        self.assertEqual(len(cellstring), len(set(cellstring)))

    def test_linestring_three_segments_with_duplicate_endpoint(self):
        """Test: three-segment trajectory with duplicate endpoint produces cells."""
        linestring = LineString([
            [10.836495399475098, 57.36823654174805],
            [10.83551025390625, 57.368526458740234],
            [10.835510777, 57.368526435]
        ])
        cellstring = convert_linestring_to_cellstring(linestring)

        self.assertGreater(len(cellstring), 0)

        # All cells should be valid
        for cell in cellstring:
            self.assertIsInstance(cell, int)
            self.assertGreater(cell, 0)

        # No duplicates after deduplication
        self.assertEqual(len(cellstring), len(set(cellstring)))

    def test_linestring_empty_returns_empty(self):
        """Test: empty LineString returns empty cellstring."""
        linestring = LineString()
        cellstring = convert_linestring_to_cellstring(linestring)

        self.assertEqual(cellstring, [])

    def test_linestring_uses_correct_zoom_levels(self):
        """Test: cellstrings at different zoom levels have different cell counts."""
        linestring = LineString([
            (10.0, 55.0),
            (10.1, 55.1),
        ])

        cs_z13 = convert_linestring_to_cellstring(linestring, zoom=13)
        cs_z17 = convert_linestring_to_cellstring(linestring, zoom=17)
        cs_z21 = convert_linestring_to_cellstring(linestring, zoom=21)

        self.assertGreater(len(cs_z21), 0)
        self.assertGreater(len(cs_z17), 0)
        self.assertGreater(len(cs_z13), 0)

        # Higher zoom = more granular = more cells
        self.assertGreaterEqual(len(cs_z21), len(cs_z17))
        self.assertGreaterEqual(len(cs_z17), len(cs_z13))

    def test_linestring_temporal_order_preserved(self):
        """Test: cells progress in trajectory direction (temporal order)."""
        linestring = LineString([
            (10.0, 55.0),
            (10.1, 55.1),
        ])
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        first_x, first_y = tile_coords[0]
        last_x, last_y = tile_coords[-1]

        # Should progress northeast
        self.assertLess(first_x, last_x, "Should progress eastward")
        self.assertGreater(first_y, last_y, "Should progress northward (Web Mercator)")

    def test_process_trajectory_row(self):
        """Test: process_trajectory_row works and produces unique cells."""
        linestring = LineString([
            (10.0, 55.0),
            (10.05, 55.05),
            (10.1, 55.1),
        ])
        geom_wkb = dumps(linestring)

        row: Row = (2, 54321, 2000, 3000, geom_wkb)
        result = process_trajectory_row(row)

        trajectory_id, mmsi, ts_start, ts_end, cs_z13, cs_z17, cs_z21 = result

        self.assertEqual(trajectory_id, 2)
        self.assertEqual(mmsi, 54321)

        # All should be deduplicated
        self.assertEqual(len(cs_z13), len(set(cs_z13)))
        self.assertEqual(len(cs_z17), len(set(cs_z17)))
        self.assertEqual(len(cs_z21), len(set(cs_z21)))

    def test_linestring_diagonal_northeast(self):
        """Test: diagonal northeast trajectory produces valid cells."""
        linestring = LineString([
            (10.0, 55.0),
            (10.05, 55.05),
            (10.1, 55.1),
        ])
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        first_x, first_y = tile_coords[0]
        last_x, last_y = tile_coords[-1]

        self.assertLess(first_x, last_x, "Should progress eastward")
        self.assertGreater(first_y, last_y, "Should progress northward (Web Mercator)")

        # No duplicates
        self.assertEqual(len(cellstring), len(set(cellstring)))


class TestPolygonToCellStrings(unittest.TestCase):

    def test_convert_polygon_to_cellstrings(self):
        polygon = Polygon([
            [10.788898468017578, 57.37221145629883],
            [10.787409782409668, 57.37289810180664],
            [10.787253379821777, 57.37300491333008],
            [10.786907196044922, 57.37324905395508],
            [10.786700248718262, 57.37343215942383],
            [10.786727905273438, 57.37344741821289],
            [10.78807258605957, 57.373050689697266],
            [10.788095474243164, 57.373043060302734],
            [10.788132667541504, 57.373023986816406],
            [10.788783073425293, 57.37237548828125],
            [10.78880500793457, 57.372352600097656],
            [10.788829803466797, 57.37232208251953],
            [10.788851737976074, 57.372291564941406],
            [10.7888765335083, 57.37225341796875],
            [10.788898468017578, 57.37221145629883]
        ])

        cellstring = deprecated_convert_polygon_to_cellstring(polygon, 21)

        expected = [
            111114130638469, 111114130638470, 111114140638469, 111114140638470, 111114140638471,
            111114140638472, 111114150638470, 111114150638471, 111114150638472, 111114150638473,
            111114160638470, 111114160638471, 111114160638472, 111114160638473, 111114160638474,
            111114170638471, 111114170638472, 111114170638473, 111114170638474, 111114170638475,
            111114180638471, 111114180638472, 111114180638473, 111114180638474, 111114180638475,
            111114180638476, 111114190638472, 111114190638473, 111114190638474, 111114190638475,
            111114190638476, 111114190638477, 111114200638473, 111114200638474, 111114200638475,
            111114200638476, 111114200638477, 111114200638478, 111114210638473, 111114210638474,
            111114210638475, 111114210638476, 111114210638477, 111114210638478, 111114210638479,
            111114220638474, 111114220638475, 111114220638476, 111114220638477, 111114220638478,
            111114220638479, 111114220638480, 111114230638476, 111114230638477, 111114230638478,
            111114230638479, 111114230638480, 111114230638481, 111114240638478, 111114240638479,
            111114240638480, 111114240638481, 111114250638480, 111114250638481, 111114250638482
        ]

        self.assertEqual(cellstring, expected)


class TestHierarchicalPolygonToCellString(unittest.TestCase):

    def test_hierarchical_vs_original_same_results(self):
        """Verify hierarchical algorithm produces same cellstrings as original algorithm."""
        polygon = Polygon([
            [
              10.314142022338359,
              56.989841283038544
            ],
            [
              10.308192009866758,
              56.96758619876718
            ],
            [
              10.32171476548396,
              56.97466210465393
            ],
            [
              10.339564802898877,
              56.97525170280585
            ],
            [
              10.343892084696563,
              56.969650143499706
            ],
            [
              10.324419316607646,
              56.9565274047078
            ],
            [
              10.341457988686244,
              56.94753049842973
            ],
            [
              10.36390576301099,
              56.96729134018483
            ],
            [
              10.378510339077962,
              56.99617639075896
            ],
            [
              10.353087558517444,
              56.99882797615109
            ],
            [
              10.347137546045786,
              56.99043064087442
            ],
            [
              10.336860251775192,
              56.97878909571352
            ],
            [
              10.32766477795559,
              56.97937862853158
            ],
            [
              10.327935233067706,
              56.98880988437111
            ],
            [
              10.314142022338359,
              56.989841283038544
            ]
        ])

        # Original algorithm
        z13_old = deprecated_convert_polygon_to_cellstring(polygon, 13)
        z17_old = deprecated_convert_polygon_to_cellstring(polygon, 17)
        z21_old = deprecated_convert_polygon_to_cellstring(polygon, 21)

        # Hierarchical algorithm
        z13_new, z17_new, z21_new = convert_polygon_to_cellstrings(polygon)

        # Should produce identical results (using sets since order may differ)
        self.assertEqual(set(z13_old), set(z13_new), "Z13 cellstrings should match")
        self.assertEqual(set(z17_old), set(z17_new), "Z17 cellstrings should match")
        self.assertEqual(set(z21_old), set(z21_new), "Z21 cellstrings should match")

    def test_hierarchical_empty_polygon(self):
        """Verify hierarchical algorithm handles empty polygons correctly."""
        polygon = Polygon()
        z13, z17, z21 = convert_polygon_to_cellstrings(polygon)

        self.assertEqual(z13, [])
        self.assertEqual(z17, [])
        self.assertEqual(z21, [])

    def test_classify_tile_containment(self):
        """Test the classify_tile_containment helper function."""
        # Create a simple polygon
        polygon = Polygon([
            [10.0, 57.0],
            [10.0, 58.0],
            [11.0, 58.0],
            [11.0, 57.0],
            [10.0, 57.0]
        ])

        # Get a tile inside the polygon
        tile_inside = mercantile.tile(10.5, 57.5, 13)
        classification_inside = classify_tile_containment(polygon, tile_inside)
        self.assertEqual(classification_inside, Classification.FULLY_CONTAINED)

        # Get a tile outside the polygon
        tile_outside = mercantile.tile(15.0, 60.0, 13)
        classification_outside = classify_tile_containment(polygon, tile_outside)
        self.assertEqual(classification_outside, Classification.NO_INTERSECTION)
        
    def make_point(self, lon, lat, ts):
        from shapely.wkb import dumps
        return dumps(Point(lon, lat, ts))
    def test_single_point_leftover_does_not_connect(self):
        mmsi = 123456789
        points = []

        start_ts = 1700000000

        # Step 1: England trajectory (2 points)
        points.append((self.make_point(-1.0, 52.0, start_ts), 12.0))
        points.append((self.make_point(-0.99, 52.0, start_ts + 60), 12.0))

        # Step 2: small gap > 1 hour to cut the first trajectory
        gap1_ts = start_ts + 7200  # 2 hours later
        points.append((self.make_point(-0.98, 52.0, gap1_ts), 12.0))  # leftover England point

        # Step 3: big gap of 3 days before Germany points
        gap2_ts = gap1_ts + 3 * 24 * 3600  # 3 days later

        # Step 4: Germany points (enough to form a trajectory)
        for i in range(11):
            points.append((self.make_point(8.5 + i * 0.01, 53.5, gap2_ts + i * 60), 12.0))

        # Run ETL
        _, trajs, _ = process_single_mmsi(mmsi, points)

        # Only Germany trajectory should remain (England trajectory is too short)
        self.assertEqual(len(trajs), 1, "Only Germany trajectory should be kept")

        # All points in trajectory must be Germany points
        coords = list(trajs[0][3].coords)
        for lon, lat, _ in coords:
            self.assertGreater(lon, 8.0, "No England points should appear in Germany trajectory")
            self.assertAlmostEqual(lat, 53.5, delta=0.01)

        # First coordinate should be Germany, not England
        first_lon, first_lat, _ = coords[0]
        self.assertGreater(first_lon, 8.0, "Trajectory must start in Germany")


if __name__ == "__main__":
    unittest.main()
