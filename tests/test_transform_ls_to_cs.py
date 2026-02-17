import unittest
import mercantile
from shapely import LineString, Polygon
import src.transform_ls_to_cs as transform


class TestEncodeLonLatToMVTCellId(unittest.TestCase):

    def test_HouHavn(self):
        lon, lat = 10.383365, 57.056374
        cell_id = transform.encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1109063_0641880)

    def test_toprightquadrant_VenoeHavn(self):
        lon, lat = 8.614294, 56.550693
        cell_id = transform.encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1098757_0647260)

    def test_topleftquadrant_Canada(self):
        lon, lat = -123.120231, 49.290563
        cell_id = transform.encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_0331348_0717620)

    def test_bottomleftquadrant_BuenosAires(self):
        lon, lat = -57.853151, -34.469250
        cell_id = transform.encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_0711556_1262712)

    def test_bottomrightquadrant_Melbourne(self):
        lon, lat = 144.944281, -37.815050
        cell_id = transform.encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1892937_1286854)


class TestLineStringToCellStringTransformation(unittest.TestCase):
    """Tests for convert_linestring_to_cellstring transformation logic."""

    def _decode_cellid_to_tile(self, cellid: int, zoom: int) -> tuple[int, int]:
        """Decode a cell ID back to tile (x, y) coordinates."""
        if zoom == 13:
            offset = transform.ENCODE_OFFSET_Z13
            mult = transform.ENCODE_MULT_Z13
        elif zoom == 17:
            offset = transform.ENCODE_OFFSET_Z17
            mult = transform.ENCODE_MULT_Z17
        else:
            offset = transform.ENCODE_OFFSET_Z21
            mult = transform.ENCODE_MULT_Z21

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
        cellstring = transform.convert_linestring_to_cellstring(linestring, zoom=13)

        # Should have cells
        self.assertGreater(len(cellstring), 0, "Should produce cells for trajectory")

        # Cells should be valid (decodable)
        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        self.assertEqual(len(tile_coords), len(cellstring))

    def test_linestring_coverage_simple_north(self):
        """Test: simple north-moving trajectory produces coverage."""
        linestring = LineString([
            (10.0, 55.0),
            (10.0, 55.1),
            (10.0, 55.2),
        ])
        cellstring = transform.convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        self.assertEqual(len(tile_coords), len(cellstring))

    def test_linestring_two_segments_produces_cells(self):
        """Test: two-segment trajectory produces cells for both segments."""
        linestring = LineString([
            [10.836495399475098, 57.36823654174805],
            [10.83551025390625, 57.368526458740234]
        ])
        cellstring = transform.convert_linestring_to_cellstring(linestring)

        # Should produce multiple cells
        self.assertGreater(len(cellstring), 0, "Two-segment trajectory should produce cells")

        # All cells should be decodable
        for cell in cellstring:
            tile_coords = self._decode_cellid_to_tile(cell, 21)
            self.assertIsInstance(tile_coords, tuple)
            self.assertEqual(len(tile_coords), 2)

    def test_linestring_three_segments_with_duplicate_endpoint(self):
        """Test: three-segment trajectory with duplicate endpoint produces cells."""
        linestring = LineString([
            [10.836495399475098, 57.36823654174805],
            [10.83551025390625, 57.368526458740234],
            [10.835510777, 57.368526435]  # Very close to previous point
        ])
        cellstring = transform.convert_linestring_to_cellstring(linestring)

        # Should produce cells even with duplicate endpoint
        self.assertGreater(len(cellstring), 0)

        # All cells should be valid
        for cell in cellstring:
            self.assertIsInstance(cell, int)
            self.assertGreater(cell, 0)

    def test_linestring_empty_returns_empty(self):
        """Test: empty LineString returns empty cellstring."""
        linestring = LineString()
        cellstring = transform.convert_linestring_to_cellstring(linestring)

        self.assertEqual(cellstring, [])

    def test_linestring_uses_correct_zoom_levels(self):
        """Test: cellstrings at different zoom levels have different cell counts."""
        linestring = LineString([
            (10.0, 55.0),
            (10.1, 55.1),
        ])

        cs_z13 = transform.convert_linestring_to_cellstring(linestring, zoom=13)
        cs_z17 = transform.convert_linestring_to_cellstring(linestring, zoom=17)
        cs_z21 = transform.convert_linestring_to_cellstring(linestring, zoom=21)

        # Higher zoom levels should generally produce more cells (more granular)
        self.assertGreater(len(cs_z21), 0)
        self.assertGreater(len(cs_z17), 0)
        self.assertGreater(len(cs_z13), 0)

        # z21 should have more or equal cells than z17, which should have more than z13
        # (higher zoom = smaller tiles = more tiles needed)
        self.assertGreaterEqual(len(cs_z21), len(cs_z17))
        self.assertGreaterEqual(len(cs_z17), len(cs_z13))

    def test_process_trajectory_row_deduplicates(self):
        """Test: process_trajectory_row returns deduplicated cellstrings for all zoom levels."""
        from shapely.wkb import dumps

        linestring = LineString([
            (10.0, 55.0),
            (10.05, 55.05),
            (10.1, 55.1),
        ])
        geom_wkb = dumps(linestring)

        row: transform.Row = (1, 12345, 1000, 2000, geom_wkb)
        result = transform.process_trajectory_row(row, use_supercover=False)

        trajectory_id, mmsi, ts_start, ts_end, is_unique, cs_z13, cs_z17, cs_z21 = result

        # Verify structure
        self.assertEqual(trajectory_id, 1)
        self.assertEqual(mmsi, 12345)
        self.assertEqual(ts_start, 1000)
        self.assertEqual(ts_end, 2000)
        self.assertIsInstance(is_unique, bool)

        # Verify all zoom levels produce cells
        self.assertGreater(len(cs_z13), 0)
        self.assertGreater(len(cs_z17), 0)
        self.assertGreater(len(cs_z21), 0)

        # Verify deduplication: no duplicates in any cellstring
        self.assertEqual(len(cs_z13), len(set(cs_z13)),
                         "z13 cellstring should have no duplicates")
        self.assertEqual(len(cs_z17), len(set(cs_z17)),
                         "z17 cellstring should have no duplicates")
        self.assertEqual(len(cs_z21), len(set(cs_z21)),
                         "z21 cellstring should have no duplicates")

    def test_process_trajectory_row_with_supercover(self):
        """Test: process_trajectory_row works with supercover=True."""
        from shapely.wkb import dumps

        linestring = LineString([
            (10.0, 55.0),
            (10.05, 55.05),
            (10.1, 55.1),
        ])
        geom_wkb = dumps(linestring)

        row: transform.Row = (2, 54321, 2000, 3000, geom_wkb)
        result = transform.process_trajectory_row(row, use_supercover=True)

        trajectory_id, mmsi, ts_start, ts_end, is_unique, cs_z13, cs_z17, cs_z21 = result

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
        cellstring = transform.convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

        # Verify cells are decodable
        tile_coords = [self._decode_cellid_to_tile(cell, 13) for cell in cellstring]
        first_x, first_y = tile_coords[0]
        last_x, last_y = tile_coords[-1]

        # Overall progression should be northeast
        self.assertLess(first_x, last_x, "Should progress eastward")
        self.assertGreater(first_y, last_y, "Should progress northward (Web Mercator)")


class TestPolygonToCellString(unittest.TestCase):

    def test_convert_polygon_to_cellstring(self):
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

        cellstring = transform.convert_polygon_to_cellstring(polygon)

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


if __name__ == "__main__":
    unittest.main()
