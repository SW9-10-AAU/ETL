import os
import sys

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

import unittest

import mercantile
from shapely import LineString, Point, Polygon, from_wkb, from_wkt

from core.cellstring_utils import (
    Classification,
    classify_tile_containment,
    deprecated_encode_lonlat_to_cellid,
)
from core.ls_poly_to_cs import (
    convert_linestring_to_cellids,
    convert_linestring_to_cellstring,
    convert_polygon_to_cellstrings,
    deprecated_convert_polygon_to_cellstring,
)
from core.points_to_ls_poly import AISPointWKB, process_single_mmsi


class TestEncodeLonLatToMVTCellId(unittest.TestCase):

    def test_HouHavn(self):
        lon, lat = 10.383365, 57.056374
        cell_id = deprecated_encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1109063_0641880)

    def test_toprightquadrant_VenoeHavn(self):
        lon, lat = 8.614294, 56.550693
        cell_id = deprecated_encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1098757_0647260)

    def test_topleftquadrant_Canada(self):
        lon, lat = -123.120231, 49.290563
        cell_id = deprecated_encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_0331348_0717620)

    def test_bottomleftquadrant_BuenosAires(self):
        lon, lat = -57.853151, -34.469250
        cell_id = deprecated_encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_0711556_1262712)

    def test_bottomrightquadrant_Melbourne(self):
        lon, lat = 144.944281, -37.815050
        cell_id = deprecated_encode_lonlat_to_cellid(lon, lat)
        self.assertEqual(cell_id, 1_1892937_1286854)


class TestLineStringToCellStringTransformation(unittest.TestCase):
    """Tests for convert_linestring_to_cellstring transformation logic."""

    def test_linestring_coverage_simple_east(self):
        """Test: simple east-moving trajectory produces coverage."""
        linestring = LineString(
            [
                (10.0, 55.0, 1000),
                (10.1, 55.0, 1010),
                (10.2, 55.0, 1020),
            ]
        )
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0, "Should produce cells for trajectory")

    def test_linestring_coverage_simple_north(self):
        """Test: simple north-moving trajectory produces coverage."""
        linestring = LineString(
            [
                (10.0, 55.0, 1000),
                (10.0, 55.1, 1010),
                (10.0, 55.2, 1020),
            ]
        )
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

    def test_linestring_two_segments_produces_cells(self):
        """Test: two-segment trajectory produces cells for both segments."""
        linestring = LineString(
            [
                [10.836495399475098, 57.36823654174805, 1000],
                [10.83551025390625, 57.368526458740234, 1010],
            ]
        )
        cellstring = convert_linestring_to_cellstring(linestring)

        self.assertGreater(
            len(cellstring), 0, "Two-segment trajectory should produce cells"
        )

    def test_linestring_three_segments_with_duplicate_endpoint(self):
        """Test: three-segment trajectory with duplicate endpoint produces cells."""
        linestring = LineString(
            [
                [10.836495399475098, 57.36823654174805, 1000],
                [10.83551025390625, 57.368526458740234, 1010],
                [10.835510777, 57.368526435, 1020],
            ]
        )
        cellstring = convert_linestring_to_cellstring(linestring)

        self.assertGreater(len(cellstring), 0)

    def test_linestring_empty_returns_empty(self):
        """Test: empty LineString returns empty cellstring."""
        linestring = LineString()
        cellstring = convert_linestring_to_cellstring(linestring)

        self.assertEqual(cellstring, [])

    def test_linestring_uses_correct_zoom_levels(self):
        """Test: cellstrings at different zoom levels have different cell counts."""
        linestring = LineString(
            [
                (10.0, 55.0, 1000),
                (10.1, 55.1, 1010),
            ]
        )

        cs_z13 = convert_linestring_to_cellstring(linestring, zoom=13)
        cs_z17 = convert_linestring_to_cellstring(linestring, zoom=17)
        cs_z21 = convert_linestring_to_cellstring(linestring, zoom=21)

        self.assertGreater(len(cs_z21), 0)
        self.assertGreater(len(cs_z17), 0)
        self.assertGreater(len(cs_z13), 0)

        # Higher zoom = more granular = more cells
        self.assertGreaterEqual(len(cs_z21), len(cs_z17))
        self.assertGreaterEqual(len(cs_z17), len(cs_z13))

    def test_linestring_self_intersecting_preserves_revisited_cells(self):
        """Test: a trajectory that crosses its own path preserves revisited cells."""
        # A figure-eight-ish path that revisits the starting area
        linestring = LineString(
            [
                (10.0, 55.0, 1000),
                (10.1, 55.1, 1010),
                (10.2, 55.0, 1020),
                (10.1, 54.9, 1030),
                (10.0, 55.0, 1040),
            ]
        )
        cellstring = convert_linestring_to_cellstring(linestring, zoom=13)

        self.assertGreater(len(cellstring), 0)

        # The cellstring should have MORE entries than unique cells,
        # because the path revisits cells it already passed through.
        self.assertGreater(
            len(cellstring),
            len(set((cell_id for cell_id, _ in cellstring))),
            "Self-intersecting trajectory should have duplicate cells",
        )

    def test_2d_linestring_conversion_without_timestamps(self):
        linestring = LineString(
            [
                (10.0, 55.0),
                (10.1, 55.0),
                (10.2, 55.0),
            ]
        )

        cell_ids = convert_linestring_to_cellids(linestring, zoom=13)

        self.assertGreater(len(cell_ids), 0)
        self.assertIsInstance(cell_ids[0], int)


class TestPolygonToCellStrings(unittest.TestCase):

    def test_convert_polygon_to_cellstrings(self):
        self.maxDiff = None

        polygon = Polygon(
            [
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
                [10.788898468017578, 57.37221145629883],
            ]
        )

        cellstring = deprecated_convert_polygon_to_cellstring(polygon, 21)

        expected = [
            1661610825011,
            1661610825017,
            1661610825014,
            1661610825020,
            1661610825022,
            1661610825108,
            1661610825021,
            1661610825023,
            1661610825109,
            1661610825111,
            1661610825064,
            1661610825066,
            1661610825152,
            1661610825154,
            1661610825160,
            1661610825067,
            1661610825153,
            1661610825155,
            1661610825161,
            1661610825163,
            1661610825070,
            1661610825156,
            1661610825158,
            1661610825164,
            1661610825166,
            1661610825188,
            1661610825157,
            1661610825159,
            1661610825165,
            1661610825167,
            1661610825189,
            1661610825191,
            1661610825170,
            1661610825176,
            1661610825178,
            1661610825200,
            1661610825202,
            1661610825208,
            1661610825171,
            1661610825177,
            1661610825179,
            1661610825201,
            1661610825203,
            1661610825209,
            1661610825211,
            1661610825180,
            1661610825182,
            1661610825204,
            1661610825206,
            1661610825212,
            1661610825214,
            1661610825556,
            1661610825205,
            1661610825207,
            1661610825213,
            1661610825215,
            1661610825557,
            1661610825559,
            1661610836136,
            1661610836138,
            1661610836480,
            1661610836482,
            1661610836481,
            1661610836483,
            1661610836489,
        ]

        self.assertEqual(cellstring, expected)


class TestHierarchicalPolygonToCellString(unittest.TestCase):

    def test_hierarchical_vs_original_same_results(self):
        """Verify hierarchical algorithm produces same cellstrings as original algorithm."""
        polygon = Polygon(
            [
                [10.314142022338359, 56.989841283038544],
                [10.308192009866758, 56.96758619876718],
                [10.32171476548396, 56.97466210465393],
                [10.339564802898877, 56.97525170280585],
                [10.343892084696563, 56.969650143499706],
                [10.324419316607646, 56.9565274047078],
                [10.341457988686244, 56.94753049842973],
                [10.36390576301099, 56.96729134018483],
                [10.378510339077962, 56.99617639075896],
                [10.353087558517444, 56.99882797615109],
                [10.347137546045786, 56.99043064087442],
                [10.336860251775192, 56.97878909571352],
                [10.32766477795559, 56.97937862853158],
                [10.327935233067706, 56.98880988437111],
                [10.314142022338359, 56.989841283038544],
            ]
        )

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

    def test_custom_zoom_levels_z19(self):
        """Test custom zoom levels with z19 as the finest level."""
        polygon = Polygon(
            [
                [10.314142022338359, 56.989841283038544],
                [10.308192009866758, 56.96758619876718],
                [10.32171476548396, 56.97466210465393],
                [10.339564802898877, 56.97525170280585],
                [10.343892084696563, 56.969650143499706],
                [10.324419316607646, 56.9565274047078],
                [10.341457988686244, 56.94753049842973],
                [10.36390576301099, 56.96729134018483],
                [10.378510339077962, 56.99617639075896],
                [10.353087558517444, 56.99882797615109],
                [10.347137546045786, 56.99043064087442],
                [10.336860251775192, 56.97878909571352],
                [10.32766477795559, 56.97937862853158],
                [10.327935233067706, 56.98880988437111],
                [10.314142022338359, 56.989841283038544],
            ]
        )

        # Generate cellstrings at z13, z17, z19
        z13, z17, z19 = convert_polygon_to_cellstrings(polygon, zoom_levels=(13, 17, 19))

        # Verify we get results at all three zoom levels
        self.assertGreater(len(z13), 0, "Should have z13 cells")
        self.assertGreater(len(z17), 0, "Should have z17 cells")
        self.assertGreater(len(z19), 0, "Should have z19 cells")

        # z19 should have fewer cells than z21 but more than z17
        z13_21, z17_21, z21 = convert_polygon_to_cellstrings(polygon, zoom_levels=(13, 17, 21))
        self.assertGreater(len(z21), len(z19), "Z21 should have more cells than z19")
        self.assertGreater(len(z19), len(z17), "Z19 should have more cells than z17")

        # z13 and z17 should be the same regardless of finest zoom level
        self.assertEqual(set(z13), set(z13_21), "Z13 cells should be the same")
        self.assertEqual(set(z17), set(z17_21), "Z17 cells should be the same")

    def test_custom_zoom_levels_validation(self):
        """Test that zoom level validation works correctly."""
        polygon = Polygon(
            [[10.0, 57.0], [10.0, 58.0], [11.0, 58.0], [11.0, 57.0], [10.0, 57.0]]
        )

        # Should raise error for non-increasing zoom levels
        with self.assertRaises(ValueError):
            convert_polygon_to_cellstrings(polygon, zoom_levels=(17, 13, 21))

        with self.assertRaises(ValueError):
            convert_polygon_to_cellstrings(polygon, zoom_levels=(13, 21, 17))

        with self.assertRaises(ValueError):
            convert_polygon_to_cellstrings(polygon, zoom_levels=(13, 13, 21))

    def test_classify_tile_containment(self):
        """Test the classify_tile_containment helper function."""
        # Create a simple polygon
        polygon = Polygon(
            [[10.0, 57.0], [10.0, 58.0], [11.0, 58.0], [11.0, 57.0], [10.0, 57.0]]
        )

        # Get a tile inside the polygon
        tile_inside = mercantile.tile(10.5, 57.5, 13)
        classification_inside = classify_tile_containment(polygon, tile_inside)
        self.assertEqual(classification_inside, Classification.FULLY_CONTAINED)

        # Get a tile outside the polygon
        tile_outside = mercantile.tile(15.0, 60.0, 13)
        classification_outside = classify_tile_containment(polygon, tile_outside)
        self.assertEqual(classification_outside, Classification.NO_INTERSECTION)

    def make_point(self, lon: float, lat: float, epoch_ts: int):
        return from_wkt(f"POINT M ({lon} {lat} {int(epoch_ts)})").wkb

    def test_single_point_leftover_does_not_connect(self):
        mmsi = 123456789
        points: list[AISPointWKB] = []

        start_ts = 1700000000

        # Step 1: England trajectory (2 points)
        points.append((self.make_point(-1.0, 52.0, start_ts), 12.0))
        points.append((self.make_point(-0.99, 52.0, start_ts + 60), 12.0))

        # Step 2: small gap > 1 hour to cut the first trajectory
        gap1_ts = start_ts + 7200  # 2 hours later
        points.append(
            (self.make_point(-0.98, 52.0, gap1_ts), 12.0)
        )  # leftover England point

        # Step 3: big gap of 3 days before Germany points
        gap2_ts = gap1_ts + 3 * 24 * 3600  # 3 days later

        # Step 4: Germany points (enough to form a trajectory)
        for i in range(11):
            points.append(
                (self.make_point(8.5 + i * 0.01, 53.5, gap2_ts + i * 60), 12.0)
            )

        # Run ETL
        _, trajs, _ = process_single_mmsi(mmsi, points)

        # Only Germany trajectory should remain (England trajectory is too short)
        self.assertEqual(len(trajs), 1, "Only Germany trajectory should be kept")

        # All points in trajectory must be Germany points
        _, _, _, geom_wkb = trajs[0]
        coords = list(from_wkb(geom_wkb).coords)
        for lon, lat, _ in coords:
            self.assertGreater(
                lon, 8.0, "No England points should appear in Germany trajectory"
            )
            self.assertAlmostEqual(lat, 53.5, delta=0.01)

        # First coordinate should be Germany, not England
        first_lon, first_lat, _ = coords[0]
        self.assertGreater(first_lon, 8.0, "Trajectory must start in Germany")


class TestProcessSingleMmsiCoincidentNullSog(unittest.TestCase):
    """
    Regression test: a vessel transmitting null SOG at a single fixed location
    (e.g. an AtoN or moored vessel with constant lat/lon) must be classified as
    a stop, NOT a trajectory.

    Previously, concave_hull / envelope on coincident MultiPoint returned a Point
    rather than a Polygon, causing the stop to fall through to
    try_merge_invalid_merged_stop_with_trajectories which emitted it as a trajectory.
    """

    def make_point(self, lon: float, lat: float, epoch_ts: int):
        return from_wkt(f"POINT M ({lon} {lat} {int(epoch_ts)})").wkb

    def test_coincident_null_sog_produces_stop_not_trajectory(self):
        mmsi = 999000001
        lon, lat = 10.383365, 57.056374
        start_ts = 1700000000
        n_points = 100  # 100 × 10 s = 990 s total (> MIN_STOP_DURATION=600 s)

        # All points at the exact same location, SOG=None, 10-second intervals
        wkb_points: list[AISPointWKB] = [
            (
                self.make_point(lon, lat, start_ts + i * 10),
                None,
            )  # SOG=12 every 10th point, None otherwise
            for i in range(n_points)
        ]

        mmsi_out, trajs, stops = process_single_mmsi(mmsi, wkb_points)

        self.assertEqual(mmsi_out, mmsi)
        self.assertEqual(
            len(trajs), 0, "Coincident null-SOG points must not produce a trajectory"
        )
        self.assertEqual(
            len(stops), 1, "Coincident null-SOG points must produce exactly one stop"
        )

        # Stop bounds should be very close to the fixed location
        _, ts_start, ts_end, geom_wkb = stops[0]
        self.assertEqual(ts_start, float(start_ts))
        self.assertEqual(ts_end, float(start_ts + (n_points - 1) * 10))
        self.assertAlmostEqual(from_wkb(geom_wkb).centroid.x, lon, places=2)
        self.assertAlmostEqual(from_wkb(geom_wkb).centroid.y, lat, places=2)


if __name__ == "__main__":
    unittest.main()
