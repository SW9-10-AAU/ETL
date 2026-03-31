import math
import os
import sys
import unittest

from shapely import LineString

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

from core.cellstring_utils import _point_to_tile_fraction
from core.ls_poly_to_cs import (
    convert_linestring_to_cellids,
    convert_linestring_to_cellstring,
)


class TestLinecoverSameCell(unittest.TestCase):

    # deprecated with new timestamp logic (keep only first/entering timestamp with cell)
    # def test_short_segment_in_same_cell_keeps_both_timestamps(self):
    #     linestring = LineString(
    #         [
    #             (10.0, 55.0, 1000),
    #             (10.0000001, 55.0000001, 1010),
    #         ]
    #     )

    #     cells_with_time = convert_linestring_to_cellstring(linestring, zoom=21)

    #     self.assertEqual(len(cells_with_time), 2)

    #     first_cell, first_ts = cells_with_time[0]
    #     second_cell, second_ts = cells_with_time[1]

    #     self.assertEqual(first_cell, second_cell)
    #     self.assertEqual(first_ts, 1000)
    #     self.assertEqual(second_ts, 1010)

    def test_point_to_tile_fraction(self):
        pointA = _point_to_tile_fraction(12.617384910583496, 56.032630920410156, 21)
        pointB = _point_to_tile_fraction(12.617497444152832, 56.03263854980469, 21)

        dx = pointB[0] - pointA[0]
        dy = pointB[1] - pointA[1]

        self.assertNotEqual(dy == 0 and dx == 0, True)  # Different fractions

        x0_tile = int(math.floor(pointA[0]))
        y0_tile = int(math.floor(pointA[1]))
        x1_tile = int(math.floor(pointB[0]))
        y1_tile = int(math.floor(pointB[1]))

        self.assertNotEqual((x0_tile, y0_tile), (x1_tile, y1_tile))  # Different tiles

        self.assertNotEqual(pointA, pointB)

    def test_2d_linestring_same_cell_returns_single_cell(self):
        linestring = LineString(
            [
                (10.0, 55.0),
                (10.0000001, 55.0000001),
            ]
        )

        cells = convert_linestring_to_cellids(linestring, zoom=21)

        self.assertEqual(len(cells), 1)

    def test_2d_linestring_revisit_removes_duplicates(self):
        linestring = LineString(
            [
                (10.0, 55.0),
                (10.1, 55.1),
                (10.2, 55.0),
                (10.1, 54.9),
                (10.0, 55.0),
            ]
        )

        cells = convert_linestring_to_cellids(linestring, zoom=13)

        self.assertGreater(len(cells), 0)
        self.assertEqual(len(cells), len(set(cells)))
        
    def test_only_first_timestamp_cell(self):
        """Test that when multiple points map to the same cell, only the entering timestamp is kept."""
        linestring = LineString(
            [
                (10.0, 55.0, 1000),
                (10.0000001, 55.0000001, 1010),
                (10.0000002, 55.0000002, 1020),
            ]
        )
        
        cells = convert_linestring_to_cellstring(linestring, zoom=21)

        # All 3 points are so close they map to the same cell at zoom 21
        # Should return only 1 cell with the entering (first) timestamp
        self.assertEqual(len(cells), 1)
        
        # Verify the cell has the first timestamp (1000), not later ones
        cell_id, timestamp = cells[0]
        self.assertEqual(timestamp, 1000)
        
        
        
    def test_ship_returns_to_cell_after_leaving(self):
        """Test that visiting the same cell multiple times keeps all non-consecutive entries.
        
        Simulates a ship that:
        - Starts in cell A (points 1-3)
        - Leaves cell A and visits cells B, C
        - Returns to cell A later
        
        Expected behavior: Both entries to cell A are kept, but consecutive 
        duplicates within the same cell are discarded.
        """
        linestring = LineString(
            [
                # Movement 1: Ship enters cell A
                (10.0, 55.0, 1000),
                (10.00001, 55.00001, 1005),     # Stay in cell A (discard)
                (10.00002, 55.00002, 1010),     # Stay in cell A (discard)
                # Movement 2: Ship leaves cell A
                (10.0003, 55.0, 1020),              # Enter cell B
                (10.0003, 55.0, 1030),              # Enter cell B (discard)
                # Movement 3: Ship returns to cell A
                (10.0, 55.0, 1040),              # Re-enter cell A (KEEP - not consecutive)
                (10.00001, 55.00001, 1045),     # Stay in cell A (discard)
            ]
        )
        
        cells = convert_linestring_to_cellstring(linestring, zoom=21)
        
       
        # Should have at least 4 distinct visits:
        # 1. Cell A at t=1000 (first entry)
        # 2. Cell B at t=1020 (leaves A)
        # 3. Cell A at t=1040 (returns to A)
        self.assertEqual(len(cells), 3)
        
        # Verify the pattern: first and last cell should be the same (same area)
        first_cell_id = cells[0][0]
        last_cell_id = cells[-1][0]
        self.assertEqual(first_cell_id, last_cell_id, 
                        "Ship should enter and re-enter the same cell")
        
        # Verify timestamps make sense
        first_ts = cells[0][1]
        last_ts = cells[-1][1]
        self.assertEqual(first_ts, 1000)
        self.assertGreaterEqual(last_ts, 1040)


if __name__ == "__main__":
    unittest.main()

if __name__ == "__main__":
    unittest.main()
