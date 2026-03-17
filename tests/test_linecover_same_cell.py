import math
import os
import sys
import unittest

from shapely import LineString

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

from core.cellstring_utils import _point_to_tile_fraction
from core.ls_poly_to_cs import convert_linestring_to_cellids, convert_linestring_to_cellstring


class TestLinecoverSameCell(unittest.TestCase):

    def test_short_segment_in_same_cell_keeps_both_timestamps(self):
        linestring = LineString([
            (10.0, 55.0, 1000),
            (10.0000001, 55.0000001, 1010),
        ])

        cells_with_time = convert_linestring_to_cellstring(linestring, zoom=21)

        self.assertEqual(len(cells_with_time), 2)

        first_cell, first_ts = cells_with_time[0]
        second_cell, second_ts = cells_with_time[1]

        self.assertEqual(first_cell, second_cell)
        self.assertEqual(first_ts, 1000)
        self.assertEqual(second_ts, 1010)


    def test_point_to_tile_fraction(self):
        pointA = _point_to_tile_fraction(12.617384910583496, 56.032630920410156, 21)
        pointB = _point_to_tile_fraction(12.617497444152832, 56.03263854980469, 21)
        
        dx = pointB[0] - pointA[0]
        dy = pointB[1] - pointA[1]
        
        self.assertNotEqual(dy == 0 and dx == 0, True) # Different fractions
        
        x0_tile = int(math.floor(pointA[0]))
        y0_tile = int(math.floor(pointA[1]))
        x1_tile = int(math.floor(pointB[0]))
        y1_tile = int(math.floor(pointB[1]))

        self.assertNotEqual((x0_tile, y0_tile), (x1_tile, y1_tile)) # Different tiles
        
        self.assertNotEqual(pointA, pointB)

    def test_2d_linestring_same_cell_returns_single_cell(self):
        linestring = LineString([
            (10.0, 55.0),
            (10.0000001, 55.0000001),
        ])

        cells = convert_linestring_to_cellids(linestring, zoom=21)

        self.assertEqual(len(cells), 1)

    def test_2d_linestring_revisit_removes_duplicates(self):
        linestring = LineString([
            (10.0, 55.0),
            (10.1, 55.1),
            (10.2, 55.0),
            (10.1, 54.9),
            (10.0, 55.0),
        ])

        cells = convert_linestring_to_cellids(linestring, zoom=13)

        self.assertGreater(len(cells), 0)
        self.assertEqual(len(cells), len(set(cells)))

if __name__ == '__main__':
    unittest.main()
