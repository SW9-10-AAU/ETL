import unittest
from mercantile import Tile
import src.transform_ls_to_cs as transform
from shapely import LineString, Polygon

from src.transform_ls_to_cs import is_unique_cells


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
    
    
class TestLineStringToCellString(unittest.TestCase):

    def test_convert_linestring_to_cellstring(self):
        linestring = LineString([[10.836495399475098, 57.36823654174805],[10.83551025390625, 57.368526458740234]])
        
        cellstring = transform.convert_linestring_to_cellstring(linestring)
        expected_cellstring = [1_1111703_0638525, 1_1111702_0638525, 1_1111701_0638524, 1_1111700_0638524, 1_1111699_0638523, 1_1111698_0638523, 1_1111697_0638522]
        
        self.assertEqual(cellstring, expected_cellstring)
    
    def test_convert_linestring_to_cellstring_last_two_points_in_same_cell(self):
        linestring = LineString([[10.836495399475098, 57.36823654174805],[10.83551025390625, 57.368526458740234], [10.835510777, 57.368526435]])
        
        cellstring = transform.convert_linestring_to_cellstring(linestring)
        expected_cellstring = [1_1111703_0638525, 1_1111702_0638525, 1_1111701_0638524, 1_1111700_0638524, 1_1111699_0638523, 1_1111698_0638523, 1_1111697_0638522, 1_1111697_0638522]
        
        self.assertEqual(cellstring, expected_cellstring)
    
class TestPolygonToCellString(unittest.TestCase):

    def test_convert_polygon_to_cellstring(self):
        polygon = Polygon([[10.788898468017578, 57.37221145629883], [10.787409782409668, 57.37289810180664], [10.787253379821777, 57.37300491333008], [10.786907196044922, 57.37324905395508], [10.786700248718262, 57.37343215942383], [10.786727905273438, 57.37344741821289], [10.78807258605957, 57.373050689697266], [10.788095474243164, 57.373043060302734], [10.788132667541504, 57.373023986816406], [10.788783073425293, 57.37237548828125], [10.78880500793457, 57.372352600097656], [10.788829803466797, 57.37232208251953], [10.788851737976074, 57.372291564941406], [10.7888765335083, 57.37225341796875], [10.788898468017578, 57.37221145629883]])
        
        tiles = transform.get_tiles_in_polygon_bbox(polygon)
        example_tile = Tile(1111423,638477,21) # Tile that is inside the bounding box of the polygon, but its centre is not inside the polygon itself
        self.assertIn(example_tile, tiles)
        example_cellid = transform.encode_tile_xy_to_cellid(example_tile.x, example_tile.y)
        
        cellstring = transform.convert_polygon_to_cellstring(polygon)
        expected_cellstring = [111114130638469,111114140638470,111114140638471,111114150638471,111114150638472,111114160638471,111114160638472,111114160638473,111114170638472,111114170638473,111114170638474,111114180638472,111114180638473,111114180638474,111114180638475,111114190638473,111114190638474,111114190638475,111114190638476,111114200638473,111114200638474,111114200638475,111114200638476,111114200638477,111114210638474,111114210638475,111114210638476,111114210638477,111114210638478,111114220638476,111114220638477,111114220638478,111114220638479,111114230638478,111114230638479,111114230638480,111114240638480,111114250638481]
        
        self.assertEqual(cellstring, expected_cellstring)
        self.assertNotIn(example_cellid, cellstring) # cell that overlaps the polygon, but its centre is not inside the polygon

class TestLinestringToCellStringInsertion(unittest.TestCase):

    def test_unique_cells_false(self):
        linestring = LineString([[10.836495399475098, 57.36823654174805],[10.836495399475099, 57.36823654174805],[10.83551025390625, 57.368526458740234]])
        cellstring = transform.convert_linestring_to_cellstring(linestring)

        is_unique = transform.is_unique_cells(cellstring)

        self.assertFalse(is_unique) # because the first two points are in the same cell

    def test_unique_cells_true(self):
        linestring = LineString([[10.836495399475098, 57.36823654174805],[10.83551025390625, 57.368526458740234]])
        cellstring = transform.convert_linestring_to_cellstring(linestring)

        is_unique = transform.is_unique_cells(cellstring)
        print(cellstring)

        self.assertTrue(is_unique) # all points are in different cells
    
if __name__ == '__main__':
    unittest.main()
