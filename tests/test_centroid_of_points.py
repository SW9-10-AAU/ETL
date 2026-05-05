import os
import sys
import unittest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

from shapely import MultiPoint

from core.utils import Coord, compute_centroid_of_coords


def _make_coord(lon: float, lat: float, epoch_ts: int) -> Coord:
    """Create a Coord tuple from lon, lat, and epoch timestamp."""
    return (lon, lat, float(epoch_ts))


class TestCentroidOfPoints(unittest.TestCase):
    """Verify compute_centroid_of_coords matches MultiPoint(...).centroid."""

    def _assert_centroid_matches(self, coords: list[Coord], msg=""):
        """Helper: compute centroid via compute_centroid_of_coords and compare to MultiPoint.centroid."""
        sx, sy, n = compute_centroid_of_coords(coords)
        computed_x = sx / n
        computed_y = sy / n

        expected = MultiPoint([(c[0], c[1]) for c in coords]).centroid

        self.assertAlmostEqual(
            computed_x, expected.x, places=10, msg=f"x mismatch {msg}"
        )
        self.assertAlmostEqual(
            computed_y, expected.y, places=10, msg=f"y mismatch {msg}"
        )

    def test_single_point(self):
        coords = [_make_coord(10.383365, 57.056374, 1700000000)]
        self._assert_centroid_matches(coords)

    def test_two_points(self):
        coords = [
            _make_coord(10.0, 55.0, 1700000000),
            _make_coord(10.2, 55.4, 1700000060),
        ]
        self._assert_centroid_matches(coords)

    def test_many_points_cluster(self):
        """Cluster of points around a harbor."""
        coords = [
            _make_coord(10.383 + i * 0.001, 57.056 + i * 0.0005, 1700000000 + i * 10)
            for i in range(20)
        ]
        self._assert_centroid_matches(coords)

    def test_coincident_points(self):
        """All points at the exact same location."""
        coords = [_make_coord(10.0, 55.0, 1700000000 + i * 10) for i in range(50)]
        self._assert_centroid_matches(coords)

    def test_antipodal_hemisphere_points(self):
        """Points spread across different hemispheres."""
        coords = [
            _make_coord(-57.853, -34.469, 1700000000),
            _make_coord(144.944, -37.815, 1700000060),
            _make_coord(-123.120, 49.290, 1700000120),
            _make_coord(10.383, 57.056, 1700000180),
        ]
        self._assert_centroid_matches(coords)


if __name__ == "__main__":
    unittest.main()
