import os
import sys
import unittest

sys.path.insert(
    0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src")
)

from shapely import MultiPoint, from_wkt

from core.utils import compute_centroid_of_points


def _make_point_m(lon: float, lat: float, epoch_ts: int):
    """Create a Shapely Point M from lon, lat, and epoch timestamp."""
    return from_wkt(f"POINT M ({lon} {lat} {int(epoch_ts)})")


class TestCentroidOfPoints(unittest.TestCase):
    """Verify _centroid_of_points matches MultiPoint(...).centroid."""

    def _assert_centroid_matches(self, points, msg=""):
        """Helper: compute centroid via _centroid_of_points and compare to MultiPoint.centroid."""
        sx, sy, n = compute_centroid_of_points(points)
        computed_x = sx / n
        computed_y = sy / n

        expected = MultiPoint(points).centroid

        self.assertAlmostEqual(
            computed_x, expected.x, places=10, msg=f"x mismatch {msg}"
        )
        self.assertAlmostEqual(
            computed_y, expected.y, places=10, msg=f"y mismatch {msg}"
        )

    def test_single_point(self):
        points = [_make_point_m(10.383365, 57.056374, 1700000000)]
        self._assert_centroid_matches(points)

    def test_two_points(self):
        points = [
            _make_point_m(10.0, 55.0, 1700000000),
            _make_point_m(10.2, 55.4, 1700000060),
        ]
        self._assert_centroid_matches(points)

    def test_many_points_cluster(self):
        """Cluster of points around a harbor."""
        points = [
            _make_point_m(10.383 + i * 0.001, 57.056 + i * 0.0005, 1700000000 + i * 10)
            for i in range(20)
        ]
        self._assert_centroid_matches(points)

    def test_coincident_points(self):
        """All points at the exact same location."""
        points = [_make_point_m(10.0, 55.0, 1700000000 + i * 10) for i in range(50)]
        self._assert_centroid_matches(points)

    def test_antipodal_hemisphere_points(self):
        """Points spread across different hemispheres."""
        points = [
            _make_point_m(-57.853, -34.469, 1700000000),
            _make_point_m(144.944, -37.815, 1700000060),
            _make_point_m(-123.120, 49.290, 1700000120),
            _make_point_m(10.383, 57.056, 1700000180),
        ]
        self._assert_centroid_matches(points)


if __name__ == "__main__":
    unittest.main()
