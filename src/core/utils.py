from math import inf
from geopy.distance import geodesic
from shapely import Polygon, Point, MultiPoint, from_wkt

KNOT_AS_MPS = 0.514444  # 1 knot = 0.514444 m/s
MIN_POINTS_IN_SEGMENT = 2  # Minimum number of points in a trajectory or stop segment


def distance_m(p1: Point, p2: Point) -> float:
    """Return distance between two Shapely Points in meters."""
    return geodesic(
        (p1.y, p1.x), (p2.y, p2.x)
    ).meters  # x and y is swapped because this is (lat,lon), and Shapely/PostGIS is (lon,lat)


def extract_time_s(p: Point) -> float:
    """Extract time in seconds from the M dimension of a Shapely Point."""
    return p.m


def extract_start_end_time_s(points: list[Point]) -> tuple[float, float]:
    """Extract start and end time in seconds from a list of Shapely Points."""
    return extract_time_s(points[0]), extract_time_s(points[-1])


def compute_motion(prev_point: Point, curr_point: Point) -> tuple[float, float, float]:
    """Compute time difference (s), distance difference (m), and average vessel speed (knots) between two points."""
    time_diff = extract_time_s(curr_point) - extract_time_s(prev_point)
    dist_diff = distance_m(prev_point, curr_point)
    avg_vessel_speed = (dist_diff / time_diff / KNOT_AS_MPS) if time_diff > 0 else inf
    return time_diff, dist_diff, avg_vessel_speed


def compute_mbr_area(poly: Polygon) -> float:
    """Compute the area of the Minimum Bounding Rectangle (MBR) of a polygon in square meters."""
    minx, miny, maxx, maxy = poly.bounds
    w = geodesic((miny, minx), (miny, maxx)).meters
    h = geodesic((miny, minx), (maxy, minx)).meters
    return w * h


def merge_candidate_stops(
    candidate_stops: list[list[Point]],
    merge_time_threshold: float,
    merge_distance_threshold: float,
) -> list[list[Point]]:
    """Merge nearby candidate stops based on distance and time thresholds."""
    if not candidate_stops:
        return []

    merged_stops: list[list[Point]] = []
    current_merged_stop = candidate_stops[0]

    for current_candidate_stop in candidate_stops[1:]:
        center_merged = MultiPoint(current_merged_stop).centroid
        center_candidate = MultiPoint(current_candidate_stop).centroid

        # Time difference between end of current merged stop and start of candidate stop
        time_diff = extract_time_s(current_candidate_stop[0]) - extract_time_s(
            current_merged_stop[-1]
        )

        # Distance between centroids of the current merged stop and the candidate stop
        dist_diff = distance_m(center_merged, center_candidate)

        if time_diff < merge_time_threshold and dist_diff < merge_distance_threshold:
            # Merge candidate stop into current merged stop
            current_merged_stop.extend(current_candidate_stop)
        else:
            # Finalize current merged stop and start a new one
            merged_stops.append(current_merged_stop)
            current_merged_stop = current_candidate_stop

    # Append the last merged stop
    merged_stops.append(current_merged_stop)

    return merged_stops


def add_connecting_point_to_segment(current_segment: list[Point], point: Point):
    """Add the connecting point (previous point) to the current segment if it is empty (i.e. starting a new segment). A segment can be a trajectory or a stop."""
    if len(current_segment) == 0:
        current_segment.append(point)


def append_segment_if_nonempty_and_clear_segment(
    candidate_segments: list[list[Point]], current_segment: list[Point]
):
    """Appends current segment to candidate segments if it has at least 2 points. Finally, it clears the segment (regardless of whether it was appended). A segment can be a trajectory or a stop."""
    if len(current_segment) >= MIN_POINTS_IN_SEGMENT:
        candidate_segments.append(current_segment.copy())  # snapshot

    current_segment.clear()  # empties in caller


def try_merge_invalid_merged_stop_with_trajectories(
    trajs: list[list[Point]],
    invalid_merged_stop: list[Point],
    traj_max_speed_kn: float,
    traj_max_gap_s: float,
    min_ais_points_in_traj: int,
):
    """Insert or merge a non-valid stop with existing trajectories."""

    # First, validate the invalid_merged_stop points to ensure no traj with unrealistic speeds/time gaps is created
    for p1, p2 in zip(invalid_merged_stop, invalid_merged_stop[1:]):
        time_diff, _, avg_vessel_speed = compute_motion(p1, p2)
        if avg_vessel_speed > traj_max_speed_kn or time_diff > traj_max_gap_s:
            return  # Discard the invalid stop

    # Used to compare start/end points between stop and trajectories
    first_stop_pt = invalid_merged_stop[0]
    last_stop_pt = invalid_merged_stop[-1]

    traj_before_idx = None
    traj_after_idx = None

    # Find trajectories to merge with
    for i, traj in enumerate(trajs):
        first_traj_pt = traj[0]
        last_traj_pt = traj[-1]

        # Compare full (x, y, m) - exact equality
        if last_traj_pt.coords[0] == first_stop_pt.coords[0]:
            traj_before_idx = i
        if first_traj_pt.coords[0] == last_stop_pt.coords[0]:
            traj_after_idx = i

    # Case 1: Stop connects/bridges two trajectories (stop starts where one trajectory ends and ends where another trajectory starts)
    if (
        traj_before_idx is not None
        and traj_after_idx is not None
        and traj_before_idx != traj_after_idx
    ):
        before_traj = trajs[traj_before_idx]
        after_traj = trajs[traj_after_idx]
        merged_traj = before_traj + invalid_merged_stop.copy() + after_traj

        # replace both in list
        trajs[traj_before_idx] = merged_traj
        # remove the later one (index may shift if before < after)
        trajs.pop(
            traj_after_idx if traj_after_idx > traj_before_idx else traj_before_idx + 1
        )
        return

    # Case 2: Stop continues a trajectory (stop starts where a trajectory ends)
    if traj_before_idx is not None:
        trajs[traj_before_idx].extend(invalid_merged_stop)
        return

    # Case 3: Stop precedes a trajectory (stop ends where a trajectory starts)
    if traj_after_idx is not None:
        trajs[traj_after_idx] = invalid_merged_stop + trajs[traj_after_idx]
        return

    # Case 4: No merge possible = treat as new trajectory (if it has enough points)
    if len(invalid_merged_stop) >= min_ais_points_in_traj:
        trajs.append(invalid_merged_stop)


def points_to_linestringm_as_wkb(points: list[Point]) -> bytes:
    """Build a LineStringM, treating the third coordinate as M (epoch timestamp) from a list of Points. Returns the LineStringM as WKB."""
    coords_m = " , ".join(f"{p.x} {p.y} {int(p.m)}" for p in points)

    return from_wkt(f"LINESTRING M ({coords_m})").wkb
