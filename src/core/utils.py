from math import inf

import numpy as np
from shapely import Polygon, from_wkt

KNOT_AS_MPS = 0.514444  # 1 knot = 0.514444 m/s
MIN_POINTS_IN_SEGMENT = 2  # Minimum number of points in a trajectory or stop segment

EARTH_RADIUS_M = 6_371_000.0  # Mean Earth radius in meters

# Coordinate (lon, lat, epoch_ts)
Coord = tuple[float, float, float]


def haversine_distance_m(lon1: float, lat1: float, lon2: float, lat2: float) -> float:
    """Haversine distance in meters between two (lon, lat) points."""
    lon1, lat1, lon2, lat2 = (
        np.radians(lon1),
        np.radians(lat1),
        np.radians(lon2),
        np.radians(lat2),
    )
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    return float(EARTH_RADIUS_M * 2 * np.arcsin(np.sqrt(a)))


def distance_m(c1: Coord, c2: Coord) -> float:
    """Return distance between two Coords in meters (Haversine)."""
    return haversine_distance_m(c1[0], c1[1], c2[0], c2[1])


def extract_time_s(c: Coord) -> float:
    """Extract time in seconds from a Coord (the third element)."""
    return c[2]


def extract_start_end_time_s(coords: list[Coord]) -> tuple[float, float]:
    """Extract start and end time in seconds from a list of Coords."""
    return coords[0][2], coords[-1][2]


def compute_motion(prev: Coord, curr: Coord) -> tuple[float, float, float]:
    """Compute time difference (s), distance difference (m), and average vessel speed (knots) between two Coords."""
    time_diff = curr[2] - prev[2]
    dist_diff = haversine_distance_m(prev[0], prev[1], curr[0], curr[1])
    avg_vessel_speed = (dist_diff / time_diff / KNOT_AS_MPS) if time_diff > 0 else inf
    return time_diff, dist_diff, avg_vessel_speed


def compute_mbr_area(poly: Polygon) -> float:
    """Compute the area of the Minimum Bounding Rectangle (MBR) of a polygon in square meters."""
    minx, miny, maxx, maxy = poly.bounds
    w = haversine_distance_m(minx, miny, maxx, miny)
    h = haversine_distance_m(minx, miny, minx, maxy)
    return w * h


def compute_centroid_of_coords(coords: list[Coord]) -> tuple[float, float, int]:
    """Return (sum_x, sum_y, count) for incremental centroid tracking."""
    sx = sum(c[0] for c in coords)
    sy = sum(c[1] for c in coords)
    return sx, sy, len(coords)


def merge_candidate_stops(
    candidate_stops: list[list[Coord]],
    merge_time_threshold: float,
    merge_distance_threshold: float,
) -> list[list[Coord]]:
    """Merge nearby candidate stops based on distance and time thresholds."""
    if not candidate_stops:
        return []

    merged_stops: list[list[Coord]] = []
    current_merged_stop = candidate_stops[0]

    # Running centroid as (sum_x, sum_y, count) — avoids rebuilding MultiPoint each iteration
    merged_sx, merged_sy, merged_n = compute_centroid_of_coords(current_merged_stop)

    for current_candidate_stop in candidate_stops[1:]:
        cand_sx, cand_sy, cand_n = compute_centroid_of_coords(current_candidate_stop)

        # Time difference between end of current merged stop and start of candidate stop
        time_diff = current_candidate_stop[0][2] - current_merged_stop[-1][2]

        # Distance between centroids of the current merged stop and the candidate stop
        dist_diff = haversine_distance_m(
            merged_sx / merged_n,
            merged_sy / merged_n,
            cand_sx / cand_n,
            cand_sy / cand_n,
        )

        if time_diff < merge_time_threshold and dist_diff < merge_distance_threshold:
            # Merge candidate stop into current merged stop
            current_merged_stop.extend(current_candidate_stop)

            # Update running centroid
            merged_sx += cand_sx
            merged_sy += cand_sy
            merged_n += cand_n
        else:
            # Finalize current merged stop and start a new one
            merged_stops.append(current_merged_stop)
            current_merged_stop = current_candidate_stop
            merged_sx, merged_sy, merged_n = cand_sx, cand_sy, cand_n

    # Append the last merged stop
    merged_stops.append(current_merged_stop)

    return merged_stops


def add_connecting_point_to_segment(current_segment: list[Coord], coord: Coord):
    """Add the connecting point (previous point) to the current segment if it is empty (i.e. starting a new segment). A segment can be a trajectory or a stop."""
    if len(current_segment) == 0:
        current_segment.append(coord)


def append_segment_if_nonempty_and_clear_segment(
    candidate_segments: list[list[Coord]], current_segment: list[Coord]
):
    """Appends current segment to candidate segments if it has at least 2 points. Finally, it clears the segment (regardless of whether it was appended). A segment can be a trajectory or a stop."""
    if len(current_segment) >= MIN_POINTS_IN_SEGMENT:
        candidate_segments.append(current_segment.copy())  # snapshot

    current_segment.clear()  # empties in caller


def try_merge_invalid_merged_stop_with_trajectories(
    trajs: list[list[Coord]],
    invalid_merged_stop: list[Coord],
    traj_max_speed_kn: float,
    traj_max_gap_s: float,
    min_ais_points_in_traj: int,
):
    """Insert or merge a non-valid stop with existing trajectories."""

    # First, validate the invalid_merged_stop points to ensure no traj with unrealistic speeds/time gaps is created
    for c1, c2 in zip(invalid_merged_stop, invalid_merged_stop[1:]):
        time_diff, _, avg_vessel_speed = compute_motion(c1, c2)
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

        # Compare full (lon, lat, epoch_ts) - exact equality
        if last_traj_pt == first_stop_pt:
            traj_before_idx = i
        if first_traj_pt == last_stop_pt:
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


def coords_to_linestringm_as_wkb(coords: list[Coord]) -> bytes:
    """Build a LineStringM from a list of Coords. Returns the LineStringM as WKB."""
    coords_m = " , ".join(f"{c[0]} {c[1]} {int(c[2])}" for c in coords)

    return from_wkt(f"LINESTRING M ({coords_m})").wkb
