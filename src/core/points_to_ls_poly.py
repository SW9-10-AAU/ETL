from typing import cast
import time
from shapely import Polygon, from_wkb, from_wkt, Point, MultiPoint, concave_hull
from core.utils import (
    add_connecting_point_to_segment,
    append_segment_if_nonempty_and_clear_segment,
    compute_mbr_area,
    compute_motion,
    extract_start_end_time_s,
    extract_time_s,
    merge_candidate_stops,
    points_to_linestringm_as_wkb,
    try_merge_invalid_merged_stop_with_trajectories,
)

# Stops
STOP_SOG_THRESHOLD = 1.0  # knots, vT (original = 1 knot)
STOP_DISTANCE_THRESHOLD = 250  # meters, disT (original = 2 km) CHANGED TO 250 m
STOP_TIME_THRESHOLD = 5400  # seconds, tT (original = 1.5 h)
MIN_STOP_POINTS = 10  # Δn (original = 10 points)
MIN_STOP_DURATION = 600  # seconds, Δstopt (original = 1.5 h)   CHANGED TO 10 min (600s)
MERGE_DISTANCE_THRESHOLD = 50  # meters, Δd. (original = 2 km)  CHANGED TO 50 m
MERGE_TIME_THRESHOLD = 3600  # seconds, Δt (original = 1 h)
MAX_MBR_AREA = 5_000_000  # 5 km², Maximum area of the Minimum Bounding Rectangle (MBR) for a valid stop polygon
STOP_POINT_BUFFER_DEG = 1e-5  # ~1 m radius buffer in WGS84 degrees, used when all stop points have same lat,lon (e.g. null-SOG stationary vessel)

# Trajectories
TRAJ_MAX_SPEED_KN = 50.0  # knots, used to filter out false AIS points (e.g. > 50 knots)
TRAJ_MAX_GAP_S = (
    3600  # seconds (1 h), max time gap between two AIS points in a valid trajectory
)
MIN_AIS_POINTS_IN_TRAJ = 10  # Minimum AIS messages required to record a trajectory, Remove trajectories with only small number of AIS points

AISPointWKB = tuple[bytes, float | None]  # (geom as WKB, sog)
DuckDBRawPoint = tuple[float, float, float | None, float]  # (lon, lat, sog, epoch_ts)
InputPoint = AISPointWKB | DuckDBRawPoint
AISPoint = tuple[Point, float | None]  # (geom as PointM, sog)
Traj = tuple[int, float, float, bytes]  # (mmsi, ts_start, ts_end, geom as WKB)
Stop = tuple[int, float, float, bytes]  # (mmsi, ts_start, ts_end, geom as WKB)
ProcessResult = tuple[
    int, list[Traj], list[Stop]
]  # (mmsi, trajs_to_insert, stops_to_insert)

AISPointRow = tuple[int, bytes, float | None]  # (mmsi, geom as WKB, sog)
DictInputPoint = dict[
    int, list[InputPoint]
]  # mmsi -> list of InputPoint (AISPointWKB or DuckDBRawPoint)


def process_single_mmsi(mmsi: int, input_points: list[InputPoint]) -> ProcessResult:
    """
    Process the points of a single MMSI - constructs trajectories and stops.
    Returns (mmsi, trajs_to_insert, stops_to_insert).
    """
    if not input_points:
        return (mmsi, [], [])

    start_total = time.perf_counter()
    start_phase1 = time.perf_counter()

    points: list[AISPoint] = []

    # Phase 1: Parse input points into AISPoints (Point, SOG) tuples
    for input_point in input_points:
        if len(input_point) == 2:
            geom_wkb, sog = input_point
            points.append((cast(Point, from_wkb(geom_wkb)), sog))
        elif len(input_point) == 4:
            lon, lat, sog, epoch_ts = input_point
            pt = cast(Point, from_wkt(f"POINT M ({lon} {lat} {int(epoch_ts)})"))
            points.append((pt, float(sog) if sog is not None else None))

    time_phase1 = time.perf_counter() - start_phase1
    print(
        f"[MMSI: {mmsi}] Phase 1 (Parsing Input Points) completed in {time_phase1:.1f}s",
        flush=True,
    )

    prev_point = None
    current_traj: list[Point] = []
    current_stop: list[Point] = []
    candidate_trajs: list[list[Point]] = []
    candidate_stops: list[list[Point]] = []

    # Final trajectories and stops to insert
    trajs_to_insert: list[Traj] = []
    stops_to_insert: list[Stop] = []

    start_phase2 = time.perf_counter()

    # Phase 2: Iterate through points to construct candidate trajectories and stops
    for current_point, sog in points:

        # Initialization of first point
        if prev_point is None:
            if sog is None or sog < STOP_SOG_THRESHOLD:
                current_stop.append(current_point)
            else:
                current_traj.append(current_point)

            prev_point = current_point
            continue

        current_time = extract_time_s(current_point)

        # Skip points that have identical timestamps
        if current_time == extract_time_s(prev_point):
            continue

        # Compute differences between previous and current point
        time_diff, dist_diff, avg_vessel_speed = compute_motion(
            prev_point, current_point
        )

        # Use minimum of SOG or average speed if SOG is not null, otherwise use the computed average speed between points
        current_speed = (
            min(sog, avg_vessel_speed) if sog is not None else avg_vessel_speed
        )

        # Candidate stop condition
        if (
            current_speed < STOP_SOG_THRESHOLD
            and time_diff < STOP_TIME_THRESHOLD
            and dist_diff < STOP_DISTANCE_THRESHOLD
        ):
            add_connecting_point_to_segment(current_stop, prev_point)
            current_stop.append(current_point)

            # Append trajectory (if any)
            append_segment_if_nonempty_and_clear_segment(candidate_trajs, current_traj)

        # Trajectory condition
        else:
            add_connecting_point_to_segment(current_traj, prev_point)
            if avg_vessel_speed < TRAJ_MAX_SPEED_KN:
                if time_diff < TRAJ_MAX_GAP_S:
                    current_traj.append(current_point)
                else:
                    # Append trajectory (start a new one due to large time gap)
                    append_segment_if_nonempty_and_clear_segment(
                        candidate_trajs, current_traj
                    )
            else:
                continue  # Don't update previous point to the skewed AIS point

            # Append candidate stop (if any)
            append_segment_if_nonempty_and_clear_segment(candidate_stops, current_stop)

        # Update previous point
        prev_point = current_point

    # Phase 2.1: Final append (remaining traj or stop)
    append_segment_if_nonempty_and_clear_segment(candidate_trajs, current_traj)
    append_segment_if_nonempty_and_clear_segment(candidate_stops, current_stop)

    time_phase2 = time.perf_counter() - start_phase2
    num_candidate_stops = len(candidate_stops)
    num_candidate_trajs = len(candidate_trajs)
    print(
        f"[MMSI: {mmsi}] Phase 2 (Iterate through AISPoints) completed in {time_phase2:.1f}s",
        flush=True,
    )

    start_phase3 = time.perf_counter()

    # Phase 3: Merge nearby candidate stops
    merged_stops = merge_candidate_stops(
        candidate_stops, MERGE_TIME_THRESHOLD, MERGE_DISTANCE_THRESHOLD
    )

    time_phase3 = time.perf_counter() - start_phase3
    num_merged_stops = len(merged_stops)
    print(
        f"[MMSI: {mmsi}] Phase 3 (Merge of Stops) completed in {time_phase3:.1f}s",
        flush=True,
    )

    start_phase4 = time.perf_counter()
    time_phase4_1 = 0.0
    time_concave_hull = 0.0
    max_points_in_stop = 0

    # Phase 4: Final validation of stops after merging (fallback to merge invalid stops with trajectories)
    for merged_stop in merged_stops:
        max_points_in_stop = max(max_points_in_stop, len(merged_stop))
        ts_start, ts_end = extract_start_end_time_s(merged_stop)
        stop_duration = ts_end - ts_start

        if len(merged_stop) >= MIN_STOP_POINTS and stop_duration >= MIN_STOP_DURATION:
            start_hull = time.perf_counter()
            geom_points = MultiPoint(merged_stop)
            hull = concave_hull(geom_points, ratio=0.2, allow_holes=False)
            envelope = geom_points.envelope
            time_concave_hull += time.perf_counter() - start_hull

            # Use the envelope (MBR) if the hull is not Polygon
            stop_geom = hull if hull.geom_type == "Polygon" else envelope

            # We use a buffer for null-SOG stationary vessel (all points have same lat,lon).
            if stop_geom.geom_type == "Point":
                stop_geom = geom_points.centroid.buffer(STOP_POINT_BUFFER_DEG)

            if stop_geom.geom_type == "Polygon":
                stop_poly = cast(Polygon, stop_geom)
                mbr_area = compute_mbr_area(stop_poly)

                if mbr_area <= MAX_MBR_AREA:
                    # Fully valid stop
                    stops_to_insert.append((mmsi, ts_start, ts_end, stop_poly.wkb))
                    continue  # Skip fallback

        start_fallback = time.perf_counter()
        # Phase 4.1: Fallback - Try to merge invalid merged stop with trajectories
        try_merge_invalid_merged_stop_with_trajectories(
            trajs=candidate_trajs,
            invalid_merged_stop=merged_stop,
            traj_max_speed_kn=TRAJ_MAX_SPEED_KN,
            traj_max_gap_s=TRAJ_MAX_GAP_S,
            min_ais_points_in_traj=MIN_AIS_POINTS_IN_TRAJ,
        )
        time_phase4_1 += time.perf_counter() - start_fallback

    time_phase4 = time.perf_counter() - start_phase4
    print(
        f"[MMSI: {mmsi}] Phase 4 (Validation/add of Stops) completed in {time_phase4:.1f}s",
        flush=True,
    )

    start_phase5 = time.perf_counter()

    # Phase 5: Final validation of trajectories
    for trajectory in candidate_trajs:
        ts_start, ts_end = extract_start_end_time_s(trajectory)
        if len(trajectory) >= MIN_AIS_POINTS_IN_TRAJ and ts_end > ts_start:
            trajs_to_insert.append(
                (mmsi, ts_start, ts_end, points_to_linestringm_as_wkb(trajectory))
            )

    time_phase5 = time.perf_counter() - start_phase5
    print(
        f"[MMSI: {mmsi}] Phase 5 (Validation/add of Trajectories) completed in {time_phase5:.1f}s",
        flush=True,
    )

    total_time = time.perf_counter() - start_total

    print(
        f"[MMSI: {mmsi}] ({len(points)} points, {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops) processed in {total_time:.1f}s\n"
        f"  - Phase 1 (Parsing Input Points):   {time_phase1:.1f}s\n"
        f"  - Phase 2 (Iterate AISPoints):      {time_phase2:.1f}s (Candidate Trajs: {num_candidate_trajs}, Candidate Stops: {num_candidate_stops})\n"
        f"  - Phase 3 (Merge of Stops):         {time_phase3:.1f}s (Merged Stops: {num_merged_stops})\n"
        f"  - Phase 4 (Validate/add Stops):     {time_phase4:.1f}s (Max pts in stop: {max_points_in_stop})\n"
        f"    - 4.0 Concave hull:               {time_concave_hull:.1f}s\n"
        f"    - 4.1 Fallback (merge w. trajs):  {time_phase4_1:.1f}s\n"
        f"  - Phase 5 (Validate/add Trajs):     {time_phase5:.1f}s\n"
        f"{'-'*60}",
        flush=True,
    )

    return (mmsi, trajs_to_insert, stops_to_insert)
