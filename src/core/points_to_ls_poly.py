from typing import cast
from shapely import Polygon, from_wkb, Point, LineString, MultiPoint, concave_hull
from core.utils import add_connecting_point_to_segment, append_segment_if_nonempty_and_clear_segment, compute_mbr_area, compute_motion, extract_start_end_time_s, extract_time_s, merge_candidate_stops, try_merge_invalid_merged_stop_with_trajectories

# Stops
STOP_SOG_THRESHOLD = 1.0            # knots, vT (original = 1 knot)
STOP_DISTANCE_THRESHOLD = 250       # meters, disT (original = 2 km)    CHANGED TO 250m
STOP_TIME_THRESHOLD = 5400          # seconds, tT (original = 1.5 h)
MIN_STOP_POINTS = 10                # Δn (original = 10 points)
MIN_STOP_DURATION = 600            # seconds, Δstopt (original = 1.5 h) CHANGED TO 10 min (600s)
MERGE_DISTANCE_THRESHOLD = 50       # meters, Δd. (original = 2 km)     CHANGED TO 50m
MERGE_TIME_THRESHOLD = 3600         # seconds, Δt (original = 1 h)
MAX_MBR_AREA = 5_000_000            # 5 km², Maximum area of the Minimum Bounding Rectangle (MBR) for a valid stop polygon

# Trajectories
TRAJ_MAX_SPEED_KN = 50.0            # knots, used to filter out false AIS points (e.g. > 50 knots)
TRAJ_MAX_GAP_S = 3600               # seconds (1h), max time gap between two AIS points in a valid trajectory
MIN_AIS_POINTS_IN_TRAJ = 10         # Minimum AIS messages required to record a trajectory, Remove trajectories with only small number of AIS points

AISPointWKB = tuple[bytes, float | None]            # (geom as WKB, sog)
AISPoint = tuple[Point, float | None]               # (geom as Point, sog)
Traj = tuple[int, float, float, LineString]         # (mmsi, ts_start, ts_end, geom)
Stop = tuple[int, float, float, Polygon]            # (mmsi, ts_start, ts_end, geom)
ProcessResult = tuple[int, list[Traj], list[Stop]]  # (mmsi, trajs_to_insert, stops_to_insert)

AISPointRow = tuple[int, bytes, float | None]       # (mmsi, geom as WKB, sog)
DictAISPointWKB = dict[int, list[AISPointWKB]]      # mmsi -> list of (geom as WKB, sog)

def process_single_mmsi(mmsi: int, wkb_points: list[AISPointWKB]) -> ProcessResult:
    """
    Process the points of a single MMSI - constructs trajectories and stops.
    Returns (mmsi, trajs_to_insert, stops_to_insert).
    """
    if not wkb_points:
        return (mmsi, [], [])
    
    # Convert WKB to Shapely Points
    points: list[AISPoint] = [(cast(Point, from_wkb(geom_wkb)), sog) for (geom_wkb, sog) in wkb_points]
    
    prev_point = None
    current_traj: list[Point] = []
    current_stop: list[Point] = []
    candidate_trajs: list[list[Point]] = []
    candidate_stops: list[list[Point]] = []

    # Final trajectories and stops to insert
    trajs_to_insert: list[Traj] = []
    stops_to_insert: list[Stop] = []
    
    for current_point, sog in points:
        
        # Initialization of first point
        if prev_point is None:
            if sog is not None and sog < STOP_SOG_THRESHOLD:
                current_stop.append(current_point)
            else:
                current_traj.append(current_point) 
                
            prev_point = current_point
            continue
        
        current_time = extract_time_s(current_point)
        
        # Skip points that have identical timestamps
        if (current_time == extract_time_s(prev_point)):
            continue
        
        # Compute differences between previous and current point
        time_diff, dist_diff, avg_vessel_speed = compute_motion(prev_point, current_point)
        
        # Use SOG if not null, otherwise use the computed average speed between points
        current_speed = sog if sog is not None else avg_vessel_speed 
        
        # Candidate stop condition 
        if current_speed < STOP_SOG_THRESHOLD and time_diff < STOP_TIME_THRESHOLD and dist_diff < STOP_DISTANCE_THRESHOLD:
            add_connecting_point_to_segment(current_stop, prev_point)
            current_stop.append(current_point)
            
            # Append trajectory (if any)
            append_segment_if_nonempty_and_clear_segment(candidate_trajs, current_traj)
        
        # Trajectory condition
        else:
            add_connecting_point_to_segment(current_traj, prev_point)
            if (avg_vessel_speed < TRAJ_MAX_SPEED_KN):
                if time_diff < TRAJ_MAX_GAP_S:
                    current_traj.append(current_point)
                else: 
                    # Append trajectory (start a new one due to large time gap)
                    append_segment_if_nonempty_and_clear_segment(candidate_trajs, current_traj)
            else: 
                continue # Don't update previous point to the skewed AIS point
            
            # Append candidate stop (if any)
            append_segment_if_nonempty_and_clear_segment(candidate_stops, current_stop)
                
        # Update previous point
        prev_point = current_point 

    # Final append (remaining traj or stop)
    append_segment_if_nonempty_and_clear_segment(candidate_trajs, current_traj)
    append_segment_if_nonempty_and_clear_segment(candidate_stops, current_stop)

    # Merge nearby candidate stops
    merged_stops = merge_candidate_stops(candidate_stops, MERGE_DISTANCE_THRESHOLD, MERGE_TIME_THRESHOLD)

    # Validate and insert stops (fallback to merging invalid stops with trajectories)
    for merged_stop in merged_stops:
        ts_start, ts_end = extract_start_end_time_s(merged_stop)
        stop_duration = ts_end - ts_start
        
        if len(merged_stop) >= MIN_STOP_POINTS and stop_duration >= MIN_STOP_DURATION:
            geom_points = MultiPoint(merged_stop)
            hull = concave_hull(geom_points, ratio=0.2, allow_holes=False)
            envelope = geom_points.envelope
            
            # Use the envelope (MBR) if the hull is not Polygon
            stop_geom = hull if hull.geom_type == "Polygon" else envelope 
            
            if (stop_geom.geom_type == "Polygon"):
                stop_poly = cast(Polygon, stop_geom) 
                mbr_area = compute_mbr_area(stop_poly)
            
                if mbr_area <= MAX_MBR_AREA:
                    # Fully valid stop
                    stops_to_insert.append((mmsi, ts_start, ts_end, stop_poly))
                    continue  # Skip fallback

        # Fallback: Try to merge invalid merged stop with trajectories
        try_merge_invalid_merged_stop_with_trajectories(
            trajs=candidate_trajs,
            invalid_merged_stop=merged_stop,
            traj_max_speed_kn=TRAJ_MAX_SPEED_KN,
            traj_max_gap_s=TRAJ_MAX_GAP_S,
            min_ais_points_in_traj=MIN_AIS_POINTS_IN_TRAJ)
        
    # Validate and insert trajectories
    for trajectory in candidate_trajs:
        ts_start, ts_end = extract_start_end_time_s(trajectory)
        if len(trajectory) >= MIN_AIS_POINTS_IN_TRAJ and ts_end > ts_start:
            trajs_to_insert.append((mmsi, ts_start, ts_end, LineString(trajectory)))
    
    print(
        f"[MMSI: {mmsi}] ({len(points)} points, {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops)",
        flush=True,
    )
    
    return (mmsi, trajs_to_insert, stops_to_insert)