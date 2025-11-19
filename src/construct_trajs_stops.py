from collections import defaultdict
from math import inf
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import cast
from psycopg import Connection, Cursor
from shapely import Polygon, from_wkb, Point, LineString, MultiPoint
from geopy.distance import geodesic

# Stops
STOP_SOG_THRESHOLD = 1.0            # knots, vT (original = 1 knot)
STOP_DISTANCE_THRESHOLD = 250       # meters, disT (original = 2 km)    CHANGED TO 250m
STOP_TIME_THRESHOLD = 5400          # seconds, tT (original = 1.5 h)
MIN_STOP_POINTS = 10                # Δn (original = 10 points)
MIN_STOP_DURATION = 5400            # seconds, Δstopt (original = 1.5 h)
MERGE_DISTANCE_THRESHOLD = 50       # meters, Δd. (original = 2 km)     CHANGED TO 50m
MERGE_TIME_THRESHOLD = 3600         # seconds, Δt (original = 1 h)
MAX_MBR_AREA = 5_000_000            # 5 km², Maximum area of the Minimum Bounding Rectangle (MBR) for a valid stop polygon

# Trajectories
KNOT_AS_MPS = 0.514444              # 1 knot = 0.514444 m/s
TRAJ_MAX_SPEED_KN = 50.0            # knots, used to filter out false AIS points (e.g. > 50 knots)
TRAJ_MAX_GAP_S = 3600               # seconds (1h), max time gap between two AIS points in a valid trajectory
MIN_AIS_POINTS_IN_TRAJ = 10         # Minimum AIS messages required to record a trajectory, Remove trajectories with only small number of AIS points


AISPointRow = tuple[int, bytes, float | None]  # (mmsi, geom as WKB, sog)
AISPointWKB = tuple[bytes, float | None]  # (geom as WKB, sog)
DictAISPointWKB = dict[int, list[AISPointWKB]]  # mmsi -> list of (geom as WKB, sog)
AISPoint = tuple[Point, float | None]  # (geom as Point, sog)

Traj = tuple[int, float, float, LineString]  # (mmsi, ts_start, ts_end, geom)
Stop = tuple[int, float, float, Polygon]  # (mmsi, ts_start, ts_end, geom)

ProcessResult = tuple[int, list[Traj], list[Stop]]  # (mmsi, trajs_to_insert, stops_to_insert)
FutureResult = Future[ProcessResult] # Future returning ProcessResult

# --------------------------
# Helper functions
# --------------------------

def distance_m(p1: Point, p2: Point) -> float:
    """Return distance between two Shapely Points in meters."""
    return geodesic((p1.y, p1.x), (p2.y, p2.x)).meters # x and y is swapped because this is (lat,lon), and Shapely/PostGIS is (lon,lat)

def extract_time_s(p: Point) -> float:
    """Extract time in seconds from the Z dimension of a Shapely Point."""
    return p.coords[0][2]

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

def merge_candidate_stops(candidate_stops: list[list[Point]]) -> list[list[Point]]:
    """Merge nearby candidate stops based on distance and time thresholds."""
    if not candidate_stops:
        return []
    
    merged_stops: list[list[Point]] = []
    current_merged_stop = candidate_stops[0]
    
    for current_candidate_stop in candidate_stops[1:]:
        center_merged = MultiPoint(current_merged_stop).centroid
        center_candidate = MultiPoint(current_candidate_stop).centroid
        
        # Time difference between end of current merged stop and start of candidate stop
        time_diff = extract_time_s(current_candidate_stop[0]) - extract_time_s(current_merged_stop[-1])
       
        # Distance between centroids of the current merged stop and the candidate stop
        dist_diff = distance_m(center_merged, center_candidate)
        
        if time_diff < MERGE_TIME_THRESHOLD and dist_diff < MERGE_DISTANCE_THRESHOLD:
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

def append_and_clear_segment_if_nonempty(candidate_segments: list[list[Point]], current_segment: list[Point]):
    """Append current segment to candidate segments if it has more than 1 point, then clear it. A segment can be a trajectory or a stop."""
    if len(current_segment) > 1:
        candidate_segments.append(current_segment.copy())  # snapshot
        current_segment.clear()                            # empties in caller
        
# ----------------------------------------------------------------------

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
            append_and_clear_segment_if_nonempty(candidate_trajs, current_traj)
        
        # Trajectory condition
        else:
            add_connecting_point_to_segment(current_traj, prev_point)
            if (avg_vessel_speed < TRAJ_MAX_SPEED_KN):
                if time_diff < TRAJ_MAX_GAP_S:
                    current_traj.append(current_point)
                else: 
                    # Append trajectory (start a new one due to large time gap)
                    append_and_clear_segment_if_nonempty(candidate_trajs, current_traj)
            else: 
                continue # Don't update previous point to the skewed AIS point
            
            # Append candidate stop (if any)
            append_and_clear_segment_if_nonempty(candidate_stops, current_stop)
                
        # Update previous point
        prev_point = current_point 

    # Final append (remaining traj or stop)
    append_and_clear_segment_if_nonempty(candidate_trajs, current_traj)
    append_and_clear_segment_if_nonempty(candidate_stops, current_stop)

    # Merge nearby candidate stops
    merged_stops = merge_candidate_stops(candidate_stops)

    # Validate and insert stops (fallback to merging invalid stops with trajectories)
    for merged_stop in merged_stops:
        ts_start, ts_end = extract_start_end_time_s(merged_stop)
        stop_duration = ts_end - ts_start
        
        if len(merged_stop) >= MIN_STOP_POINTS and stop_duration >= MIN_STOP_DURATION:
            geom_points = MultiPoint(merged_stop)
            hull = geom_points.convex_hull 
            envelope = geom_points.envelope
            
            # Use the envelope (MBR) if the convex hull is not Polygon
            stop_geom = hull if hull.geom_type == "Polygon" else envelope 
            
            if (stop_geom.geom_type == "Polygon"):
                stop_poly = cast(Polygon, stop_geom) 
                mbr_area = compute_mbr_area(stop_poly)
            
                if mbr_area <= MAX_MBR_AREA:
                    # Fully valid stop
                    stops_to_insert.append((mmsi, ts_start, ts_end, stop_poly))
                    continue  # Skip fallback

        # Fallback: Try to merge invalid merged stop with trajectories
        try_merge_invalid_merged_stop_with_trajectories(trajs=candidate_trajs, invalid_merged_stop=merged_stop)
        
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

# ----------------------------------------------------------------------

BATCH_SIZE = 50 # Number of MMSIs to process in parallel

INSERT_TRAJ_SQL = """
    INSERT INTO prototype2.trajectory_ls (mmsi, ts_start, ts_end, geom)
    VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_Force2D(ST_GeomFromWKB(%s, 4326)))
"""

INSERT_STOP_SQL = """
    INSERT INTO prototype2.stop_poly (mmsi, ts_start, ts_end, geom)
    VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
"""

def construct_trajectories_and_stops(conn: Connection, max_workers: int = 4, batch_size: int = BATCH_SIZE):
    """Construct trajectories and stops for all MMSIs in the database. Processes MMSIs in batches."""
    cur = conn.cursor()
    all_mmsis = get_mmsis(cur)
    cur.close()
    
    num_mmsis = len(all_mmsis)
    if num_mmsis == 0:
        print("No MMSIs to process.")
        return
    
    start_time = time.perf_counter()
    print(f"Processing {num_mmsis} MMSIs in batches of {batch_size} MMSIs using {max_workers} workers.")

    with conn.cursor() as read_cur: 
        # Iterate in batches
        for batch_start in range(0, num_mmsis, batch_size):
            mmsis_in_batch = all_mmsis[batch_start : batch_start + batch_size]
            batch_num = batch_start//batch_size + 1
            num_batches = (num_mmsis + batch_size - 1) // batch_size
            batch_start_time = time.perf_counter()
            print(f"\n--- Processing batch {batch_num} of {num_batches} ({batch_size} MMSIs: {mmsis_in_batch[0]} to {mmsis_in_batch[-1]}) ---")
            
            # Retrieve points for all MMSIs in the batch
            print(f"Fetching points for MMSIs in batch {batch_num}...")
            points: DictAISPointWKB = get_points_for_mmsis_in_batch(read_cur, mmsis_in_batch)
            print(f"{sum(len(pts) for pts in points.values()):,} points fetched.")
            
            trajs_to_insert: list[Traj] = []
            stops_to_insert: list[Stop] = []

            # Parallel processing of the batch of MMSIs
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures: dict[FutureResult, int] = {executor.submit(process_single_mmsi, mmsi, points[mmsi]) : mmsi for mmsi in mmsis_in_batch}
                
                for future in as_completed(futures):
                    mmsi = futures[future]
                    try:
                        (mmsi, trajs, stops) = future.result()
                        trajs_to_insert.extend(trajs)
                        stops_to_insert.extend(stops)
                    except Exception as e:
                        print(f"Error processing MMSI {mmsi}: {e}")
                        continue

            # Batch insert trajectories and stops into the database
            with conn.cursor() as insert_cur:
                if trajs_to_insert:
                    insert_cur.executemany(
                        INSERT_TRAJ_SQL,
                        [(mmsi, ts_start, ts_end, geom.wkb) for (mmsi, ts_start, ts_end, geom) in trajs_to_insert]
                    )

                if stops_to_insert:
                    insert_cur.executemany(
                        INSERT_STOP_SQL,
                        [(mmsi, ts_start, ts_end, geom.wkb) for (mmsi, ts_start, ts_end, geom) in stops_to_insert]
                    )

            conn.commit()

            print(f"Batch {batch_num} inserted: {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops.")
            elapsed_time = time.perf_counter() - start_time
            batch_time = time.perf_counter() - batch_start_time
            print(f"Progress: {batch_num/num_batches*100:.2f}% | Elapsed time: {elapsed_time:.2f}s | Batch time: {batch_time:.2f}s | Avg per MMSI: {batch_time/batch_size:.2f}s")
            
    total_time = time.perf_counter() - start_time
    print(f"\nAll MMSIs processed.")
    print(f"Total time: {total_time/60:.2f} min | Avg per MMSI: {total_time/num_mmsis:.2f}s")
    
# ----------------------------------------------------------------------

def try_merge_invalid_merged_stop_with_trajectories(trajs: list[list[Point]], invalid_merged_stop: list[Point]):
    """Insert or merge a non-valid stop with existing trajectories."""
    
    # First, validate the invalid_merged_stop points to ensure no traj with unrealistic speeds/time gaps is created
    for p1, p2 in zip(invalid_merged_stop, invalid_merged_stop[1:]):
        time_diff, _, avg_vessel_speed = compute_motion(p1, p2)
        if avg_vessel_speed > TRAJ_MAX_SPEED_KN or time_diff > TRAJ_MAX_GAP_S:
            return # Discard the invalid stop
        
    # Used to compare start/end points between stop and trajectories
    first_stop_pt = invalid_merged_stop[0]
    last_stop_pt = invalid_merged_stop[-1]

    traj_before_idx = None
    traj_after_idx = None

    # Find trajectories to merge with
    for i, traj in enumerate(trajs):
        first_traj_pt = traj[0]
        last_traj_pt = traj[-1]

        # Compare full (x, y, z) - exact equality
        if (last_traj_pt.coords[0] == first_stop_pt.coords[0]):
            traj_before_idx = i
        if (first_traj_pt.coords[0] == last_stop_pt.coords[0]):
            traj_after_idx = i

    # Case 1: Stop connects/bridges two trajectories (stop starts where one trajectory ends and ends where another trajectory starts)
    if traj_before_idx is not None and traj_after_idx is not None and traj_before_idx != traj_after_idx:
        before_traj = trajs[traj_before_idx]
        after_traj = trajs[traj_after_idx]
        merged_traj = before_traj + invalid_merged_stop.copy() + after_traj
        
        # replace both in list
        trajs[traj_before_idx] = merged_traj
        # remove the later one (index may shift if before < after)
        trajs.pop(traj_after_idx if traj_after_idx > traj_before_idx else traj_before_idx + 1)
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
    if (len(invalid_merged_stop) >= MIN_AIS_POINTS_IN_TRAJ):
        trajs.append(invalid_merged_stop)


def get_mmsis(cur: Cursor) -> list[int]:
    """
    Fetch MMSIs that still need processing, ordered by number of points (descending).
    """
    cur.execute("""
        SELECT p.mmsi, COUNT(*) AS num_points
        FROM prototype2.points p
        WHERE p.mmsi NOT IN (
            SELECT mmsi FROM prototype2.stop_poly
            UNION
            SELECT mmsi FROM prototype2.trajectory_ls
        )
        GROUP BY p.mmsi
        ORDER BY num_points DESC;
    """)

    rows: list[tuple[int, int]] = cur.fetchall()
    return [mmsi for mmsi, _ in rows]

def get_points_for_mmsis_in_batch(cur: Cursor, mmsis: list[int]) -> DictAISPointWKB:
    """Fetch all points for multiple MMSIs grouped by MMSI, ordered by time."""
    
    cur.execute("""
        SELECT mmsi, ST_AsBinary(geom), sog
        FROM prototype2.points
        WHERE mmsi = ANY(%s)
        ORDER BY mmsi, ST_M(geom);
    """, (mmsis,))

    rows: list[AISPointRow] = cur.fetchall()

    # Group points by MMSI
    grouped_points: DictAISPointWKB = defaultdict(list)
    
    for mmsi, geom_wkb, sog in rows:
        if mmsi is None or geom_wkb is None:
            continue

        grouped_points[mmsi].append(
            (
                geom_wkb,
                sog
            )
        )

    return grouped_points
