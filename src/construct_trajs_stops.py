from math import inf
import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import cast
from psycopg import Connection, Cursor, connect
from shapely import Polygon, from_wkb, Point, LineString, MultiPoint
from geopy.distance import geodesic

# Stops
STOP_SOG_THRESHOLD = 1.0            # knots, vT (original = 1 knot)
STOP_DISTANCE_THRESHOLD = 250       # meters, disT (original = 2 km)    CHANGED TO 250m
STOP_TIME_THRESHOLD = 5400          # seconds, tT (original = 1.5 h)
MIN_STOP_POINTS = 10                # Δn (original = 10 points)
MIN_STOP_DURATION = 5400            # seconds, Δstopt (original = 1.5 h)
MERGE_DISTANCE_THRESHOLD = 250      # meters, Δd. (original = 2 km)     CHANGED TO 250m
MERGE_TIME_THRESHOLD = 3600         # seconds, Δt (original = 1 h)
MAX_MBR_AREA = 5_000_000            # 5 km², Maximum area of the Minimum Bounding Rectangle (MBR) for a valid stop polygon

# Trajectories
KNOT_AS_MPS = 0.514444              # 1 knot = 0.514444 m/s
TRAJ_MAX_SPEED_KN = 50.0            # knots, used to filter out false AIS points (e.g. > 50 knots)
TRAJ_MAX_GAP_S = 3600               # seconds (1h), max time gap between two AIS points in a valid trajectory
MIN_AIS_POINTS_IN_TRAJ = 10         # Minimum AIS messages required to record a trajectory, Remove trajectories with only small number of AIS points


AISPoint = tuple[int, bytes, float | None]  # (mmsi, geom as WKB, sog)

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
    time_diff = extract_time_s(curr_point) - extract_time_s(prev_point)
    dist_diff = distance_m(prev_point, curr_point)
    avg_vessel_speed = (dist_diff / time_diff / KNOT_AS_MPS) if time_diff > 0 else inf
    return time_diff, dist_diff, avg_vessel_speed

def is_valid_candidate_stop(sog: float | None, avg_vessel_speed: float, dist_diff: float, time_diff: float) -> bool:
    # Use SOG if not null, otherwise fall back to computed average speed between points
    current_speed = sog if sog is not None else avg_vessel_speed 
    
    return (current_speed < STOP_SOG_THRESHOLD 
            and time_diff < STOP_TIME_THRESHOLD 
            and dist_diff < STOP_DISTANCE_THRESHOLD)

def compute_mbr_area(poly: Polygon) -> float:
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

# ----------------------------------------------------------------------

def process_single_mmsi(db_conn_str: str, mmsi: int) -> ProcessResult:
    """
    Process a single MMSI - constructs trajectories and stops.
    Returns (mmsi, trajs_to_insert, stops_to_insert).
    """
    with connect(db_conn_str) as conn:
        cur = conn.cursor()
        points: list[tuple[Point, float | None]] = [
            (cast(Point, from_wkb(geom_wkb)), sog)
            for (_, geom_wkb, sog) in get_points_for_mmsi(cur, mmsi)
        ]
        
        prev_point = None
        current_traj : list[Point] = []
        current_stop : list[Point] = []
        candidate_trajs : list[list[Point]] = []
        candidate_stops : list[list[Point]] = []

        # Final results to insert
        trajs_to_insert : list[Traj] = []
        stops_to_insert : list[Stop] = []
        
        for current_point, sog in points:
            current_time = extract_time_s(current_point)
            
            # First point initialization
            if prev_point is None:
                if sog is not None and sog < STOP_SOG_THRESHOLD:
                    current_stop.append(current_point)
                else:
                   current_traj.append(current_point) 
                   
                prev_point = current_point
                continue
            
            # Skip points that have identical timestamps
            if (current_time == extract_time_s(prev_point)):
                continue
            
            time_diff, dist_diff, avg_vessel_speed = compute_motion(prev_point, current_point)
          
            # Candidate stop condition 
            if is_valid_candidate_stop(sog, avg_vessel_speed, dist_diff, time_diff):
                current_stop.append(current_point)
                
                # finish trajectory
                if len(current_traj) > 1:
                    candidate_trajs.append(current_traj)
                    current_traj = []
            else:
                if (avg_vessel_speed < TRAJ_MAX_SPEED_KN): # Only use points that do not imply an unrealistic speed
                    if time_diff < TRAJ_MAX_GAP_S:
                        current_traj.append(current_point)
                    else: 
                        # Cut trajectory due to large time gap
                        if len(current_traj) > 1:
                            candidate_trajs.append(current_traj)
                            current_traj = [current_point]
                            prev_point = current_point
                            continue
                else: 
                    continue # Important to not update prev_point to the skewed AIS point
                
                # finish candidate stop
                if len(current_stop) > 1:
                    candidate_stops.append(current_stop)
                    current_stop = []
                    
            prev_point = current_point

        # Final append (remaining traj or stop)
        if len(current_traj) > 1:
            candidate_trajs.append(current_traj)
        if len(current_stop) > 1:
            candidate_stops.append(current_stop)

        # Merge nearby candidate stops
        merged_stops = merge_candidate_stops(candidate_stops)

        # Validate and insert stops (fallback to merging invalid stops with trajectories)
        for merged_stop in merged_stops:
            ts_start, ts_end = extract_start_end_time_s(merged_stop)
            stop_duration = ts_end - ts_start
            
            if len(merged_stop) >= MIN_STOP_POINTS and stop_duration >= MIN_STOP_DURATION:
                stop_geom = MultiPoint(merged_stop).convex_hull

                if stop_geom.geom_type == "Polygon":
                    stop_poly = cast(Polygon, stop_geom)
                    mbr_area = compute_mbr_area(stop_poly)

                    if mbr_area <= MAX_MBR_AREA:
                        # Fully valid stop
                        stops_to_insert.append((mmsi, ts_start, ts_end, stop_poly))
                        continue  # success = skip fallback

            # If Stop does not meet criteriary to merge with trajectories
            try_merge_invalid_stop_with_trajectories(candidate_trajs, merged_stop)
            
        # Insert trajectories
        for trajectory in candidate_trajs:
            ts_start, ts_end = extract_start_end_time_s(trajectory)
            if len(trajectory) >= MIN_AIS_POINTS_IN_TRAJ and ts_end > ts_start:
                trajs_to_insert.append((mmsi, ts_start, ts_end, LineString(trajectory)))
                
        conn.commit()
        cur.close()
        
        return (mmsi, trajs_to_insert, stops_to_insert)

# ----------------------------------------------------------------------

BATCH_SIZE = 100 # Number of MMSIs to process in parallel

INSERT_TRAJ_SQL = """
    INSERT INTO prototype2.trajectory_ls_new (mmsi, ts_start, ts_end, geom)
    VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_Force2D(ST_GeomFromWKB(%s, 4326)))
"""

INSERT_STOP_SQL = """
    INSERT INTO prototype2.stop_poly_new (mmsi, ts_start, ts_end, geom)
    VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
"""

def construct_trajectories_and_stops(conn: Connection, db_conn_str: str, max_workers: int = 4, batch_size: int = BATCH_SIZE):
    """Construct trajectories and stops for all MMSIs in the database. Processes MMSIs in batches."""
    cur = conn.cursor()
    all_mmsis = get_mmsis(cur)
    cur.close()
    
    num_mmsis = len(all_mmsis)
    if num_mmsis == 0:
        print("No MMSIs to process.")
        return
    
    start_time = time.perf_counter()
    print(f"{start_time}: Processing {num_mmsis} MMSIs in batches of {batch_size} MMSIs using {max_workers} workers.")

    # Iterate in batches
    for batch_start in range(0, num_mmsis, batch_size):
        mmsis_in_batch = all_mmsis[batch_start : batch_start + batch_size]
        batch_num = batch_start//batch_size+1
        print(f"\n--- Processing batch {batch_num} (MMSIs: {mmsis_in_batch[0]} to {mmsis_in_batch[-1]}) ---")

        trajs_to_insert: list[Traj] = []
        stops_to_insert: list[Stop] = []

        # Parallel processing of the batch of MMSIs
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[FutureResult, int] = {executor.submit(process_single_mmsi, db_conn_str, mmsi) : mmsi for mmsi in mmsis_in_batch}
            
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
                    [(mmsi, ts_start, ts_end, geom) for (mmsi, ts_start, ts_end, geom) in trajs_to_insert]
                )

            if stops_to_insert:
                insert_cur.executemany(
                    INSERT_STOP_SQL,
                    [(mmsi, ts_start, ts_end, geom) for (mmsi, ts_start, ts_end, geom) in stops_to_insert]
                )

        conn.commit()

        print(f"Batch {batch_num} inserted: {len(trajs_to_insert)} trajectories, {len(stops_to_insert)} stops.")
        elapsed_time = time.perf_counter() - start_time
        print(f"Elapsed time: {elapsed_time:.2f} seconds | Avg per MMSI: {elapsed_time/batch_size:.2f}s")
            
    total_time = time.perf_counter() - start_time
    print(f"\nAll MMSIs processed.")
    print(f"Total time: {total_time/60:.2f} min | Avg per MMSI: {total_time/num_mmsis:.2f}s")
    
# ----------------------------------------------------------------------

def try_merge_invalid_stop_with_trajectories(candidate_trajs: list[list[Point]], invalid_stop: list[Point]):
    """Insert or merge a non-valid stop with existing trajectories."""
    
    # First, validate the invalid_stop points to ensure no traj with unrealistic speeds/time gaps is created
    for i in range(len(invalid_stop) - 1):
        p1 = invalid_stop[i]
        p2 = invalid_stop[i + 1]
        if (p1.coords[0] == p2.coords[0]):
            continue
        time_diff = p2.coords[0][2] - p1.coords[0][2]
        dist_diff = distance_m(p1, p2)
        avg_vessel_speed = dist_diff / time_diff / KNOT_AS_MPS if time_diff > 0 else inf
        if (avg_vessel_speed > TRAJ_MAX_SPEED_KN) or (time_diff > TRAJ_MAX_GAP_S):
            # If any pair of points violate the conditions, discard the invalid stop
            return
    
    # Used to compare start/end points between stop and trajectories
    first_stop_pt = invalid_stop[0]
    last_stop_pt = invalid_stop[-1]

    merge_before_idx = None
    merge_after_idx = None

    # Find trajectories to merge with
    for i, traj in enumerate(candidate_trajs):
        first_traj_pt = traj[0]
        last_traj_pt = traj[-1]

        # Compare full (x, y, z) - exact equality
        if (last_traj_pt.coords[0] == first_stop_pt.coords[0]):
            merge_before_idx = i
        if (first_traj_pt.coords[0] == last_stop_pt.coords[0]):
            merge_after_idx = i

    # Case 1: Stop connects two existing trajectories (bridge)
    if merge_before_idx is not None and merge_after_idx is not None and merge_before_idx != merge_after_idx:
        before_traj = candidate_trajs[merge_before_idx]
        after_traj = candidate_trajs[merge_after_idx]
        merged_traj = before_traj + invalid_stop + after_traj
        # replace both in list
        candidate_trajs[merge_before_idx] = merged_traj
        # remove the later one (index may shift if before < after)
        candidate_trajs.pop(merge_after_idx if merge_after_idx > merge_before_idx else merge_before_idx + 1)
        return

    # Case 2: Stop continues an existing trajectory
    if merge_before_idx is not None:
        candidate_trajs[merge_before_idx].extend(invalid_stop)
        return

    # Case 3: Stop precedes an existing trajectory
    if merge_after_idx is not None:
        candidate_trajs[merge_after_idx] = invalid_stop + candidate_trajs[merge_after_idx]
        return

    # Case 4: No merge possible = treat as new trajectory
    candidate_trajs.append(invalid_stop)

def get_mmsis(cur: Cursor) -> list[int]:
    """Fetch distinct MMSIs from the database. Exclude those already processed."""
    cur.execute("""
            SELECT DISTINCT mmsi
            FROM prototype2.points
            WHERE mmsi NOT IN (
                SELECT DISTINCT mmsi FROM prototype2.stop_poly_new
                UNION
                SELECT DISTINCT mmsi FROM prototype2.trajectory_ls_new
            )
            ORDER BY mmsi;
        """)
    rows = cur.fetchall()
    return [row[0] for row in rows]

def get_points_for_mmsi(cur: Cursor, mmsi: int) -> list[AISPoint]:
    """Fetch all points for a specific MMSI from the database ordered by time."""
    cur.execute("""
            SELECT mmsi, ST_AsBinary(geom), sog
            FROM prototype2.points
            WHERE mmsi = %s
            ORDER BY ST_M(geom);
        """, (mmsi,))
    rows = cur.fetchall()
    
    return [
        (int(row[0]), bytes(row[1]), float(row[2]) if row[2] is not None else None)
        for row in rows 
        if row[0] is not None and row[1] is not None
    ]
