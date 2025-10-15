import time
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
from typing import cast
from psycopg import Connection, Cursor, connect
from shapely import Polygon, from_wkb, Point, LineString, MultiPoint
from geopy.distance import geodesic

# Threshold constants matching the paper but with ADJUSTMENTS
MIN_SOG = 0.0
STOP_SOG_THRESHOLD = 1.0            # knots, vT (original = 1 knot)
STOP_DISTANCE_THRESHOLD = 250       # meters, disT (original = 2 km)    CHANGED TO 250m
STOP_TIME_THRESHOLD = 5400          # seconds, tT (original = 1.5 h)
MIN_STOP_POINTS = 10                # Δn (original = 10 points)
MIN_STOP_DURATION = 5400            # seconds, Δstopt (original = 1.5 h)
MERGE_DISTANCE_THRESHOLD = 250      # meters, Δd. (original = 2 km)     CHANGED TO 250m
MERGE_TIME_THRESHOLD = 3600         # seconds, Δt (original = 1 h)
MAX_DIST_DIFF_POINTS_IN_TRAJ = 2500 

AISPoint = tuple[int, bytes, float | None]  # (mmsi, geom as WKB, sog)
ProcessResult = tuple[int, int, int, int, list[int]]  # (mmsi, num_points, num_trajs, num_stops, merge_case_count)
FutureResult = Future[ProcessResult] # Future returning ProcessResult

def distance_m(p1: Point, p2: Point) -> float:
    """Return distance between two Shapely Points in meters."""
    return geodesic((p1.y, p1.x), (p2.y, p2.x)).meters # x and y is swapped because this is (lat,lon), and Shapely/PostGIS is (lon,lat)

def process_single_mmsi(db_conn_str: str, mmsi: int) -> ProcessResult:
    """
    Process a single MMSI — constructs trajectories and stops.
    Returns (mmsi, num_points, num_trajs, num_stops, merge_case_count).
    """
    with connect(db_conn_str) as conn:
        cur = conn.cursor()
        points: list[tuple[Point, float | None]] = [
            (cast(Point, from_wkb(geom_wkb)), sog)
            for (_, geom_wkb, sog) in get_points_for_mmsi(cur, mmsi)
        ]

        traj : list[Point] = []
        stop : list[Point] = []
        prev_point = None
        trajs : list[list[Point]] = []
        candidate_stops : list[list[Point]] = []
        merge_case_count: list[int] = [0,0,0,0] # List corresponding to the 4 merge cases for non-valid stops merged with existing trajectories
        num_stops: int = 0

        for point, sog in points:
            current_time = point.coords[0][2] # epoch time
            if prev_point is None:
                traj = [point]
                prev_point = point
                continue
            
            time_diff = current_time - prev_point.coords[0][2]
            dist_diff = distance_m(prev_point, point)
                
            # Candidate stop condition
            if sog is not None and sog < STOP_SOG_THRESHOLD and time_diff < STOP_TIME_THRESHOLD and dist_diff < STOP_DISTANCE_THRESHOLD:
                stop.append(prev_point)
                stop.append(point)
                
                # finish trajectory
                if len(traj) > 1:
                    trajs.append(traj)
                    traj = []
            else:
                traj.append(prev_point)
                if (dist_diff < MAX_DIST_DIFF_POINTS_IN_TRAJ): # Only use non-skewed AIS points
                    traj.append(point)
                else: 
                    continue # Important to not update prev_point to the skewed AIS point
                
                # finish candidate stop
                if len(stop) > 1:
                    candidate_stops.append(stop)
                    stop = []

            prev_point = point

        # Final append (remaining traj or stop)
        if len(traj) > 1:
            trajs.append(traj)
        if len(stop) > 1:
            candidate_stops.append(stop)

        # Merge nearby candidate stops
        merged_stops : list[list[Point]] = []
        if candidate_stops:
            merged_stop = candidate_stops[0]
            for candidate_stop in candidate_stops[1:]:
                center_prev = MultiPoint(merged_stop).centroid
                center_curr = MultiPoint(candidate_stop).centroid
                start_time_curr = candidate_stop[0].coords[0][2]
                end_time_prev = merged_stop[-1].coords[0][2]
                time_gap = start_time_curr - end_time_prev
                if distance_m(center_prev, center_curr) < MERGE_DISTANCE_THRESHOLD and time_gap < MERGE_TIME_THRESHOLD:
                    merged_stop.extend(candidate_stop)
                else:
                    merged_stops.append(merged_stop)
                    merged_stop = candidate_stop
            merged_stops.append(merged_stop)

        # Insert final stops
        for merged_stop in merged_stops:
            ts_start = merged_stop[0].coords[0][2]
            ts_end = merged_stop[-1].coords[0][2]
            if len(merged_stop) >= MIN_STOP_POINTS and (ts_end - ts_start) >= MIN_STOP_DURATION:
                num_stops += 1
                insert_stop(cur, mmsi, ts_start, ts_end, MultiPoint(merged_stop).convex_hull.buffer(0))
            else:
                # Non-valid stop, try to merge with existing trajectories
                insert_or_merge_with_trajectories(trajs, merged_stop, merge_case_count)

        # Insert trajectories
        for trajectory in trajs:
            ts_start = trajectory[0].coords[0][2]
            ts_end = trajectory[-1].coords[0][2]
            if len(trajectory) > 1 and ts_end > ts_start:
                insert_trajectory(cur, mmsi, ts_start, ts_end, LineString(trajectory))

        conn.commit()
        cur.close()
        return (mmsi, len(points), len(trajs), num_stops, merge_case_count)

# ----------------------------------------------------------------------

def construct_trajectories_and_stops(conn: Connection, db_conn_str: str, max_workers: int = 4):
    """Parallel version of the main loop."""
    cur = conn.cursor()
    # mmsis = get_mmsis(cur)
    mmsis = [277547000, 266457000, 210388000]
    cur.close()
    
    total_mmsis = len(mmsis)
    if total_mmsis == 0:
        print("No MMSIs to process.")
        return
    
    print(f"Found {total_mmsis} distinct MMSIs to process using {max_workers} workers.")

    start_time = time.perf_counter()

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures: dict[FutureResult, int] = {}

        for mmsi in mmsis:
            fut: FutureResult = executor.submit(process_single_mmsi, db_conn_str, mmsi)
            futures[fut] = mmsi

        completed_count = 0
        for future in as_completed(futures):
            completed_count += 1
            mmsi = futures[future]
            try:
                mmsi, num_points, num_trajs, num_stops, merge_cases = future.result()
                print(f"MMSI {mmsi}: {num_points} points, {num_trajs} trajectories, {num_stops} stops, merge cases: {merge_cases}")
            except Exception as e:
                print(f"Error processing MMSI {mmsi}: {e}")
                
            # Timing and progress tracking
            elapsed = time.perf_counter() - start_time
            avg_time = elapsed / completed_count
            remaining = total_mmsis - completed_count
            eta = remaining * avg_time

            print(
                f"Progress: {completed_count}/{total_mmsis} ({(completed_count / total_mmsis) * 100:.2f}%) | "
                f"Avg per MMSI: {avg_time:.2f}s | ETA: {eta/60:.1f} min | Total elapsed: {elapsed/60:.1f} min\n"
            )

    total_time = time.perf_counter() - start_time
    print("All MMSIs processed.")
    print(f"Total elapsed: {total_time/60:.2f} min | Avg per MMSI: {total_time/total_mmsis:.2f} sec")
    
# ----------------------------------------------------------------------

def insert_or_merge_with_trajectories(trajs: list[list[Point]], stop: list[Point], case_count: list[int]):
    """Insert or merge a non-valid stop with existing trajectories."""
    if not stop or not trajs:
        trajs.append(stop)
        return

    # Used to compare start/end points between stop and trajectories
    first_stop_pt = stop[0]
    last_stop_pt = stop[-1]

    merge_before_idx = None
    merge_after_idx = None

    # Find trajectories to merge with
    for i, traj in enumerate(trajs):
        first_traj_pt = traj[0]
        last_traj_pt = traj[-1]

        # Compare full (x, y, z) - exact equality
        if (last_traj_pt.coords[0] == first_stop_pt.coords[0]):
            merge_before_idx = i
        if (first_traj_pt.coords[0] == last_stop_pt.coords[0]):
            merge_after_idx = i

    # Case 1: Stop connects two existing trajectories (bridge)
    if merge_before_idx is not None and merge_after_idx is not None and merge_before_idx != merge_after_idx:
        before_traj = trajs[merge_before_idx]
        after_traj = trajs[merge_after_idx]
        merged_traj = before_traj + stop + after_traj
        # replace both in list
        trajs[merge_before_idx] = merged_traj
        # remove the later one (index may shift if before < after)
        trajs.pop(merge_after_idx if merge_after_idx > merge_before_idx else merge_before_idx + 1)
        case_count[0] += 1
        return

    # Case 2: Stop continues an existing trajectory
    if merge_before_idx is not None:
        trajs[merge_before_idx].extend(stop)
        case_count[1] += 1
        return

    # Case 3: Stop precedes an existing trajectory
    if merge_after_idx is not None:
        trajs[merge_after_idx] = stop + trajs[merge_after_idx]
        case_count[2] += 1
        return

    # Case 4: No merge possible = treat as new trajectory
    trajs.append(stop)
    case_count[3] += 1

def insert_trajectory(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, line: LineString):
    cur.execute("""
            INSERT INTO prototype1.trajectory_ls_testing (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_Force3DM(ST_GeomFromWKB(%s, 4326)))
        """, (mmsi, ts_start, ts_end, line.wkb))

def insert_stop(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, poly: Polygon):
    cur.execute("""
            INSERT INTO prototype1.stop_poly_testing (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
        """, (mmsi, ts_start, ts_end, poly.wkb))

def get_mmsis(cur: Cursor) -> list[int]:
    """Fetch distinct MMSIs with at least 2 AIS points from the database."""
    cur.execute("""
            SELECT mmsi
            FROM prototype1.points
            WHERE LENGTH(mmsi::text) = 9
                AND LEFT(mmsi::text, 1) BETWEEN '2' AND '7'
                AND mmsi NOT IN (
                    SELECT DISTINCT mmsi FROM prototype1.stop_poly_testing
                    UNION
                    SELECT DISTINCT mmsi FROM prototype1.trajectory_ls_testing
                )
            GROUP BY mmsi
            HAVING COUNT(*) > 1
            ORDER BY mmsi;
        """)
    rows = cur.fetchall()
    return [row[0] for row in rows]

def get_points_for_mmsi(cur: Cursor, mmsi: int) -> list[AISPoint]:
    """Fetch all points for a specific MMSI from the database ordered by time."""
    cur.execute("""
            SELECT mmsi, ST_AsBinary(geom), sog
            FROM prototype1.points
            WHERE mmsi = %s
            ORDER BY ST_M(geom);
        """, (mmsi,))
    rows = cur.fetchall()
    
    return [
        (int(row[0]), bytes(row[1]), float(row[2]) if row[2] is not None else None)
        for row in rows 
        if row[0] is not None and row[1] is not None
    ]
