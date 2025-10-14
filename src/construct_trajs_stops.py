from psycopg import Connection, Cursor
from shapely import Polygon, from_wkb, Point, LineString, MultiPoint
from geopy.distance import geodesic

# Threshold constants matching the paper but with adjustmenst
STOP_SOG_THRESHOLD = 1.0            # knots, vT (original = 1 knot)
STOP_DISTANCE_THRESHOLD = 250       # meters, disT (original = 2 km)
STOP_TIME_THRESHOLD = 5400          # seconds, tT (original = 1.5 h)
MIN_STOP_POINTS = 10                # Δn
MIN_STOP_DURATION = 5400            # seconds, Δstopt (original = 1.5 h)
MERGE_DISTANCE_THRESHOLD = 250      # meters, Δd. (original = 2 km)
MERGE_TIME_THRESHOLD = 3600         # seconds, Δt (original = 1 h)

def distance_m(p1: Point, p2: Point) -> float:
    """Return distance between two Shapely Points in meters."""
    return geodesic((p1.y, p1.x), (p2.y, p2.x)).meters # x and y is swapped because this is (lat,lon), and Shapely/PostGIS is (lon,lat)

def construct_trajectories_and_stops(conn: Connection):
    cur = conn.cursor()
    point_rows = get_points(cur)
    
    # group by MMSI
    by_mmsi: dict[int, list[tuple[Point, float]]] = {}
    for (mmsi, geom, sog) in point_rows:
        point = Point(from_wkb(geom)) # decode into Shapely Point
        sog = float(sog)
        by_mmsi.setdefault(mmsi, []).append((point, sog))

    for mmsi, points in by_mmsi.items():
        traj : list[Point] = []
        stop : list[Point] = []
        prev_point = None
        trajs : list[list[Point]] = []
        candidate_stops : list[list[Point]] = []
        case_count: list[int] = [0,0,0,0]
        num_stops: int = 0

        for point, sog in points:
            t = point.coords[0][2] # epoch time
            if prev_point is None:
                traj = [point]
                prev_point = point
                continue
            
            dt = t - prev_point.coords[0][2]
            dist = distance_m(prev_point, point)

            # Candidate stop condition
            if sog < STOP_SOG_THRESHOLD and dt < STOP_TIME_THRESHOLD and dist < STOP_DISTANCE_THRESHOLD:
                stop.append(prev_point)
                stop.append(point)
                
                # finish trajectory
                if len(traj) > 1:
                    trajs.append(traj)
                    # insert_trajectory(cur, mmsi, traj[0].coords[0][2], traj[-1].coords[0][2], LineString(traj))
                    traj = []
            else:
                traj.append(prev_point)
                traj.append(point)
                
                # finish candidate stop
                if len(stop) >= MIN_STOP_POINTS:
                    candidate_stops.append(stop)
                    stop = []

            prev_point = point

        # flush last trajectory
        if len(traj) > 1:
            trajs.append(traj)
            # insert_trajectory(cur, mmsi, traj[0].coords[0][2], traj[-1].coords[0][2], LineString(traj))
        # flush last stop
        if len(stop) >= MIN_STOP_POINTS:
            candidate_stops.append(stop)

        # Merge nearby candidate stops
        merged_stops : list[list[Point]] = []
        if candidate_stops:
            merged = candidate_stops[0]
            for candidate_stop in candidate_stops[1:]:
                # distance between centers
                center_prev = MultiPoint(merged).centroid
                center_curr = MultiPoint(candidate_stop).centroid
                time_gap = candidate_stop[0].coords[0][2] - merged[-1].coords[0][2]
                if distance_m(center_prev, center_curr) < MERGE_DISTANCE_THRESHOLD and time_gap < MERGE_TIME_THRESHOLD:
                    merged.extend(candidate_stop)
                else:
                    merged_stops.append(merged)
                    merged = candidate_stop
            merged_stops.append(merged)

        # Insert final stops with duration check
        for merged_stop in merged_stops:
            ts_start = merged_stop[0].coords[0][2]
            ts_end   = merged_stop[-1].coords[0][2]
            if len(merged_stop) >= MIN_STOP_POINTS and (ts_end - ts_start) >= MIN_STOP_DURATION:
                num_stops += 1
                insert_stop(cur, mmsi, ts_start, ts_end, MultiPoint(merged_stop).convex_hull.buffer(0))
            else:
                # insert_trajectory(cur, mmsi, ts_start, ts_end, LineString(merged_stop))
                # print(f"Inserted 'slow' trajectory (MMSI: {mmsi})")
                insert_or_merge_with_trajectories(trajs, merged_stop, case_count)
        
        # Insert trajectories
        for trajectory in trajs:
            ts_start = trajectory[0].coords[0][2]
            ts_end   = trajectory[-1].coords[0][2]
            insert_trajectory(cur, mmsi, ts_start, ts_end, LineString(trajectory))
        
        print(f"MMSI {mmsi}: Inserted {len(trajs)} trajectories, {num_stops} stops, cases: {case_count} merges of non-valid stops")
                
    conn.commit()
    cur.close()

def insert_or_merge_with_trajectories(trajs: list[list[Point]], stop: list[Point], case_count: list[int]):
    """Insert or merge a non-valid stop with existing trajectories."""
    if not stop or not trajs:
        trajs.append(stop)
        return

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
            INSERT INTO ls_experiment.trajectory_ls_new (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_Force3DM(ST_GeomFromWKB(%s, 4326)))
        """, (mmsi, ts_start, ts_end, line.wkb))

def insert_stop(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, poly: Polygon):
    cur.execute("""
            INSERT INTO ls_experiment.stop_poly_new (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
        """, (mmsi, ts_start, ts_end, poly.wkb))

def get_points(cur: Cursor) -> list:
    # Retrieve points from Materialized View in DW
    cur.execute("""
            SELECT mmsi, ST_AsBinary(geom), sog
            FROM ls_experiment.points
            ORDER BY mmsi, ST_M(geom);
        """)
    return cur.fetchall()
