from psycopg import Connection, Cursor
from shapely import Polygon, from_wkb, Point, LineString, MultiPoint
from geopy.distance import geodesic

# Threshold constants matching the paper but with adjustmenst
STOP_SOG_THRESHOLD = 1.0          # knots, vT
STOP_DISTANCE_THRESHOLD = 2000   # meters, disT
STOP_TIME_THRESHOLD = 5400       # seconds, tT (1.5 h)
MIN_STOP_POINTS = 10             # Δn
MIN_STOP_DURATION = 5400         # seconds, Δstopt (1.5 h)
MERGE_DISTANCE_THRESHOLD = 200  # meters, Δd.  (changed from 2km)
MERGE_TIME_THRESHOLD = 86400     # seconds, Δt (1 d) (changed from 1h)

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
        candidate_stops : list[list[Point]] = []

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
                stop.append(point)
                # finish trajectory
                if len(traj) > 1:
                    insert_trajectory(cur, mmsi, traj[0].coords[0][2], traj[-1].coords[0][2], LineString(traj))
                traj = []
            else:
                # finish candidate stop
                if len(stop) >= MIN_STOP_POINTS:
                    candidate_stops.append(stop)
                stop = []
                # continue trajectory
                traj.append(point)

            prev_point = point

        # flush last trajectory
        if len(traj) > 1:
            insert_trajectory(cur, mmsi, traj[0].coords[0][2], traj[-1].coords[0][2], LineString(traj))
        # flush last stop
        if len(stop) >= MIN_STOP_POINTS:
            candidate_stops.append(stop)

        # Merge nearby candidate stops
        merged_stops : list[list[Point]] = []
        if candidate_stops:
            merged = candidate_stops[0]
            for st in candidate_stops[1:]:
                # distance between centers
                center_prev = MultiPoint(merged).centroid
                center_curr = MultiPoint(st).centroid
                time_gap = st[0].coords[0][2] - merged[-1].coords[0][2]
                if distance_m(center_prev, center_curr) < MERGE_DISTANCE_THRESHOLD and time_gap < MERGE_TIME_THRESHOLD:
                    merged.extend(st)
                else:
                    merged_stops.append(merged)
                    merged = st
            merged_stops.append(merged)

        # Insert final stops with duration check
        for st in merged_stops:
            ts_start = st[0].coords[0][2]
            ts_end   = st[-1].coords[0][2]
            if len(st) >= MIN_STOP_POINTS and (ts_end - ts_start) >= MIN_STOP_DURATION:
                insert_stop(cur, mmsi, ts_start, ts_end, MultiPoint(st).convex_hull.buffer(0))
            else:
                print("Not valid stop") # TODO: Fix this

    conn.commit()
    cur.close()

def insert_trajectory(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, line: LineString):
    cur.execute("""
            INSERT INTO prototype1.trajectory_ls (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_Force3DM(ST_GeomFromWKB(%s, 4326)))
        """, (mmsi, ts_start, ts_end, line.wkb))

def insert_stop(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, poly: Polygon):
    cur.execute("""
            INSERT INTO prototype1.stop_poly (mmsi, ts_start, ts_end, geom)
            VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
        """, (mmsi, ts_start, ts_end, poly.wkb))

def get_points(cur: Cursor) -> list:
    # Retrieve points from Materialized View in DW
    cur.execute("""
            SELECT mmsi, ST_AsBinary(geom), sog
            FROM prototype1.points
            ORDER BY mmsi, ST_M(geom);
        """)
    return cur.fetchall()
