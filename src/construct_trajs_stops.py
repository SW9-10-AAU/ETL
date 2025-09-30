from psycopg import Connection, Cursor
from shapely import convex_hull, from_wkb, Point, LineString, Polygon, MultiPoint
from geopy.distance import geodesic

def distance_m(p1: Point, p2: Point) -> float:
    """Return distance between two Shapely Points in meters."""
    # x and y is swapped because this is (lat,lon), and Shapely/PostGIS is (lon,lat)
    return geodesic((p1.y, p1.x), (p2.y, p2.x)).meters

def construct_trajectories_and_stops(conn : Connection):
    cur = conn.cursor()
    point_rows = get_points(cur)
    
    # group by MMSI
    by_mmsi: dict[int, list[Point]] = {}
    for (mmsi,geom) in point_rows:
        point : Point = from_wkb(geom) # decode into Shapely geometry
        by_mmsi.setdefault(mmsi, []).append(point)

    for mmsi, points in by_mmsi.items():
        traj, stop = [], []
        prev = None
        
        for p in points:
            t = p.coords[0][2] # epoch time
            if prev is None:
                traj = [p]
                prev = p
                continue
            
            # difference in time/meters from prev point
            dt = t - prev.coords[0][2] 
            dist = distance_m(prev, p)
            
            if dt < 60 and dist < 1000:
                # break stop
                if (len(stop) > 1): # build Polygon if stop consists of at least two points
                    poly = MultiPoint(stop).convex_hull.buffer(0)
                    ts_start = stop[0].coords[0][2]
                    ts_end   = stop[-1].coords[0][2]
                    insert_stop(cur, mmsi, ts_start, ts_end, poly)
                    
                    # reset stop
                    stop = []
                    
                elif (len(stop) == 1):
                    traj.append(stop[0])
                    stop = []
                    
                # build trajectory
                traj.append(p)
            else:
                # break trajectory
                if len(traj) > 1: # build LineString if trajectory consists of at least two points
                    line = LineString(traj)
                    ts_start = traj[0].coords[0][2]
                    ts_end   = traj[-1].coords[0][2]
                    insert_trajectory(cur, mmsi, ts_start, ts_end, line)
                    
                    # reset traj
                    traj = []
                    
                elif (len(traj) == 1):
                    stop.append(traj[0])
                    traj = []
                
                # build stop
                stop.append(p)
                
            prev = p
        
        # flush last trajectory
        if len(traj) > 1:
            line = LineString(traj)
            insert_trajectory(cur, mmsi, traj[0].coords[0][2], traj[-1].coords[0][2], line)
        
    conn.commit()
    cur.close()
    
def insert_trajectory(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, line: LineString):
    cur.execute("""--sql
        INSERT INTO ls_experiment.trajectory_ls (mmsi, ts_start, ts_end, geom)
        VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_Force3DM(ST_GeomFromWKB(%s, 4326)))
    """, (mmsi, ts_start, ts_end, line.wkb))

def insert_stop(cur: Cursor, mmsi: int, ts_start: float, ts_end: float, poly):
    cur.execute("""--sql
        INSERT INTO ls_experiment.stop_poly (mmsi, ts_start, ts_end, geom)
        VALUES (%s, TO_TIMESTAMP(%s), TO_TIMESTAMP(%s), ST_GeomFromWKB(%s, 4326))
    """, (mmsi, ts_start, ts_end, poly.wkb))

def get_points(cur : Cursor) -> list:
    # Retrieve points from Materialized View in DW
    cur.execute("""--sql
                SELECT mmsi, ST_AsBinary(geom) 
                FROM linestring_test.points
                ORDER BY mmsi, ST_M(geom);
                """)
    return cur.fetchall() 
        
    