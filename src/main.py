from connect import connect_to_db
from dotenv import load_dotenv
import shapely
from shapely import LineString

def main():
    load_dotenv()
    
    connection = connect_to_db() 
    
    cur = connection.cursor()
    
    # Retrieve "long" trajectories from Materialized View in DW
    cur.execute("""--sql
                SELECT traj FROM linestring_test.trajectories;
                """)
    trajectories = cur.fetchall()
    
    print(trajectories)
    # [(BINARY_ENCODING,), (BINARY_ENCODING,), (BINARY_ENCODING,)]
    
    for (traj,) in trajectories:
        geom : LineString = shapely.from_wkb(traj)  # decode into Shapely geometry
        print(geom)            # -> LINESTRING (...)
        print(geom.geom_type)  # -> 'LineString'
        print(list(geom.coords))
    
    
    # Create new tables: 
    # LineString:   ls_trajectory, ls_stop
    # SOC:          soc_trajectory, soc_stop
    
    
    # Split long trajectories into ls_trajectories and ls_stops + soc format
    
    
    # Upload to DB
    
if __name__ == "__main__":
    main()