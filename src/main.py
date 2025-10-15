from dotenv import load_dotenv
import os
import sys
from connect import connect_to_db
from tables.drop_all_tables import drop_all_tables
from tables.create_ls_traj_stop_tables import create_ls_traj_stop_tables
from tables.create_cs_traj_stop_tables import create_cs_traj_stop_tables
from tables.mat_points_view import mat_points_view
from construct_trajs_stops import construct_trajectories_and_stops
from transform_ls_to_cs import transform_ls_trajectories_to_cs, transform_ls_stops_to_cs

def main():
    load_dotenv()
    db_url = os.getenv('DATABASE_URL')
    
    if not db_url:
        sys.exit("DATABASE_URL not defined in .env file")
    
    connection = connect_to_db()

    # Drop existing tables and views
    # drop_all_tables(connection)

    # Create LineString/Polygon tables Trajectory and Stop
    create_ls_traj_stop_tables(connection)

    # Create CellString tables Trajectory and Stop
    create_cs_traj_stop_tables(connection)

    # Create Materialized View POINTS
    # mat_points_view(connection)
    
    # Construct Trajectories and Stops from the Points Materialized View 
    construct_trajectories_and_stops(connection, db_url, min(os.cpu_count() or 4, 12))

    # Transform LS Trajectories to CS Trajectories
    #transform_ls_trajectories_to_cs(connection)
    #transform_ls_stops_to_cs(connection)
    
    connection.close()
     
if __name__ == "__main__":
    main()