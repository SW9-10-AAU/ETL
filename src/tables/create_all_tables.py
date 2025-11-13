from psycopg import Connection
from tables.create_ls_traj_stop_tables import create_ls_traj_stop_tables
from tables.create_cs_traj_stop_tables import create_cs_traj_stop_tables
from tables.create_supercover_cs_traj_stop_tables import create_supercover_cs_traj_stop_tables
from tables.mat_points_view import mat_points_view
from tables.create_area_tables import create_area_tables

def create_all_tables(conn: Connection):
    
    # Create LineString/Polygon tables Trajectory and Stop
    #create_ls_traj_stop_tables(conn)

    # Create CellString tables Trajectory and Stop
    #create_cs_traj_stop_tables(conn)
    create_supercover_cs_traj_stop_tables(conn)
    # Create Materialized View POINTS
    # mat_points_view(connection)
    
    # Create Area Polygon/CellString tables
    # create_area_tables(conn)
    
    print("Created all tables and materialized views if not exist.")
