from psycopg import Connection
from create_crossing_tables import create_crossing_tables
from create_ls_traj_stop_tables import create_ls_traj_stop_tables
from create_cs_traj_stop_tables import create_cs_traj_stop_tables
from mat_points_view import mat_points_view
from create_area_tables import create_area_tables

def create_postgresql_tables(conn: Connection, db_schema: str):
    cur = conn.cursor()
    
    # Create DB schema
    cur.execute(f"""CREATE SCHEMA IF NOT EXISTS {db_schema};""")
    
    print(f"Ensured database schema {db_schema} exists.")
    
    # Create Point Materialized View
    mat_points_view(conn, db_schema)
    
    # Create LineString/Polygon tables Trajectory and Stop
    create_ls_traj_stop_tables(conn, db_schema)

    # Create CellString tables Trajectory and Stop
    create_cs_traj_stop_tables(conn, db_schema)
    
    # Create (area and crossing) tables
    create_area_tables(conn, db_schema)
    create_crossing_tables(conn, db_schema)
    
    print(f"Created all tables and materialized views if not exist in database schema {db_schema}.")
