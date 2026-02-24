from psycopg import Connection, sql
from db_setup.postgresql.create_area_tables import create_area_tables
from db_setup.postgresql.create_crossing_tables import create_crossing_tables
from db_setup.postgresql.create_cs_traj_stop_tables import create_cs_traj_stop_tables
from db_setup.postgresql.create_ls_traj_stop_tables import create_ls_traj_stop_tables
from db_setup.postgresql.mat_points_view import mat_points_view

def create_postgresql_tables(conn: Connection, db_schema: str):
    
    # Create DB schema
    cur = conn.cursor()
    cur.execute(sql.SQL("""CREATE SCHEMA IF NOT EXISTS {db_schema};""").format(db_schema=sql.Identifier(db_schema)))
    conn.commit()
    cur.close()
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
