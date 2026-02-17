from dotenv import load_dotenv
import os
import sys

def main():
    load_dotenv()
    backend = os.getenv('DB_BACKEND', 'postgres').lower()

    if backend == 'duckdb':
        main_duckdb()
    else:
        main_postgres()

def main_duckdb():
    import duckdb
    from tables.create_duckdb_tables import create_duckdb_tables
    from tables.create_duckdb_points import create_duckdb_points
    from duckdb_construct_trajs_stops import construct_trajectories_and_stops_duckdb
    from duckdb_transform_ls_to_cs import transform_ls_trajectories_to_cs_duckdb, transform_ls_stops_to_cs_duckdb

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.getenv('DUCKDB_PATH', os.path.join(project_root, 'ais_data', 'ais_db.duckdb'))
    conn = duckdb.connect(database=db_path)

    # Create points table from raw AIS data
    # create_duckdb_points(conn)

    # Create output tables (trajectory_ls, stop_poly, *_cs)
    create_duckdb_tables(conn)

    # Construct Trajectories and Stops from the Points table
    construct_trajectories_and_stops_duckdb(conn, min(os.cpu_count() or 4, 12))

    # Transform LS Trajectories to CS Trajectories
    transform_ls_trajectories_to_cs_duckdb(conn, min(os.cpu_count() or 4, 12), batch_size=10)
    transform_ls_stops_to_cs_duckdb(conn, min(os.cpu_count() or 4, 12), batch_size=10)

    conn.close()

def main_postgres():
    from connect import connect_to_db
    from tables.create_all_tables import create_all_tables
    from tables.drop_all_tables import drop_all_tables
    from construct_trajs_stops import construct_trajectories_and_stops
    from transform_ls_to_cs import transform_ls_trajectories_to_cs, transform_ls_stops_to_cs

    db_url = os.getenv('DATABASE_URL')

    if not db_url:
        sys.exit("DATABASE_URL not defined in .env file")

    connection = connect_to_db()

    # Drop existing tables and views
    # drop_all_tables(connection)

    # Create all necessary tables and Materialized view
    create_all_tables(connection)

    # Construct Trajectories and Stops from the Points Materialized View
    # construct_trajectories_and_stops(connection, min(os.cpu_count() or 4, 12))

    # Transform LS Trajectories to CS Trajectories
    # transform_ls_trajectories_to_cs(connection, min(os.cpu_count() or 4, 12), batch_size=1000)
    # transform_ls_trajectories_to_cs(connection, min(os.cpu_count() or 4, 12), batch_size=1000, use_supercover=True)
    # transform_ls_stops_to_cs(connection, min(os.cpu_count() or 4, 12), batch_size=1000)

    connection.close()

if __name__ == "__main__":
    main()
