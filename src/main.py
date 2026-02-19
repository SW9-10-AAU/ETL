from dotenv import load_dotenv
import os
import sys

def main():
    load_dotenv()
    backend = os.getenv('DB_BACKEND').lower()

    if backend == 'duckdb':
        main_duckdb()
    elif backend == 'postgres':
        main_postgres()
    else: 
        sys.exit(f"Unsupported DB_BACKEND: {backend}. Please set DB_BACKEND to 'duckdb' or 'postgres' in the .env file.")

def main_duckdb():
    import duckdb
    from db_setup.duckdb.create_duckdb_tables import create_duckdb_tables
    from db_setup.duckdb.create_duckdb_points import create_duckdb_points
    from duckdb_construct_trajs_stops import construct_trajectories_and_stops_duckdb
    from duckdb_transform_ls_to_cs import transform_ls_trajectories_to_cs_duckdb, transform_poly_stops_to_cs_duckdb

    db_path = os.getenv('DUCKDB_PATH')
    db_schema = os.getenv('DUCKDB_SCHEMA')
    if not db_path:
        sys.exit("DUCKDB_PATH not defined in .env file")
        
    connection = duckdb.connect(database=db_path)

    # Create tables 
    create_duckdb_tables(connection, db_schema)

    # Construct LineString trajectories and Polygon stops from the Points table
    construct_trajectories_and_stops_duckdb(connection, min(os.cpu_count() or 4, 12))

    # Transform LineString trajectories and Polygon stops to CellStrings
    transform_ls_trajectories_to_cs_duckdb(connection, min(os.cpu_count() or 4, 12), batch_size=100)
    transform_poly_stops_to_cs_duckdb(connection, min(os.cpu_count() or 4, 12), batch_size=100)

    connection.close()

def main_postgres():
    from connect import connect_to_db
    from db_setup.postgresql.create_postgresql_tables import create_postgresql_tables
    from db_setup.postgresql.drop_postgresql_tables import drop_postgresql_tables
    from construct_trajs_stops import construct_trajectories_and_stops
    from transform_ls_to_cs import transform_ls_trajectories_to_cs, transform_poly_stops_to_cs

    db_url = os.getenv('DATABASE_URL')
    db_schema = os.getenv('POSTGRESQL_SCHEMA')

    if not db_url:
        sys.exit("DATABASE_URL not defined in .env file")

    connection = connect_to_db()

    # Drop existing tables and views
    drop_postgresql_tables(connection, db_schema)

    # Create all necessary tables and Materialized view
    create_postgresql_tables(connection, db_schema)
    
    # Construct Trajectories and Stops from the Points Materialized View 
    construct_trajectories_and_stops(connection, db_schema, min(os.cpu_count() or 4, 12))

    # Transform LS Trajectories to CS Trajectories
    transform_ls_trajectories_to_cs(connection, db_schema, min(os.cpu_count() or 4, 12), batch_size=1000)
    transform_poly_stops_to_cs(connection, db_schema, min(os.cpu_count() or 4, 12), batch_size=1000)
    
    connection.close()

if __name__ == "__main__":
    main()
