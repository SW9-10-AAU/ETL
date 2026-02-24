import os

from db_setup.utils.db_utils import get_db_backend, get_db_path, get_db_schema

def main():
    backend = get_db_backend()

    if backend == 'duckdb':
        main_duckdb()
    elif backend == 'postgresql':
        main_postgres()
    else:
        raise ValueError(f"Unsupported database backend: {backend}")

def main_duckdb():
    import duckdb
    from db_setup.duckdb.create_duckdb_tables import create_duckdb_tables
    from duckdb_construct_trajs_stops import construct_trajectories_and_stops_duckdb
    from duckdb_transform_ls_to_cs import transform_ls_trajectories_to_cs, transform_poly_stops_to_cs

    db_path = get_db_path('duckdb')
    db_schema = get_db_schema('duckdb')
    connection = duckdb.connect(database=db_path)

    # Create tables 
    create_duckdb_tables(connection, db_schema)

    # Construct LineString trajectories and Polygon stops from the Points table
    construct_trajectories_and_stops_duckdb(connection, min(os.cpu_count() or 4, 12))

    # Transform LineString trajectories and Polygon stops to CellStrings
    transform_ls_trajectories_to_cs(connection, min(os.cpu_count() or 4, 12), batch_size=100)
    transform_poly_stops_to_cs(connection, min(os.cpu_count() or 4, 12), batch_size=100)

    connection.close()

def main_postgres():
    from db_setup.utils.connect import connect_to_postgres_db
    from db_setup.postgresql.create_postgresql_tables import create_postgresql_tables
    from db_setup.postgresql.drop_postgresql_tables import drop_postgresql_tables
    from pg_construct_trajs_stops import construct_trajectories_and_stops
    from pg_transform_ls_to_cs import transform_ls_trajectories_to_cs, transform_poly_stops_to_cs

    db_schema = os.getenv('POSTGRESQL_SCHEMA')
    connection = connect_to_postgres_db()

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
