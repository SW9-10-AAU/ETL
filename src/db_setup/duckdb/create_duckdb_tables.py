import duckdb

from db_setup.duckdb.create_duckdb_points import create_duckdb_points

def create_duckdb_tables(conn: duckdb.DuckDBPyConnection, db_schema: str):
    conn.execute("INSTALL spatial; LOAD spatial;")

    # create schema if not exists
    conn.execute(f"""CREATE SCHEMA IF NOT EXISTS {db_schema};""")
    print(f"Ensured duckDB schema {db_schema} exists.")

    # Create the points table first
    create_duckdb_points(conn, db_schema)

    # trajectory_ls
    conn.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS {db_schema}.trajectory_ls_seq START 1;
        CREATE TABLE IF NOT EXISTS {db_schema}.trajectory_ls (
            trajectory_id INTEGER PRIMARY KEY DEFAULT nextval('{db_schema}.trajectory_ls_seq'),
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            geom          GEOMETRY NOT NULL
        );
    """)

    # stop_poly
    conn.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS {db_schema}.stop_poly_seq START 1;
        CREATE TABLE IF NOT EXISTS {db_schema}.stop_poly (
            stop_id  INTEGER PRIMARY KEY DEFAULT nextval('{db_schema}.stop_poly_seq'),
            mmsi     BIGINT NOT NULL,
            ts_start TIMESTAMP NOT NULL,
            ts_end   TIMESTAMP NOT NULL,
            geom     POLYGON_2D NOT NULL
        );
    """)

    # trajectory_cs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.trajectory_cs (
            trajectory_id BIGINT PRIMARY KEY,
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            cellstring_z13 INTEGER[] NOT NULL,
            cellstring_z17 BIGINT[]  NOT NULL,
            cellstring_z21 BIGINT[]  NOT NULL
        );
    """)

    # stop_cs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {db_schema}.stop_cs (
            stop_id    BIGINT PRIMARY KEY,
            mmsi       BIGINT NOT NULL,
            ts_start   TIMESTAMP NOT NULL,
            ts_end     TIMESTAMP NOT NULL,
            cellstring_z13 INTEGER[] NOT NULL,
            cellstring_z17 BIGINT[]  NOT NULL,
            cellstring_z21 BIGINT[]  NOT NULL
        );
    """)

    print(f"Created all DuckDB tables in schema '{db_schema}'.")
