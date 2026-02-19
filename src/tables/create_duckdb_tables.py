import duckdb


def create_duckdb_tables(conn: duckdb.DuckDBPyConnection):
    conn.execute("INSTALL spatial; LOAD spatial;")

    # trajectory_ls
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS trajectory_ls_seq START 1;
        CREATE TABLE IF NOT EXISTS trajectory_ls (
            trajectory_id INTEGER PRIMARY KEY DEFAULT nextval('trajectory_ls_seq'),
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            geom          GEOMETRY NOT NULL
        );
    """)

    # stop_poly
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS stop_poly_seq START 1;
        CREATE TABLE IF NOT EXISTS stop_poly (
            stop_id  INTEGER PRIMARY KEY DEFAULT nextval('stop_poly_seq'),
            mmsi     BIGINT NOT NULL,
            ts_start TIMESTAMP NOT NULL,
            ts_end   TIMESTAMP NOT NULL,
            geom     POLYGON_2D NOT NULL
        );
    """)

    # trajectory_cs
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trajectory_cs (
            trajectory_id BIGINT PRIMARY KEY,
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            unique_cells  BOOLEAN DEFAULT FALSE,
            cellstring_z13 INTEGER[] NOT NULL,
            cellstring_z17 BIGINT[]  NOT NULL,
            cellstring_z21 BIGINT[]  NOT NULL
        );
    """)

    # stop_cs
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stop_cs (
            stop_id    BIGINT PRIMARY KEY,
            mmsi       BIGINT NOT NULL,
            ts_start   TIMESTAMP NOT NULL,
            ts_end     TIMESTAMP NOT NULL,
            unique_cells BOOLEAN DEFAULT TRUE,
            cellstring_z13 INTEGER[] NOT NULL,
            cellstring_z17 BIGINT[]  NOT NULL,
            cellstring_z21 BIGINT[]  NOT NULL
        );
    """)

    # trajectory_supercover_cs
    conn.execute("""
        CREATE TABLE IF NOT EXISTS trajectory_supercover_cs (
            trajectory_id BIGINT PRIMARY KEY,
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            unique_cells  BOOLEAN DEFAULT FALSE,
            cellstring_z13 INTEGER[] NOT NULL,
            cellstring_z17 BIGINT[]  NOT NULL,
            cellstring_z21 BIGINT[]  NOT NULL
        );
    """)

    print("Created all DuckDB output tables.")
