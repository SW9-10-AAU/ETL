import duckdb


def create_duckdb_schema(conn: duckdb.DuckDBPyConnection, db_schema: str):
    conn.execute(f"""CREATE SCHEMA IF NOT EXISTS {db_schema};""")
    print(f"Ensured DuckDB schema {db_schema} exists.")


def create_duckdb_tables(
    conn: duckdb.DuckDBPyConnection, ls_schema: str, cs_schema: str
):

    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {ls_schema}.points_ingestion_log (
            file_name TEXT PRIMARY KEY,
            file_path TEXT NOT NULL,
            file_date DATE NOT NULL,
            min_ts TIMESTAMP,
            max_ts TIMESTAMP,
            loaded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """)

    # trajectory_ls
    conn.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS {ls_schema}.trajectory_ls_seq START 1;
        CREATE TABLE IF NOT EXISTS {ls_schema}.trajectory_ls (
            trajectory_id INTEGER PRIMARY KEY DEFAULT nextval('{ls_schema}.trajectory_ls_seq'),
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            geom          GEOMETRY NOT NULL
        );
    """)

    # stop_poly
    conn.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS {ls_schema}.stop_poly_seq START 1;
        CREATE TABLE IF NOT EXISTS {ls_schema}.stop_poly (
            stop_id  INTEGER PRIMARY KEY DEFAULT nextval('{ls_schema}.stop_poly_seq'),
            mmsi     BIGINT NOT NULL,
            ts_start TIMESTAMP NOT NULL,
            ts_end   TIMESTAMP NOT NULL,
            geom     GEOMETRY NOT NULL
        );
    """)

    # trajectory_cs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.trajectory_cs (
            trajectory_id   INTEGER NOT NULL,
            mmsi            BIGINT NOT NULL,
            ts_entry        TIMESTAMP NOT NULL,
            ts_exit         TIMESTAMP NOT NULL,
            cell_z21        UINT64 NOT NULL
        );
    """)

    # stop_cs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.stop_cs (
            stop_id    INTEGER NOT NULL,
            mmsi       BIGINT NOT NULL,
            ts_start   TIMESTAMP NOT NULL,
            ts_end     TIMESTAMP NOT NULL,
            cell_z21   UINT64 NOT NULL,
        );
    """)

    # region poly
    conn.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS {ls_schema}.region_poly_seq START 1;
        CREATE TABLE IF NOT EXISTS {ls_schema}.region_poly
        (
            region_id      INTEGER PRIMARY KEY DEFAULT nextval('{ls_schema}.region_poly_seq'),
            name         TEXT NOT NULL,
            geom         GEOMETRY NOT NULL
        );
    """)

    # region cs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.region_cs
        (
            region_id      INTEGER NOT NULL,
            name         TEXT NOT NULL,
            cell_z21     UINT64 NOT NULL
        );
    """)

    # passage ls
    conn.execute(f"""
        CREATE SEQUENCE IF NOT EXISTS {ls_schema}.passage_ls_seq START 1;
        CREATE TABLE IF NOT EXISTS {ls_schema}.passage_ls
        (
            passage_id  INTEGER PRIMARY KEY DEFAULT nextval('{ls_schema}.passage_ls_seq'),
            name         TEXT NOT NULL,
            geom         GEOMETRY NOT NULL
        );
    """)

    # passage cs
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.passage_cs
        (
            passage_id  INTEGER NOT NULL,
            name         TEXT NOT NULL,
            cell_z21     UINT64 NOT NULL
        );
    """)

    # Spatial indexes on geometry columns
    conn.execute(f"""
        CREATE INDEX IF NOT EXISTS trajectory_ls_geom_rtree_idx
        ON {ls_schema}.trajectory_ls USING RTREE (geom);

        CREATE INDEX IF NOT EXISTS stop_poly_geom_rtree_idx
        ON {ls_schema}.stop_poly USING RTREE (geom);

        CREATE INDEX IF NOT EXISTS region_poly_geom_rtree_idx
        ON {ls_schema}.region_poly USING RTREE (geom);

        CREATE INDEX IF NOT EXISTS passage_ls_geom_rtree_idx
        ON {ls_schema}.passage_ls USING RTREE (geom);
    """)

    print(
        f"Created DuckDB tables (trajectory_ls, stop_poly, region_poly, passage_ls) in schema '{ls_schema}' and CellString tables (trajectory_cs, stop_cs, region_cs, passage_cs) in schema '{cs_schema}'."
    )
