import duckdb


def create_duckdb_schema(conn: duckdb.DuckDBPyConnection, db_schema: str):
    conn.execute(f"""CREATE SCHEMA IF NOT EXISTS {db_schema};""")
    print(f"Ensured DuckDB schema {db_schema} exists.")


def create_duckdb_tables(
    conn: duckdb.DuckDBPyConnection, source_schema: str, cs_schema: str
):

    # trajectory_ls
    conn.execute(
        f"""
        CREATE SEQUENCE IF NOT EXISTS {source_schema}.trajectory_ls_seq START 1;
        CREATE TABLE IF NOT EXISTS {source_schema}.trajectory_ls (
            trajectory_id INTEGER PRIMARY KEY DEFAULT nextval('{source_schema}.trajectory_ls_seq'),
            mmsi          BIGINT NOT NULL,
            ts_start      TIMESTAMP NOT NULL,
            ts_end        TIMESTAMP NOT NULL,
            geom          GEOMETRY NOT NULL
        );
    """
    )

    # stop_poly
    conn.execute(
        f"""
        CREATE SEQUENCE IF NOT EXISTS {source_schema}.stop_poly_seq START 1;
        CREATE TABLE IF NOT EXISTS {source_schema}.stop_poly (
            stop_id  INTEGER PRIMARY KEY DEFAULT nextval('{source_schema}.stop_poly_seq'),
            mmsi     BIGINT NOT NULL,
            ts_start TIMESTAMP NOT NULL,
            ts_end   TIMESTAMP NOT NULL,
            geom     GEOMETRY NOT NULL
        );
    """
    )

    # trajectory_cs
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.trajectory_cs (
            trajectory_id   INTEGER NOT NULL,
            mmsi            BIGINT NOT NULL,
            ts              TIMESTAMP NOT NULL,
            cell_z21        UINT64 NOT NULL,
        );
    """
    )

    # stop_cs
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.stop_cs (
            stop_id    INTEGER NOT NULL,
            mmsi       BIGINT NOT NULL,
            ts_start   TIMESTAMP NOT NULL,
            ts_end     TIMESTAMP NOT NULL,
            cell_z21   UINT64 NOT NULL,
        );
    """
    )

    # area poly
    conn.execute(
        f"""
        CREATE SEQUENCE IF NOT EXISTS {source_schema}.area_poly_seq START 1;
        CREATE TABLE IF NOT EXISTS {source_schema}.area_poly
        (
            area_id      INTEGER PRIMARY KEY DEFAULT nextval('{source_schema}.area_poly_seq'),
            name         TEXT NOT NULL,
            geom         GEOMETRY NOT NULL
        );
    """
    )

    # area cs
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.area_cs
        (
            area_id      INTEGER NOT NULL,
            name         TEXT NOT NULL,
            cell_z21     UINT64 NOT NULL
        );
    """
    )

    # crossing ls
    conn.execute(
        f"""
        CREATE SEQUENCE IF NOT EXISTS {source_schema}.crossing_ls_seq START 1;
        CREATE TABLE IF NOT EXISTS {source_schema}.crossing_ls
        (
            crossing_id  INTEGER PRIMARY KEY DEFAULT nextval('{source_schema}.crossing_ls_seq'),
            name         TEXT NOT NULL,
            geom         GEOMETRY NOT NULL
        );
    """
    )

    # crossing cs
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {cs_schema}.crossing_cs
        (
            crossing_id  INTEGER NOT NULL,
            name         TEXT NOT NULL,
            cell_z21     UINT64 NOT NULL
        );
    """
    )

    print(
        f"Created DuckDB tables (trajectory_ls, stop_poly, area_poly, crossing_ls) in source schema '{source_schema}' and CellString tables (trajectory_cs, stop_cs, area_cs, crossing_cs) in schema '{cs_schema}'."
    )
