from duckdb import DuckDBPyConnection


def drop_duckdb_tables(
    conn: DuckDBPyConnection,
    source_schema: str,
    cs_schema: str,
    drop_source_tables: bool = True,
    drop_cs_tables: bool = True,
):
    cur = conn.cursor()

    if drop_source_tables:
        cur.execute(f"DROP TABLE IF EXISTS {source_schema}.points;")

        cur.execute(f"DROP TABLE IF EXISTS {source_schema}.trajectory_ls;")
        cur.execute(f"DROP TABLE IF EXISTS {source_schema}.stop_poly;")

        cur.execute(f"DROP SEQUENCE IF EXISTS {source_schema}.trajectory_ls_seq;")
        cur.execute(f"DROP SEQUENCE IF EXISTS {source_schema}.stop_poly_seq;")

        cur.execute(f"DROP TABLE IF EXISTS {source_schema}.area_poly;")
        cur.execute(f"DROP SEQUENCE IF EXISTS {source_schema}.area_poly_seq;")

        cur.execute(f"DROP TABLE IF EXISTS {source_schema}.crossing_ls;")
        cur.execute(f"DROP SEQUENCE IF EXISTS {source_schema}.crossing_ls_seq;")

        print(
            f"Dropped source/LS tables and sequences in DuckDB schema '{source_schema}'."
        )

    if drop_cs_tables:
        cur.execute(f"DROP TABLE IF EXISTS {cs_schema}.trajectory_cs;")
        cur.execute(f"DROP TABLE IF EXISTS {cs_schema}.stop_cs;")
        cur.execute(f"DROP TABLE IF EXISTS {cs_schema}.area_cs;")
        cur.execute(f"DROP TABLE IF EXISTS {cs_schema}.crossing_cs;")
        print(f"Dropped CellString tables in DuckDB schema '{cs_schema}'.")

    conn.commit()
    cur.close()
    cur.close()
