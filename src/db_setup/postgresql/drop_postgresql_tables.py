from psycopg import Connection, sql


def drop_postgresql_tables(
    conn: Connection,
    ls_schema: str,
    cs_schema: str,
    drop_ls_tables: bool = True,
    drop_cs_tables: bool = True,
):
    cur = conn.cursor()

    if drop_ls_tables:
        cur.execute(
            sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {db_schema}.points;").format(
                db_schema=sql.Identifier(ls_schema)
            )
        )

        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.trajectory_ls;").format(
                db_schema=sql.Identifier(ls_schema)
            )
        )
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.stop_poly;").format(
                db_schema=sql.Identifier(ls_schema)
            )
        )

        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.area_poly;").format(
                db_schema=sql.Identifier(ls_schema)
            )
        )

        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.crossing_ls;").format(
                db_schema=sql.Identifier(ls_schema)
            )
        )

        print(
            f"Dropped LineString tables and materialized view in PostgreSQL schema '{ls_schema}'."
        )

    if drop_cs_tables:
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.trajectory_cs;").format(
                db_schema=sql.Identifier(cs_schema)
            )
        )
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.stop_cs;").format(
                db_schema=sql.Identifier(cs_schema)
            )
        )
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.area_cs;").format(
                db_schema=sql.Identifier(cs_schema)
            )
        )
        cur.execute(
            sql.SQL("DROP TABLE IF EXISTS {db_schema}.crossing_cs;").format(
                db_schema=sql.Identifier(cs_schema)
            )
        )
        print(f"Dropped CellString tables in PostgreSQL schema '{cs_schema}'.")

    conn.commit()
    cur.close()
