from psycopg import Connection, sql


def create_crossing_tables(conn: Connection, ls_schema: str, cs_schema: str):
    cur = conn.cursor()

    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {ls_schema}.crossing_ls
            (
                crossing_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(LINESTRING, 4326) NOT NULL
            );
        """
        ).format(ls_schema=sql.Identifier(ls_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {cs_schema}.crossing_cs
            (
                crossing_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                cellstring_z13 int ARRAY NOT NULL,
                cellstring_z17 bigint ARRAY NOT NULL,
                cellstring_z21 bigint ARRAY NOT NULL
            );
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )

    # Create indexes
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS crossing_ls_geom_idx
            ON {ls_schema}.crossing_ls 
            USING GIST (geom);
        """
        ).format(ls_schema=sql.Identifier(ls_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS crossing_cs_z13_gin_idx
            ON {cs_schema}.crossing_cs
            USING GIN (cellstring_z13 gin__int_ops);
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS crossing_cs_z17_gin_idx
            ON {cs_schema}.crossing_cs 
            USING GIN (cellstring_z17);
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS crossing_cs_z21_gin_idx
            ON {cs_schema}.crossing_cs 
            USING GIN (cellstring_z21);
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )
    conn.commit()

    print(
        f"Ensured crossing tables exist with crossing_ls in schema '{ls_schema}' and crossing_cs in schema '{cs_schema}'."
    )
