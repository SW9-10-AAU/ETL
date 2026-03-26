from psycopg import Connection, sql


def create_area_tables(conn: Connection, source_schema: str, cs_schema: str):
    cur = conn.cursor()

    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {source_schema}.area_poly
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(GEOMETRY, 4326) NOT NULL
            );
        """
        ).format(source_schema=sql.Identifier(source_schema))
    )

    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {cs_schema}.area_cs
            (
                area_id INTEGER PRIMARY KEY,
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
            CREATE INDEX IF NOT EXISTS area_poly_geom_idx
            ON {source_schema}.area_poly 
            USING GIST (geom);
        """
        ).format(source_schema=sql.Identifier(source_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS area_cs_z13_gin_idx
            ON {cs_schema}.area_cs
            USING GIN (cellstring_z13 gin__int_ops);
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS area_cs_z17_gin_idx
            ON {cs_schema}.area_cs 
            USING GIN (cellstring_z17);
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS area_cs_z21_gin_idx
            ON {cs_schema}.area_cs 
            USING GIN (cellstring_z21);
        """
        ).format(cs_schema=sql.Identifier(cs_schema))
    )
    conn.commit()

    print(
        f"Ensured area tables exist with area_poly in source schema '{source_schema}' and area_cs in schema '{cs_schema}'."
    )
