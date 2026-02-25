from psycopg import Connection, sql

def create_area_tables(conn: Connection, db_schema: str):
    cur = conn.cursor()
    
    cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {db_schema}.area_poly
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(GEOMETRY, 4326) NOT NULL
            );
        """).format(db_schema=sql.Identifier(db_schema)))

    cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {db_schema}.area_cs
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                cellstring_z13 int ARRAY NOT NULL,
                cellstring_z17 bigint ARRAY NOT NULL,
                cellstring_z21 bigint ARRAY NOT NULL
            );
        """).format(db_schema=sql.Identifier(db_schema)))

    # Create indexes
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS area_poly_geom_idx
            ON {db_schema}.area_poly 
            USING GIST (geom);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS area_cs_z13_gin_idx
            ON {db_schema}.area_cs
            USING GIN (cellstring_z13 gin__int_ops);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS area_cs_z17_gin_idx
            ON {db_schema}.area_cs 
            USING GIN (cellstring_z17);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS area_cs_z21_gin_idx
            ON {db_schema}.area_cs 
            USING GIN (cellstring_z21);
        """).format(db_schema=sql.Identifier(db_schema)))
    conn.commit()
    
    print(f"Ensured area tables exist in database schema {db_schema}.")    