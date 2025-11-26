from psycopg import Connection

def create_crossing_tables(conn: Connection):
    cur = conn.cursor()
    
     # Create benchmark schema and tables if not exist
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS benchmark;
        """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.crossing_ls
            (
                crossing_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(LINESTRING, 4326) NOT NULL
            );
        """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.crossing_cs
            (
                crossing_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                cellstring_z13 int ARRAY NOT NULL,
                cellstring_z17 bigint ARRAY NOT NULL,
                cellstring_z21 bigint ARRAY NOT NULL
            );
        """)

    # Create indexes
    cur.execute("""
            CREATE INDEX IF NOT EXISTS crossing_ls_geom_idx
            ON benchmark.crossing_ls 
            USING GIST (geom);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS crossing_cs_z13_gin_idx
            ON benchmark.crossing_cs
            USING GIN (cellstring_z13 gin__int_ops);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS crossing_cs_z17_gin_idx
            ON benchmark.crossing_cs 
            USING GIN (cellstring_z17);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS crossing_cs_z21_gin_idx
            ON benchmark.crossing_cs 
            USING GIN (cellstring_z21);
        """)
    conn.commit()
    print("Ensured benchmark schema and tables exist in database.")    