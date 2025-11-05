from psycopg import Connection

def create_area_tables(conn: Connection):
    cur = conn.cursor()
    
     # Create benchmark schema and tables if not exist
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS benchmark;
        """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.area_poly
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(POLYGON, 4326) NOT NULL
            );
        """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.area_cs
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                cellstring_z13 int ARRAY NOT NULL,
                cellstring_z21 bigint ARRAY NOT NULL
            );
        """)

    # Create indexes
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_poly_geom_idx
            ON benchmark.area_poly 
            USING GIST (geom);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_cs_z13_gin_idx
            ON benchmark.area_cs
            USING GIN (cellstring_z13);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_cs_z21_gin_idx
            ON benchmark.area_cs 
            USING GIN (cellstring_z21);
        """)
    conn.commit()
    print("Ensured benchmark schema and tables exist in database.")    