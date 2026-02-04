from psycopg import Connection

def create_area_tables(conn: Connection):
    cur = conn.cursor()
    
     # Create benchmark schema and tables if not exist
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS benchmark;
        """)
    # Check if table exists with old POLYGON constraint and migrate if needed
    cur.execute("""
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = 'benchmark'
            AND table_name = 'area_poly'
            AND column_name = 'geom';
        """)
    result = cur.fetchone()

    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.area_poly
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(GEOMETRY, 4326) NOT NULL
            );
        """)

    # Migrate existing table if it has POLYGON constraint
    if result and 'geometry' in str(result):
        cur.execute("""
                ALTER TABLE benchmark.area_poly
                ALTER COLUMN geom TYPE geometry(GEOMETRY, 4326);
            """)
        print("Migrated area_poly.geom to support GEOMETRY type (Polygon, MultiPolygon, etc.)")
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.area_cs
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                cellstring_z13 int ARRAY NOT NULL,
                cellstring_z17 bigint ARRAY NOT NULL,
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
            USING GIN (cellstring_z13 gin__int_ops);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_cs_z17_gin_idx
            ON benchmark.area_cs 
            USING GIN (cellstring_z17);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_cs_z21_gin_idx
            ON benchmark.area_cs 
            USING GIN (cellstring_z21);
        """)
    conn.commit()
    print("Ensured benchmark schema and tables exist in database.")    