from psycopg import Connection

def create_ls_traj_stop_tables(conn: Connection):
    cur = conn.cursor()

    # Schema
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS prototype2;
        """)

    # Trajectory table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS prototype2.trajectory_ls_testing (
                trajectory_id SERIAL PRIMARY KEY,
                mmsi          BIGINT                      NOT NULL,
                ts_start      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                geom          geometry(LINESTRING, 4326) NOT NULL,
                CONSTRAINT trajectory_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_mmsi_idx
            ON prototype2.trajectory_ls_testing (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON prototype2.trajectory_ls_testing (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_geom_idx
            ON prototype2.trajectory_ls_testing 
            USING GIST (geom);
        """)
    print("Created LS trajectory table if not exists")

    # Stop table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS prototype2.stop_poly_testing (
                stop_id SERIAL PRIMARY KEY,
                mmsi BIGINT NOT NULL,
                ts_start TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                geom geometry(POLYGON, 4326) NOT NULL,
                CONSTRAINT stop_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_mmsi_idx
            ON prototype2.stop_poly_testing (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON prototype2.stop_poly_testing (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_geom_idx
            ON prototype2.stop_poly_testing 
            USING GIST (geom);
        """)
    print("Created LS stop table if not exists")
    
    conn.commit()
    cur.close()
