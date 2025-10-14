from psycopg import Connection

def create_ls_traj_stop_tables(conn: Connection):
    cur = conn.cursor()

    # Schema
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS ls_experiment;
        """)
    
    # --- TEMPORARY DROP TABLE --- TODO: Remove this code
    cur.execute("""
        DROP TABLE IF EXISTS ls_experiment.trajectory_ls_new
        """)
    
    cur.execute("""
        DROP TABLE IF EXISTS ls_experiment.stop_poly_new
        """)
    
    # ---------------------------- 
    
    
    # Trajectory table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS ls_experiment.trajectory_ls_new (
                trajectory_id SERIAL PRIMARY KEY,
                mmsi          BIGINT                      NOT NULL,
                ts_start      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                geom          geometry(LINESTRINGM, 4326) NOT NULL,
                CONSTRAINT trajectory_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_mmsi_idx
            ON ls_experiment.trajectory_ls_new (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON ls_experiment.trajectory_ls_new (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_geom_idx
            ON ls_experiment.trajectory_ls_new 
            USING GIST (geom);
        """)
    print("Created LS trajectory table")

    # Stop table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS ls_experiment.stop_poly_new (
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
            ON ls_experiment.stop_poly_new (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON ls_experiment.stop_poly_new (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_geom_idx
            ON ls_experiment.stop_poly_new 
            USING GIST (geom);
        """)
    print("Created LS stop table")
    
    conn.commit()
    cur.close()
