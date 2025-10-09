from psycopg import Connection

def create_ls_traj_stop_tables(conn: Connection):
    cur = conn.cursor()

    # Schema
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS ls_experiment;
        """)

    # Trajectory table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS ls_experiment.trajectory_ls (
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
            ON ls_experiment.trajectory_ls (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON ls_experiment.trajectory_ls (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_geom_idx
            ON ls_experiment.trajectory_ls 
            USING GIST (geom);
        """)
    print("Created LS trajectory table")

    # Stop table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS ls_experiment.stop_poly (
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
            ON ls_experiment.stop_poly (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON ls_experiment.stop_poly (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_geom_idx
            ON ls_experiment.stop_poly 
            USING GIST (geom);
        """)
    print("Created LS stop table")
    
    conn.commit()
    cur.close()
