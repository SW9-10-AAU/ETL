from psycopg import Connection

def create_cs_traj_stop_tables(conn: Connection):
    cur = conn.cursor()

    # Schema
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS ls_experiment;
        """)

    # Trajectory table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS ls_experiment.trajectory_cs (
                trajectory_id SERIAL PRIMARY KEY,
                mmsi          BIGINT                      NOT NULL,
                ts_start      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                trajectory    bigint ARRAY                NOT NULL,
                CONSTRAINT trajectory_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_mmsi_idx 
            ON ls_experiment.trajectory_cs (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON ls_experiment.trajectory_cs (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_trajectory_idx 
            ON ls_experiment.trajectory_cs 
            USING GIN (trajectory);
        """)
    print("Created CS trajectory table")

    # Stop table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS ls_experiment.stop_cs
            (
                stop_id     SERIAL PRIMARY KEY,
                mmsi        BIGINT                      NOT NULL,
                ts_start    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                trajectory  bigint ARRAY                NOT NULL,
                CONSTRAINT stop_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_mmsi_idx
            ON ls_experiment.stop_cs (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON ls_experiment.stop_cs (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_trajectory_idx
            ON ls_experiment.stop_cs 
            USING GIN (trajectory);
        """)
    print("Created CS stop table")

    conn.commit()
    cur.close()