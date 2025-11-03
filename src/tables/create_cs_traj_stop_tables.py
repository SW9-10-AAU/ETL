from psycopg import Connection


def create_cs_traj_stop_tables(conn: Connection):
    cur = conn.cursor()

    # Trajectory table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS prototype2.trajectory_cs_extrazoom (
                trajectory_id BIGINT PRIMARY KEY,
                mmsi          BIGINT                      NOT NULL,
                ts_start      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                unique_cells  boolean DEFAULT FALSE,
                cellstring_z13    int ARRAY               NOT NULL,
                cellstring_z21    bigint ARRAY            NOT NULL,
                CONSTRAINT trajectory_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_mmsi_idx 
            ON prototype2.trajectory_cs_extrazoom (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON prototype2.trajectory_cs_extrazoom (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z13_gin_idx 
            ON prototype2.trajectory_cs_extrazoom 
            USING GIN (cellstring_z13);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z21_gin_idx 
            ON prototype2.trajectory_cs_extrazoom 
            USING GIN (cellstring_z21);
        """)
    print("Created CS trajectory table if not exists")

    # Stop table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS prototype2.stop_cs_extrazoom
            (
                stop_id     BIGINT PRIMARY KEY,
                mmsi        BIGINT                      NOT NULL,
                ts_start    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                unique_cells  boolean DEFAULT TRUE,
                cellstring_z13  int ARRAY               NOT NULL,
                cellstring_z21  bigint ARRAY            NOT NULL,
                CONSTRAINT stop_time_check CHECK (ts_start < ts_end)
            );
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_mmsi_idx
            ON prototype2.stop_cs_extrazoom (mmsi);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON prototype2.stop_cs_extrazoom (ts_start, ts_end);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_cellstring_z13_gin_idx
            ON prototype2.stop_cs_extrazoom
            USING GIN (cellstring_z13);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS stop_cellstring_z21_gin_idx
            ON prototype2.stop_cs_extrazoom
            USING GIN (cellstring_z21);
        """)
    print("Created CS stop table if not exists")

    conn.commit()
    cur.close()
