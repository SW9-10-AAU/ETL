from psycopg import Connection


def create_supercover_traj_cs_table(conn: Connection):
    cur = conn.cursor()

    # Trajectory table
    cur.execute("""
            CREATE TABLE IF NOT EXISTS prototype2.trajectory_supercover_cs (
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
        CREATE INDEX IF NOT EXISTS trajectory_supercover_cs_mmsi_idx 
        ON prototype2.trajectory_supercover_cs (mmsi);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS trajectory_supercover_cs_time_idx
        ON prototype2.trajectory_supercover_cs (ts_start, ts_end);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS trajectory_supercover_cs_z13_gin_idx 
        ON prototype2.trajectory_supercover_cs 
        USING GIN (cellstring_z13 gin__int_ops);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS trajectory_supercover_cs_z21_gin_idx 
        ON prototype2.trajectory_supercover_cs 
        USING GIN (cellstring_z21);
    """)
    print("Created CS trajectory table if not exists")

    conn.commit()
    cur.close()