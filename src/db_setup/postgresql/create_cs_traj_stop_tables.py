from psycopg import Connection, sql

def create_cs_traj_stop_tables(conn: Connection, db_schema: str):
    cur = conn.cursor()

    # Trajectory table
    cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {db_schema}.trajectory_cs (
                trajectory_id INTEGER PRIMARY KEY,
                mmsi          BIGINT                      NOT NULL,
                ts_start      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                cellstring_z13    int ARRAY               NOT NULL,
                cellstring_z17    bigint ARRAY            NOT NULL,
                cellstring_z21    bigint ARRAY            NOT NULL,
                CONSTRAINT trajectory_time_check CHECK (ts_start < ts_end)
            );
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_mmsi_idx 
            ON {db_schema}.trajectory_cs (mmsi);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON {db_schema}.trajectory_cs (ts_start, ts_end);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z13_gin_idx 
            ON {db_schema}.trajectory_cs 
            USING GIN (cellstring_z13 gin__int_ops);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z17_gin_idx 
            ON {db_schema}.trajectory_cs 
            USING GIN (cellstring_z17);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z21_gin_idx 
            ON {db_schema}.trajectory_cs 
            USING GIN (cellstring_z21);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z17_gin_idx 
            ON {db_schema}.trajectory_cs 
            USING GIN (cellstring_z17);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS trajectory_cellstring_z21_gin_idx 
            ON {db_schema}.trajectory_cs 
            USING GIN (cellstring_z21);
        """).format(db_schema=sql.Identifier(db_schema)))
    print(f"Created CS trajectory table if not exists in database schema {db_schema}.")

    # Stop table
    cur.execute(sql.SQL("""
            CREATE TABLE IF NOT EXISTS {db_schema}.stop_cs
            (
                stop_id     INTEGER PRIMARY KEY,
                mmsi        BIGINT                      NOT NULL,
                ts_start    TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                cellstring_z13  int ARRAY               NOT NULL,
                cellstring_z17  bigint ARRAY            NOT NULL,
                cellstring_z21  bigint ARRAY            NOT NULL,
                CONSTRAINT stop_time_check CHECK (ts_start < ts_end)
            );
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS stop_mmsi_idx
            ON {db_schema}.stop_cs (mmsi);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON {db_schema}.stop_cs (ts_start, ts_end);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS stop_cellstring_z13_gin_idx
            ON {db_schema}.stop_cs
            USING GIN (cellstring_z13 gin__int_ops);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS stop_cellstring_z17_gin_idx
            ON {db_schema}.stop_cs
            USING GIN (cellstring_z17);
        """).format(db_schema=sql.Identifier(db_schema)))
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS stop_cellstring_z21_gin_idx
            ON {db_schema}.stop_cs
            USING GIN (cellstring_z21);
        """).format(db_schema=sql.Identifier(db_schema)))
    print(f"Created CS stop table if not exists in database schema {db_schema}.")

    conn.commit()
    cur.close()
