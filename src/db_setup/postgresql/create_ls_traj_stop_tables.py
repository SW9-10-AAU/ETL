from psycopg import Connection, sql


def create_ls_traj_stop_tables(conn: Connection, db_schema: str):
    cur = conn.cursor()

    # Trajectory table
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {db_schema}.trajectory_ls (
                trajectory_id SERIAL PRIMARY KEY,
                mmsi          BIGINT                      NOT NULL,
                ts_start      TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end        TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                geom          geometry(LINESTRINGM, 4326) NOT NULL,
                CONSTRAINT trajectory_time_check CHECK (ts_start < ts_end)
            );
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    cur.execute(
        sql.SQL(
            """
                CREATE INDEX IF NOT EXISTS trajectory_mmsi_idx
            ON {db_schema}.trajectory_ls (mmsi);
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS trajectory_time_idx
            ON {db_schema}.trajectory_ls (ts_start, ts_end);
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS trajectory_geom_idx
            ON {db_schema}.trajectory_ls 
            USING GIST (geom);
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    print(f"Created LS trajectory table if not exists in database schema {db_schema}")

    # Stop table
    cur.execute(
        sql.SQL(
            """
            CREATE TABLE IF NOT EXISTS {db_schema}.stop_poly (
                stop_id SERIAL PRIMARY KEY,
                mmsi BIGINT NOT NULL,
                ts_start TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                ts_end   TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                geom geometry(POLYGON, 4326) NOT NULL,
                CONSTRAINT stop_time_check CHECK (ts_start < ts_end)
            );
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS stop_mmsi_idx
            ON {db_schema}.stop_poly (mmsi);
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS stop_time_idx
            ON {db_schema}.stop_poly (ts_start, ts_end);
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    cur.execute(
        sql.SQL(
            """
            CREATE INDEX IF NOT EXISTS stop_geom_idx
            ON {db_schema}.stop_poly
            USING GIST (geom);
        """
        ).format(db_schema=sql.Identifier(db_schema))
    )
    print(f"Created LS stop table if not exists in database schema {db_schema}.")

    conn.commit()
    cur.close()
