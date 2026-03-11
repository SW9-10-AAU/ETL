from psycopg import Connection, sql

def mat_points_view(conn: Connection, db_schema: str):
    cur = conn.cursor()
    
    print("Creating materialized view for points...")
    
    cur.execute(sql.SQL("""
            -- POINTM's with MMSI
            CREATE MATERIALIZED VIEW IF NOT EXISTS {db_schema}.points AS
            WITH valid_mmsi AS (
                SELECT
                    V.mmsi
                FROM fact.ais_point_fact AIS
                JOIN dim.vessel_dim V ON AIS.vessel_id = V.vessel_id
                WHERE AIS.lat <> 91
                AND LENGTH(V.mmsi::text) = 9
                AND LEFT(V.mmsi::text, 1) BETWEEN '2' AND '7'
                AND mmsi IN (219230000)
                GROUP BY V.mmsi
                HAVING COUNT(*) >= 10
            ),
            raw AS (
                SELECT
                    V.mmsi,
                    AIS.geom::geometry AS geom,
                    AIS.sog,
                    (
                        EXTRACT(
                            EPOCH FROM MAKE_TIMESTAMP(
                                dat.year_no::integer,
                                dat.month_no::integer,
                                dat.day_no::integer,
                                tim.hour_no::integer,
                                tim.minute_no::integer,
                                tim.second_no::double precision
                            )
                        )
                    )::double precision AS epoch_ts
                FROM fact.ais_point_fact AIS
                JOIN dim.vessel_dim V      ON AIS.vessel_id = V.vessel_id
                JOIN valid_mmsi vm         ON vm.mmsi = V.mmsi
                JOIN dim.time_dim TIM      ON TIM.time_id = AIS.time_id
                JOIN dim.date_dim DAT      ON DAT.date_id = AIS.date_id
                WHERE AIS.lat <> 91
            ),
            dedup AS (
                SELECT DISTINCT ON (mmsi, geom, epoch_ts)
                    mmsi,
                    geom,
                    sog,
                    epoch_ts
                FROM raw
                ORDER BY mmsi, geom, epoch_ts
            )
            SELECT
                mmsi,
                ST_PointM(ST_X(geom), ST_Y(geom), epoch_ts, 4326) AS geom,
                sog
            FROM dedup;
        """).format(db_schema=sql.Identifier(db_schema)))

     # Create index for lookup
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS POINTS_IDX
            ON {db_schema}.POINTS USING HASH (mmsi);
        """).format(db_schema=sql.Identifier(db_schema)))
    
    # Create spatial index
    cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS POINTS_GEOM_IDX
            ON {db_schema}.POINTS USING GIST (geom) INCLUDE (mmsi);
        """).format(db_schema=sql.Identifier(db_schema)))

    print(f"Created materialized view POINTS if not exists in database schema {db_schema}.")

    conn.commit()
    cur.close()