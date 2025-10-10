from psycopg import Connection

def mat_points_view(conn: Connection):
    cur = conn.cursor()

    # Example MMSI for testing
    mmsi = "210051000, 636015105"

    # language=SQL
    cur.execute("""
            DROP MATERIALIZED VIEW IF EXISTS prototype1.POINTS;
        """)
    
    cur.execute(f"""
            -- POINTM's with MMSI
            CREATE MATERIALIZED VIEW IF NOT EXISTS prototype1.POINTS AS
            SELECT
                V.mmsi,
                ST_PointM(
                    ST_X(AIS.geom::geometry),
                    ST_Y(AIS.geom::geometry),
                    EXTRACT(
                        EPOCH FROM MAKE_TIMESTAMP(
                            dat.year_no::integer,
                            dat.month_no::integer,
                            dat.day_no::integer,
                            tim.hour_no::integer,
                            tim.minute_no::integer,
                            tim.second_no::double precision
                        )
                    )::double precision
                    ,4326
                ) AS geom,
                AIS.sog,
                AIS.cog,
                AIS.delta_sog,
                AIS.delta_depth_draught
            FROM fact.ais_point_fact AIS
            JOIN dim.vessel_dim V ON AIS.vessel_id = V.vessel_id
            JOIN dim.time_dim TIM ON TIM.time_id = AIS.time_id
            JOIN dim.date_dim DAT ON DAT.date_id = AIS.date_id
            WHERE AIS.lat <> 91::double precision
            AND V.mmsi IN ({mmsi}); -- Restriction of MMSI's
        """)

     # Create index for lookup
    cur.execute("""
            CREATE INDEX IF NOT EXISTS POINTS_IDX
            ON prototype1.POINTS USING HASH (mmsi);
        """)
    
    # Create spatial index
    cur.execute("""
            CREATE INDEX IF NOT EXISTS POINTS_GEOM_IDX
            ON prototype1.POINTS USING GIST (geom) INCLUDE (mmsi);
        """)

    print("Created materialized view POINTS")

    conn.commit()
    cur.close()