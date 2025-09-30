from connect import connect_to_db
from dotenv import load_dotenv

def main():
    load_dotenv()
    conn = connect_to_db()
    cur = conn.cursor()

    # Example MMSI for testing
    mmsi = "210051000, 636015105"

    # language=SQL
    cur.execute("""
                DROP MATERIALIZED VIEW IF EXISTS ls_experiment.POINTS;
                """)
    
    cur.execute(f"""
                -- POINTM's with MMSI
                CREATE MATERIALIZED VIEW IF NOT EXISTS ls_experiment.POINTS AS
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
				AND V.mmsi IN ({mmsi});
				""")

     # Create index for lookup
    cur.execute("""--sql
        CREATE INDEX IF NOT EXISTS POINTS_IDX
        ON ls_experiment.POINTS USING HASH (mmsi);
    """)
    
    # Create spatial index
    cur.execute("""--sql
        CREATE INDEX IF NOT EXISTS POINTS_GEOM_IDX
        ON ls_experiment.POINTS USING GIST (geom) INCLUDE (mmsi);
    """)

    print("Created materialized view and indexes")

    conn.commit()
    cur.close()

if __name__ == "__main__":
    main()