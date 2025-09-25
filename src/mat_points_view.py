from connect import connect_to_db
from dotenv import load_dotenv
import shapely
from shapely import LineString

def main():
    load_dotenv()
    connection = connect_to_db()
    cur = connection.cursor()

    # language=SQL
    cur.execute("""
                DROP MATERIALIZED VIEW IF EXISTS ls_experiment.POINTS;
    
                -- POINTM's with MMSI
                CREATE MATERIALIZED VIEW IF NOT EXISTS ls_experiment.POINTS AS
                SELECT
                    V.mmsi,
                    ST_MakePointM(
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
                  AND V.mmsi = 210051000;

                -- index for lookup
                CREATE INDEX IF NOT EXISTS POINTS_IDX
                  ON ls_experiment.POINTS USING HASH (mmsi);

                -- spatial index
                CREATE INDEX IF NOT EXISTS POINTS_GEOM_IDX
                  ON ls_experiment.POINTS USING GIST (geom) INCLUDE (mmsi);
                """)

    #trajectories = cur.fetchall()

    print("Created table")

    cur.close()
    connection.commit()

if __name__ == "__main__":
    main()