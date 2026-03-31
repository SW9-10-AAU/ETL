import duckdb


def create_duckdb_points(conn: duckdb.DuckDBPyConnection, db_schema: str):
    print("Creating table 'points' in DuckDB...")

    conn.execute(
        f"""
        CREATE OR REPLACE TABLE {db_schema}.points AS
        WITH valid_mmsi AS (
            SELECT mmsi
            FROM ais_data
            WHERE lat != 91
              AND LENGTH(CAST(mmsi AS VARCHAR)) = 9
              AND CAST(mmsi AS VARCHAR)[1] BETWEEN '2' AND '7'
              AND transponder_type = 'class a'
              AND mmsi = 249814000 # FOR TESTING: Vessel that takes a long time to process in the construction part of ETL
            GROUP BY mmsi
            HAVING COUNT(*) >= 10
        ),
        dedup AS (
            SELECT DISTINCT ON (a.mmsi, a.lat, a.lon, a.timestamp)
                a.mmsi,
                a.lat,
                a.lon,
                a.sog,
                a.timestamp,
                EPOCH(a.timestamp) AS epoch_ts
            FROM ais_data a
            JOIN valid_mmsi v ON a.mmsi = v.mmsi
            WHERE a.lat != 91
            ORDER BY a.mmsi, a.timestamp, a.lat, a.lon
        )
        SELECT * FROM dedup 
        ORDER BY mmsi, epoch_ts;
    """
    )

    print(f"Created table 'points' in DuckDB schema '{db_schema}'.")
