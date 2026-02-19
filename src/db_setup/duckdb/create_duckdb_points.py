import duckdb

def create_duckdb_points(conn: duckdb.DuckDBPyConnection, db_schema: str):
    conn.execute(f"""
        CREATE OR REPLACE TABLE {db_schema}.points AS
        WITH valid_mmsi AS (
            SELECT mmsi
            FROM ais_data
            WHERE lat != 91
              AND LENGTH(CAST(mmsi AS VARCHAR)) = 9
              AND CAST(mmsi AS VARCHAR)[1] BETWEEN '2' AND '7'
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
            ORDER BY a.mmsi, a.lat, a.lon, a.timestamp
        )
        SELECT * FROM dedup;
    """)

    print(f"Created table 'points' in DuckDB schema '{db_schema}'.")
