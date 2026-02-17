import duckdb

def create_duckdb_points(conn: duckdb.DuckDBPyConnection):
    conn.execute("""
        CREATE OR REPLACE TABLE points AS
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

    print("Created table 'points' in DuckDB.")

if __name__ == "__main__":
    conn = duckdb.connect(database='./ais_data/ais_db.duckdb')
    create_duckdb_points(conn)
    
    # Verify
    print('Points count:', conn.execute('SELECT COUNT(*) FROM points').fetchone())
    print('Distinct MMSIs:', conn.execute('SELECT COUNT(DISTINCT mmsi) FROM points').fetchone())
    
    conn.close()