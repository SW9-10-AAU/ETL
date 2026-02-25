from psycopg import Connection

def drop_postgresql_tables(conn: Connection, db_schema: str):
    cur = conn.cursor()

    # cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {db_schema}.points;")

    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.trajectory_ls;")
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.stop_poly;")
    
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.trajectory_cs;")
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.stop_cs;")
    
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.area_poly;")
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.area_cs;")
    
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.crossing_ls;")
    # cur.execute(f"DROP TABLE IF EXISTS {db_schema}.crossing_cs;")
    
    print(f"Dropped all tables and materialized view if they existed in database schema {db_schema}.")
    
    # cur.execute(f"DROP SCHEMA IF EXISTS {db_schema} CASCADE;")
    
    print(f"Dropped database schema {db_schema} if it existed.")

    conn.commit()
    cur.close()
