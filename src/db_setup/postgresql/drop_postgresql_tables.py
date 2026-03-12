from psycopg import Connection, sql

def drop_postgresql_tables(conn: Connection, db_schema: str):
    cur = conn.cursor()

    # cur.execute(sql.SQL("DROP MATERIALIZED VIEW IF EXISTS {db_schema}.points;").format(db_schema=sql.Identifier(db_schema)))

    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.trajectory_ls;").format(db_schema=sql.Identifier(db_schema)))
    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.stop_poly;").format(db_schema=sql.Identifier(db_schema)))

    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.trajectory_cs;").format(db_schema=sql.Identifier(db_schema)))
    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.stop_cs;").format(db_schema=sql.Identifier(db_schema)))
    
    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.area_poly;").format(db_schema=sql.Identifier(db_schema)))
    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.area_cs;").format(db_schema=sql.Identifier(db_schema)))
    
    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.crossing_ls;").format(db_schema=sql.Identifier(db_schema)))
    # cur.execute(sql.SQL("DROP TABLE IF EXISTS {db_schema}.crossing_cs;").format(db_schema=sql.Identifier(db_schema)))
    
    print(f"Dropped all tables and materialized view if they existed in database schema {db_schema}.")
    
    # cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {db_schema} CASCADE;").format(db_schema=sql.Identifier(db_schema)))
    
    print(f"Dropped database schema {db_schema} if it existed.")

    conn.commit()
    cur.close()
