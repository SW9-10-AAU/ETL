from psycopg import Connection

def drop_all_tables(conn: Connection):
    cur = conn.cursor()

    # Drop tables if they exist
    cur.execute("DROP TABLE IF EXISTS prototype1.trajectory_cs;")
    cur.execute("DROP TABLE IF EXISTS prototype1.stop_cs;")
    cur.execute("DROP TABLE IF EXISTS prototype1.trajectory_ls;")
    cur.execute("DROP TABLE IF EXISTS prototype1.stop_poly;")
    cur.execute("DROP MATERIALIZED VIEW IF EXISTS prototype1.points;")

    print("Dropped all tables and materialized view if they existed.")

    conn.commit()
    cur.close()