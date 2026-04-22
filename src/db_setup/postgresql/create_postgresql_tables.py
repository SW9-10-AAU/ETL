from psycopg import Connection, sql
from db_setup.postgresql.create_region_tables import create_region_tables
from db_setup.postgresql.create_passage_tables import create_passage_tables
from db_setup.postgresql.create_cs_traj_stop_tables import create_cs_traj_stop_tables
from db_setup.postgresql.create_ls_traj_stop_tables import create_ls_traj_stop_tables
from db_setup.postgresql.mat_points_view import mat_points_view


def create_postgresql_schema(conn: Connection, db_schema: str):
    cur = conn.cursor()
    cur.execute(
        sql.SQL("""CREATE SCHEMA IF NOT EXISTS {db_schema};""").format(
            db_schema=sql.Identifier(db_schema)
        )
    )
    conn.commit()
    cur.close()
    print(f"Ensured database schema {db_schema} exists.")


def create_postgresql_points(conn: Connection, db_schema: str):
    mat_points_view(conn, db_schema)


def create_postgresql_tables(conn: Connection, ls_schema: str, cs_schema: str):

    # Create LineString/Polygon tables Trajectory and Stop
    create_ls_traj_stop_tables(conn, ls_schema)

    # Create CellString tables Trajectory and Stop
    create_cs_traj_stop_tables(conn, cs_schema)

    # Create (region and passage) tables
    create_region_tables(conn, ls_schema, cs_schema)
    create_passage_tables(conn, ls_schema, cs_schema)

    print(
        f"Created PostgreSQL tables (trajectory_ls, stop_poly, region_poly, passage_ls) in schema '{ls_schema}' and CellString tables (trajectory_cs, stop_cs, region_cs, passage_cs) in schema '{cs_schema}'."
    )
