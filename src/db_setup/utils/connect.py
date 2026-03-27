import sys
import psycopg
from db_setup.utils.db_utils import get_db_path_or_url


def connect_to_postgres_db() -> psycopg.Connection:
    db_url = get_db_path_or_url("postgresql")
    if not db_url:
        sys.exit("POSTGRESQL_URL not defined in .env file")

    conn = psycopg.connect(db_url)
    return conn
