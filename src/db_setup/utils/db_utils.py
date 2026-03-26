import os
from dotenv import load_dotenv


def _get_required_env(key: str) -> str:
    value = os.getenv(key)
    if not value:
        raise ValueError(f"{key} not defined in .env file")
    return value


def _get_schema_with_fallback(primary_key: str, fallback_key: str) -> str:
    value = os.getenv(primary_key)
    if value:
        return value

    fallback = os.getenv(fallback_key)
    if fallback:
        return fallback

    raise ValueError(f"{primary_key} or {fallback_key} not defined in .env file")


def get_db_schema(db_type: str) -> str:
    load_dotenv()

    if db_type == "postgresql":
        db_schema = os.getenv("POSTGRESQL_SCHEMA")
        if not db_schema:
            raise ValueError("POSTGRESQL_SCHEMA not defined in .env file")
        return db_schema
    elif db_type == "duckdb":
        db_schema = os.getenv("DUCKDB_SCHEMA")
        if not db_schema:
            raise ValueError("DUCKDB_SCHEMA not defined in .env file")
        return db_schema
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def get_db_path_or_url(db_type: str) -> str:
    """Get the database path (DuckDB) or URL (PostgreSQL) from environment variables based on the database type."""
    load_dotenv()

    if db_type == "postgresql":
        db_url = os.getenv("POSTGRESQL_URL")
        if not db_url:
            raise ValueError("POSTGRESQL_URL not defined in .env file")
        return db_url
    elif db_type == "duckdb":
        db_path = os.getenv("DUCKDB_PATH")
        if not db_path:
            raise ValueError("DUCKDB_PATH not defined in .env file")
        return db_path
    else:
        raise ValueError(f"Unsupported database type: {db_type}")


def get_db_backend() -> str:
    load_dotenv()

    backend = os.getenv("DB_BACKEND")

    if (not backend) or (backend.lower() not in ["duckdb", "postgresql"]):
        raise ValueError(
            f"Unsupported DB_BACKEND: {backend}. Please set DB_BACKEND to 'duckdb' or 'postgresql' in the .env file."
        )

    return backend


def get_ls_schema(db_type: str) -> str:
    load_dotenv()

    if db_type == "postgresql":
        return _get_schema_with_fallback("POSTGRESQL_LS_SCHEMA", "POSTGRESQL_SCHEMA")
    if db_type == "duckdb":
        return _get_schema_with_fallback("DUCKDB_LS_SCHEMA", "DUCKDB_SCHEMA")

    raise ValueError(f"Unsupported database type: {db_type}")


def get_cs_schema(db_type: str) -> str:
    load_dotenv()

    if db_type == "postgresql":
        return _get_schema_with_fallback("POSTGRESQL_CS_SCHEMA", "POSTGRESQL_SCHEMA")
    if db_type == "duckdb":
        return _get_schema_with_fallback("DUCKDB_CS_SCHEMA", "DUCKDB_SCHEMA")

    raise ValueError(f"Unsupported database type: {db_type}")
