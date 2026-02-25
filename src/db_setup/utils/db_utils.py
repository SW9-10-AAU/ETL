import os
from dotenv import load_dotenv

def get_db_schema(db_type: str) -> str:
    load_dotenv()
    
    if db_type == 'postgresql':
        db_schema = os.getenv('POSTGRESQL_SCHEMA')
        if not db_schema:
            raise ValueError("POSTGRESQL_SCHEMA not defined in .env file")
        return db_schema
    elif db_type == 'duckdb':
        db_schema = os.getenv('DUCKDB_SCHEMA')
        if not db_schema:
            raise ValueError("DUCKDB_SCHEMA not defined in .env file")
        return db_schema
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def get_db_path_or_url(db_type: str) -> str:
    """Get the database path (DuckDB) or URL (PostgreSQL) from environment variables based on the database type."""
    load_dotenv()
    
    if db_type == 'postgresql':
        db_url = os.getenv('POSTGRESQL_URL')
        if not db_url:
            raise ValueError("POSTGRESQL_URL not defined in .env file")
        return db_url 
    elif db_type == 'duckdb':
        db_path = os.getenv('DUCKDB_PATH')
        if not db_path:
            raise ValueError("DUCKDB_PATH not defined in .env file")
        return db_path
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
def get_db_backend() -> str:
    load_dotenv()
    
    backend = os.getenv('DB_BACKEND')
    
    if (not backend) or (backend.lower() not in ['duckdb', 'postgresql']):
        raise ValueError(f"Unsupported DB_BACKEND: {backend}. Please set DB_BACKEND to 'duckdb' or 'postgresql' in the .env file.")

    return backend