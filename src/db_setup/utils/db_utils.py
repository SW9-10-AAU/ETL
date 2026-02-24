import os
from dotenv import load_dotenv

def get_db_schema(db_type: str) -> str:
    load_dotenv()
    
    if db_type == 'postgresql':
        if not os.getenv('POSTGRESQL_SCHEMA'):
            raise ValueError("POSTGRESQL_SCHEMA not defined in .env file")
        return os.getenv('POSTGRESQL_SCHEMA')
    elif db_type == 'duckdb':
        if not os.getenv('DUCKDB_SCHEMA'):
            raise ValueError("DUCKDB_SCHEMA not defined in .env file")
        return os.getenv('DUCKDB_SCHEMA')
    else:
        raise ValueError(f"Unsupported database type: {db_type}")

def get_db_path(db_type: str) -> str:
    load_dotenv()
    
    if db_type == 'postgresql':
        if not os.getenv('POSTGRESQL_URL'):
            raise ValueError("POSTGRESQL_URL not defined in .env file")
        return os.getenv('POSTGRESQL_URL') 
    elif db_type == 'duckdb':
        if not os.getenv('DUCKDB_PATH'):
            raise ValueError("DUCKDB_PATH not defined in .env file")
        return os.getenv('DUCKDB_PATH')
    else:
        raise ValueError(f"Unsupported database type: {db_type}")
    
def get_db_backend() -> str:
    load_dotenv()
    
    backend = os.getenv('DB_BACKEND').lower()
    if backend not in ['duckdb', 'postgresql']:
        raise ValueError(f"Unsupported DB_BACKEND: {backend}. Please set DB_BACKEND to 'duckdb' or 'postgresql' in the .env file.")
    return backend