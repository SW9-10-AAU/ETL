import psycopg
import sys
import os

def connect_to_db():
    db_url = os.getenv('DATABASE_URL')
    
    if not db_url:
        sys.exit("DATABASE_URL not defined in .env file")
        
    conn = psycopg.connect(db_url)
    return conn