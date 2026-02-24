from shapely import LineString
from core.ls_poly_to_cs import convert_linestring_to_cellstrings
from db_setup.utils.db_utils import get_db_backend, get_db_path_or_url, get_db_schema

# PostgreSQL implementation
def convert_crossing_linestring_to_cs_postgres(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into PostGIS tables.
    """
    from db_setup.utils.connect import connect_to_postgres_db
    from psycopg import sql
    
    conn = connect_to_postgres_db()
    cur = conn.cursor()
    db_schema = get_db_schema("postgresql")
    
    # Insert crossing as linestring into table
    cur.execute(sql.SQL("""
            INSERT INTO {db_schema}.crossing_ls (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
        """).format(db_schema=sql.Identifier(db_schema)), (name, linestring.wkb))    
    conn.commit()
    print("Inserted crossing linestring into PostGIS table")
    
    # Convert crossing to cellstring and insert into table 
    print("Converting crossing to cellstrings")
    cellstring_z13, cellstring_z17, cellstring_z21 = convert_linestring_to_cellstrings(linestring)
    print(f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21).")
    
    cur.execute(sql.SQL("""
            INSERT INTO {db_schema}.crossing_cs (name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s)
        """).format(db_schema=sql.Identifier(db_schema)), (name, cellstring_z13, cellstring_z17, cellstring_z21))
    print("Inserted crossing cellstrings into PostGIS table")
    conn.commit()
    cur.close()

    print(f"Crossing ({name}) uploaded to {db_schema} schema in database")
    
# DuckDB implementation   
def convert_crossing_linestring_to_cs_duckdb(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into DuckDB tables.
    """
    import duckdb

    db_schema = get_db_schema("duckdb")
    db_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(db_path)

    # Insert crossing as linestring into table
    conn.execute(
        f"""INSERT INTO {db_schema}.crossing_ls (name, geom)
           VALUES (?, ?)""",
        [name, linestring.wkb],
    )
    print("Inserted crossing linestring into DuckDB table")

    print("Converting crossing to cellstrings")
    cellstring_z13, cellstring_z17, cellstring_z21 = (
        convert_linestring_to_cellstrings(linestring)
    )
    print(
        f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), "
        f"{len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells "
        f"(zoom 21)."
    )

    conn.execute(
        f"""INSERT INTO {db_schema}.crossing_cs
           (name, cellstring_z13, cellstring_z17, cellstring_z21)
           VALUES (?, ?, ?, ?)""",
        [name, cellstring_z13, cellstring_z17, cellstring_z21],
    )
    print("Inserted crossing cellstrings into DuckDB table")
    conn.close()

    print(f"Crossing ({name}) uploaded to {db_schema} schema in database")

def main():
    """
    Add a name for the crossing, and replace the coordinates.
    You can draw on a map using these tools: 
    - https://geojson.io/#map=6.47/55.777/10.723 
    - https://www.keene.edu/campus/maps/tool/
    """
    name = "Bornholm"
    linestring = LineString([
        [
            14.376500767303753,
            55.43940778019655
        ],
        [
            14.713765894365793,
            55.30487796624158
        ]
    ])
    
    db_backend = get_db_backend()
    if db_backend == 'postgresql': convert_crossing_linestring_to_cs_postgres(linestring, name)
    elif db_backend == 'duckdb': convert_crossing_linestring_to_cs_duckdb(linestring, name)  
    
if __name__ == "__main__":
    main()
