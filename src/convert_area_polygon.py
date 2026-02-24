from shapely import Polygon, MultiPolygon
from db_setup.utils.db_utils import get_db_backend, get_db_path_or_url, get_db_schema
from core.ls_poly_to_cs import convert_polygon_to_cellstrings

# PostgreSQL implementation
def convert_area_polygon_to_cs_postgresql(polygon: Polygon | MultiPolygon, name: str, skip_z21: bool = False):
    """
    Converts a Polygon or MultiPolygon to CellStrings and inserts both into PostGIS tables.

    Args:
        polygon: A Shapely Polygon or MultiPolygon representing the area
        name: A unique identifier for this area
    """
    from db_setup.utils.connect import connect_to_postgres_db
    from psycopg import sql
    
    conn = connect_to_postgres_db()
    cur = conn.cursor()
    db_schema = get_db_schema("postgresql")

    # Insert area as polygon into table
    cur.execute(sql.SQL("""
            INSERT INTO {db_schema}.area_poly (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
        """).format(db_schema=sql.Identifier(db_schema)), (name, polygon.wkb))    
    conn.commit()
    print("Inserted area polygon into PostGIS table")
    
    # Convert polygon to cellstring and insert into table
    print("Converting polygon to cellstrings")
    cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstrings(polygon, skip_z21=skip_z21)
    print(f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21).")
    
    cur.execute(sql.SQL("""
            INSERT INTO {db_schema}.area_cs (name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s)
        """).format(db_schema=sql.Identifier(db_schema)), (name, cellstring_z13, cellstring_z17, cellstring_z21))
    print("Inserted area cellstrings into PostGIS table")
    conn.commit()
    cur.close()

    print(f"Area ({name}) uploaded to {db_schema} schema in database")

# DuckDB implementation
def convert_area_polygon_to_cs_duckdb(polygon: Polygon | MultiPolygon, name: str, skip_z21: bool = False):
    """
    Converts a Polygon or MultiPolygon to CellStrings and inserts both into DuckDB tables.

    Args:
        polygon: A Shapely Polygon or MultiPolygon representing the area
        name: A unique identifier for this area
        duckdb_path: Path to DuckDB database file
    """
    import duckdb

    db_schema = get_db_schema("duckdb")
    duckdb_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(duckdb_path)

    # Insert area polygon as WKB blob
    conn.execute(
        f"""INSERT INTO {db_schema}.area_poly (name, geom)
           VALUES (?, ?)""",
        [name, polygon.wkb],
    )
    print("Inserted area polygon into DuckDB table")

    print("Converting polygon to cellstrings")
    cellstring_z13, cellstring_z17, cellstring_z21 = (
        convert_polygon_to_cellstrings(polygon, skip_z21=skip_z21)
    )
    print(
        f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), "
        f"{len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21)."
    )

    conn.execute(
        f"""INSERT INTO {db_schema}.area_cs
           (name, cellstring_z13, cellstring_z17, cellstring_z21)
           VALUES (?, ?, ?, ?)""",
        [name, cellstring_z13, cellstring_z17, cellstring_z21],
    )
    print("Inserted area cellstrings into DuckDB table")
    conn.close()

    print(f"Area ({name}) uploaded to {db_schema} schema in database")

def main():
    """
    Add a name for the area, and replace the coordinates.
    You can draw on a map using these tools: 
    - https://geojson.io/#map=6.47/55.777/10.723 
    - https://www.keene.edu/campus/maps/tool/
    """
    name = "Test Area"
    polygon = Polygon([
        [
          10.316413685524424,
          56.99087639182116
        ],
        [
          10.314065627808503,
          56.97054114551048
        ],
        [
          10.299771403521703,
          56.95066789065572
        ],
        [
          10.328563775100804,
          56.94490413109608
        ],
        [
          10.370374382207302,
          56.96205296354384
        ],
        [
          10.362155203242992,
          56.98172542302635
        ],
        [
          10.316413685524424,
          56.99087639182116
        ]
    ])

    db = get_db_backend()
    
    if (db == "postgresql"):
        convert_area_polygon_to_cs_postgresql(polygon, name)
    elif (db == "duckdb"):
        convert_area_polygon_to_cs_duckdb(polygon, name)

if __name__ == "__main__":
    main()
