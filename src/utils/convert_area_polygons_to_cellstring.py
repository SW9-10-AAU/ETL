from shapely import from_wkb
from core.ls_poly_to_cs import convert_polygon_to_cellstrings
from db_setup.utils.connect import connect_to_postgres_db
from db_setup.utils.db_utils import get_db_backend, get_db_path, get_db_schema

# postgresql implementation
def convert_area_polygons_to_cs_postgres():
    """
    Convert all area polygons in DB to cellstrings and upload to PostGIS. 
    """
    db_schema = get_db_schema("postgresql")
    conn = connect_to_postgres_db()
    cur = conn.cursor()
    
    # Fetch all area polygons from benchmark.area_poly (not already converted)
    cur.execute(f"""
            SELECT area_poly.name, ST_AsBinary(area_poly.geom)
            FROM {db_schema}.area_poly as area_poly
            LEFT JOIN {db_schema}.area_cs AS area_cs ON area_poly.area_id = area_cs.area_id
            WHERE area_cs.area_id IS NULL
            ORDER BY area_poly.area_id;
        """)
    rows = cur.fetchall()
    print(f"Fetched {len(rows)} area polygons from {db_schema}.area_poly")
    
    for row in rows:
        name, geom_wkb = row
        # from_wkb returns the correct type (Polygon or MultiPolygon)
        polygon = from_wkb(geom_wkb)
        
        # Convert polygon to cellstring and insert into table 
        print("Converting polygon to cellstrings")
        cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstrings(polygon)
        
        print(f"Conversion of {name} succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21).")
        
        cur.execute(f"""
                INSERT INTO {db_schema}.area_cs (name, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s)
            """, (name, cellstring_z13, cellstring_z17, cellstring_z21))
        conn.commit()
        print(f"Inserted area cellstrings for {name} into PostGIS table")
        
    print("Converted all area polygons to cellstrings and uploaded to PostGIS.")
    cur.close()

# DuckDB implementation
def convert_area_polygons_to_cs_duckdb():
    """
    Convert all area polygons in DB to cellstrings and upload to DuckDB. 
    """
    import duckdb

    db_path = get_db_path("duckdb")
    db_schema = get_db_schema("duckdb")
    conn = duckdb.connect(database=db_path)

    # Fetch all area polygons from area_poly table (not already converted)
    rows = conn.execute(f"""
            SELECT name, ST_AsBinary(geom)
            FROM {db_schema}.area_poly
            LEFT JOIN {db_schema}.area_cs ON area_poly.area_id = area_cs.area_id
            WHERE area_cs.area_id IS NULL
            ORDER BY area_poly.area_id;
        """).fetchall()
    print(f"Fetched {len(rows)} area polygons from {db_schema}.area_poly table")

    for row in rows:
        name, geom_wkb = row
        polygon = from_wkb(geom_wkb)

        # Convert polygon to cellstring and insert into table 
        print("Converting polygon to cellstrings")
        cellstring_z13 = convert_polygon_to_cellstrings(polygon, 13)
        cellstring_z17 = convert_polygon_to_cellstrings(polygon, 17)
        cellstring_z21 = convert_polygon_to_cellstrings(polygon, 21)
        print(f"Conversion of {name} succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21).")

        conn.execute(f"""
                INSERT INTO {db_schema}.area_cs (name, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (?, ?, ?, ?)
            """, (name, cellstring_z13, cellstring_z17, cellstring_z21))
        print(f"Inserted area cellstrings for {name} into DuckDB table")

    print("Converted all area polygons to cellstrings and uploaded to DuckDB.")
    conn.close()


def main():
    """
    Convert all area polygons in DB to cellstrings and upload to PostGIS. 
    """
    db_backend = get_db_backend()
    if db_backend == "postgres":
        convert_area_polygons_to_cs_postgres()
    elif db_backend == "duckdb":
        convert_area_polygons_to_cs_duckdb()
    
if __name__ == "__main__":
    main()
