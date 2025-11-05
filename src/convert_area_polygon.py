from dotenv import load_dotenv
from shapely import Polygon
from transform_ls_to_cs import convert_polygon_to_cellstring
from connect import connect_to_db
from tables.create_area_tables import create_area_tables

def convert_area_polygon_to_cs(polygon: Polygon, name: str):
    """
    Converts a Polygon to a CellString and inserts both into PostGIS tables.
    """
    load_dotenv()
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Create benchmark schema and area tables if not exist
    create_area_tables(conn)
    
    # Insert area as polygon into table
    cur.execute("""
            INSERT INTO benchmark.area_poly (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
        """, (name, polygon.wkb))    
    conn.commit()
    print("Inserted area polygon into PostGIS table")
    
    # Convert polygon to cellstring and insert into table 
    print("Converting polygon to cellstrings")
    cellstring_z13 = convert_polygon_to_cellstring(polygon, 13)
    cellstring_z21 = convert_polygon_to_cellstring(polygon, 21)
    print(f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13) and {len(cellstring_z21)} cells (zoom 21).")
    
    cur.execute("""
            INSERT INTO benchmark.area_cs (name, cellstring_z13, cellstring_z21)
            VALUES (%s, %s, %s)
        """, (name, cellstring_z13, cellstring_z21))
    print("Inserted area cellstrings into PostGIS table")
    conn.commit()
    cur.close()

    print(f"Area ({name}) uploaded to benchmark schema in database")

def main():
    """
    Example polygon — Læsø region.
    Replace with your own coordinates as needed.
    Draw polygon using this tool: https://www.keene.edu/campus/maps/tool/
    """
    
    name = "Hals-Egense"
    polygon = Polygon([
        [
            10.3018805,
            56.9909677
        ],
        [
            10.2991554,
            56.9867599
        ],
        [
            10.3095838,
            56.9857196
        ],
        [
            10.3110858,
            56.9892496
        ],
        [
            10.3018805,
            56.9909677
        ]
    ])

    convert_area_polygon_to_cs(polygon, name)

    
if __name__ == "__main__":
    main()
