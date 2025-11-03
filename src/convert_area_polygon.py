from dotenv import load_dotenv
from shapely import Polygon
from transform_ls_to_cs import convert_polygon_to_cellstring
from connect import connect_to_db

def convert_area_polygon_to_cs(polygon: Polygon, name: str):
    """
    Converts a Polygon to a CellString and inserts both into PostGIS tables.
    """
    load_dotenv()
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Create benchmark schema and tables if not exist
    cur.execute("""
            CREATE SCHEMA IF NOT EXISTS benchmark;
        """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.area_poly
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                geom geometry(POLYGON, 4326) NOT NULL
            );
        """)
    cur.execute("""
            CREATE TABLE IF NOT EXISTS benchmark.area_cs_extrazoom
            (
                area_id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                cellstring_z13 bigint ARRAY NOT NULL,
                cellstring_z21 bigint ARRAY NOT NULL
            );
        """)

    # Create indexes
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_poly_geom_idx
            ON benchmark.area_poly 
            USING GIST (geom);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_cs_extrazoom_gin_idx
            ON benchmark.area_cs_extrazoom
            USING GIN (cellstring_z13);
        """)
    cur.execute("""
            CREATE INDEX IF NOT EXISTS area_cs_z21_gin_idx
            ON benchmark.area_cs_extrazoom 
            USING GIN (cellstring_z21);
        """)
    
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
            INSERT INTO benchmark.area_cs_extrazoom (name, cellstring_z13, cellstring_z21)
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
