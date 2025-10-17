from dotenv import load_dotenv
from shapely import Polygon
from transform_ls_to_cs import convert_polygon_to_cellstring
from connect import connect_to_db

def convert_area_polygon_to_cs(polygon: Polygon, name: str):
    """
    Converts a polygon to a CellString and writes the result to a file.
    """
    load_dotenv()
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Insert area as polygon into table
    cur.execute("""
            INSERT INTO benchmark.area_poly (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
        """, (name, polygon.wkb))    
    conn.commit()
    print("Inserted area polygon into PostGIS table...")
    
    # Convert polygon to cellstring and insert into table 
    print("Converting polygon to cellstring...")
    cellstring = convert_polygon_to_cellstring(polygon)
    print(f"Conversion succeeded with {len(cellstring)} cells")
    
    cur.execute("""
            INSERT INTO benchmark.area_cs (name, cellstring)
            VALUES (%s, %s)
        """, (name, cellstring))
    print("Inserted area cellstring into PostGIS table...")
    conn.commit()
    cur.close()
    
    print("Area uploaded to benchmark schema in database")

def main():
    """
    Example polygon — Læsø region.
    Replace with your own coordinates as needed.
    Draw polygon using this tool: https://www.keene.edu/campus/maps/tool/
    """
    name = "Læsø region"
    polygon = Polygon([
        [10.5556052, 57.2720511],
        [10.8488030, 57.2786743],
        [10.8823994, 57.2801017],
        [10.8470372, 57.2623427],
        [10.9040288, 57.2229664],
        [10.9369878, 57.1597278],
        [10.9726934, 57.0945162],
        [10.3945378, 57.0639204],
        [10.4666356, 57.1727564],
        [10.5661992, 57.2129298],
        [10.5556052, 57.2720511]
    ])

    convert_area_polygon_to_cs(polygon, name)

    
if __name__ == "__main__":
    main()
