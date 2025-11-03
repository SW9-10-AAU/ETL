from dotenv import load_dotenv
from shapely import Polygon, from_wkb
from transform_ls_to_cs import convert_polygon_to_cellstring
from connect import connect_to_db

def convert_area_polygons_to_cs():
    """
    Convert all area polygons in DB to cellstrings and upload to PostGIS. 
    """
    load_dotenv()
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Fetch all area polygons from benchmark.area_poly
    cur.execute("""
            SELECT area_poly.name, ST_AsBinary(area_poly.geom)
            FROM benchmark.area_poly as area_poly
            LEFT JOIN benchmark.area_cs_extrazoom AS area_cs_extrazoom ON area_poly.area_id = area_cs_extrazoom.area_id
            WHERE area_cs_extrazoom.area_id IS NULL
            ORDER BY area_poly.area_id;
        """)
    rows = cur.fetchall()
    print(f"Fetched {len(rows)} area polygons from benchmark.area_poly")
    
    for row in rows:
        name, geom_wkb = row
        polygon = Polygon(from_wkb(geom_wkb))
        
        # Convert polygon to cellstring and insert into table 
        print("Converting polygon to cellstrings")
        cellstring_z13 = convert_polygon_to_cellstring(polygon, 13)
        cellstring_z21 = convert_polygon_to_cellstring(polygon, 21)
        print(f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13) and {len(cellstring_z21)} cells (zoom 21).")
        
        cur.execute("""
                INSERT INTO benchmark.area_cs_extrazoom (name, cellstring_z13, cellstring_z21)
                VALUES (%s, %s, %s)
            """, (name, cellstring_z13, cellstring_z21))
        conn.commit()
        print(f"Inserted area cellstrings for {name} into PostGIS table")
        
    print("Converted all area polygons to cellstrings and uploaded to PostGIS.")
    cur.close()


def main():
    """
    Convert all area polygons in DB to cellstrings and upload to PostGIS. 
    """

    convert_area_polygons_to_cs()

    
if __name__ == "__main__":
    main()
