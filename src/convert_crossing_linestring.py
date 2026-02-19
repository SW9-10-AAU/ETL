from dotenv import load_dotenv
from shapely import LineString
from db_setup.postgresql.create_crossing_tables import create_crossing_tables
from transform_ls_to_cs import convert_linestring_to_cellstring
from connect import connect_to_db

def convert_crossing_linestring_to_cs(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into PostGIS tables.
    """
    load_dotenv()
    conn = connect_to_db()
    cur = conn.cursor()
    
    # Create benchmark schema and crossing tables if not exist
    create_crossing_tables(conn)
    
    # Insert crossing as linestring into table
    cur.execute("""
            INSERT INTO benchmark.crossing_ls (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
        """, (name, linestring.wkb))    
    conn.commit()
    print("Inserted crossing linestring into PostGIS table")
    
    # Convert crossing to cellstring and insert into table 
    print("Converting crossing to cellstrings")
    cellstring_z13 = convert_linestring_to_cellstring(linestring, 13)
    cellstring_z17 = convert_linestring_to_cellstring(linestring, 17)
    cellstring_z21 = convert_linestring_to_cellstring(linestring, 21)
    print(f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21).")
    
    cur.execute("""
            INSERT INTO benchmark.crossing_cs (name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s)
        """, (name, cellstring_z13, cellstring_z17, cellstring_z21))
    print("Inserted crossing cellstrings into PostGIS table")
    conn.commit()
    cur.close()

    print(f"Crossing ({name}) uploaded to benchmark schema in database")

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

    convert_crossing_linestring_to_cs(linestring, name)

    
if __name__ == "__main__":
    main()
