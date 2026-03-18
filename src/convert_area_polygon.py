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
    import pyarrow as pa
    from db_setup.duckdb.pyarrow_schemas import AREA_CS_SCHEMA
    
    db_schema = get_db_schema("duckdb")
    duckdb_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(duckdb_path)

    if skip_z21:
      print("DuckDB requires z21 cells; ignoring skip_z21=True.")
      skip_z21 = False

    # Insert area polygon as geom from WKB
    conn.execute("LOAD SPATIAL;")
    area_row = conn.execute(
        f"""INSERT INTO {db_schema}.area_poly (name, geom)
         VALUES (?, ST_GeomFromWKB(?))
         RETURNING area_id""",
        [name, polygon.wkb],
    ).fetchone()
    if area_row is None:
      raise RuntimeError("Failed to insert area geometry into DuckDB")

    area_id = int(area_row[0])
    print("Inserted area polygon into DuckDB table")

    print("Converting polygon to cellstrings")
    _, _, cellstring_z21 = convert_polygon_to_cellstrings(polygon, skip_z21=skip_z21)
    print(f"Conversion succeeded with {len(cellstring_z21)} cells (zoom 21).")

    if cellstring_z21:
      arrow_table = pa.table({
          "area_id": pa.array([area_id] * len(cellstring_z21), type=pa.int32()),
          "name": pa.array([name] * len(cellstring_z21), type=pa.string()),
          "cell_z21": pa.array(cellstring_z21, type=pa.uint64()),
        }, schema=AREA_CS_SCHEMA)
      conn.execute(f"INSERT INTO {db_schema}.area_cs SELECT * FROM arrow_table")
      print("Inserted area cellstrings into DuckDB table")
    else:
      print("No area cells to insert into DuckDB table")

    conn.close()

    print(f"Area ({name}) uploaded to {db_schema} schema in database")

def main():
    """
    Add a name for the area, and replace the coordinates.
    You can draw on a map using these tools: 
    - https://geojson.io/#map=6.47/55.777/10.723 
    - https://www.keene.edu/campus/maps/tool/
    """
    name = "Malmö Havn"
    polygon = Polygon([
        [
            12.984992202749055,
            55.62161687816493
        ],
        [
            12.984738277907326,
            55.62069513585311
        ],
        [
            12.985101028785948,
            55.61872868145255
        ],
        [
            12.989127556109594,
            55.61917933663952
        ],
        [
            12.990905031581434,
            55.6175200834918
        ],
        [
            12.991594252257215,
            55.61528715292363
        ],
        [
            12.992464840907473,
            55.61395553466488
        ],
        [
            12.993988378205017,
            55.612931184731565
        ],
        [
            12.996600169072082,
            55.61260338592231
        ],
        [
            12.99504037360407,
            55.614693049896744
        ],
        [
            12.99605607723015,
            55.61487742671693
        ],
        [
            13.002585565018876,
            55.61366871300376
        ],
        [
            13.00312970901291,
            55.614406235154775
        ],
        [
            12.993952134469907,
            55.61631144752866
        ],
        [
            12.993444285211183,
            55.617130865740876
        ],
        [
            12.99416978790876,
            55.61762250858058
        ],
        [
            12.995511966898704,
            55.61778638825601
        ],
        [
            12.996164918575488,
            55.61768396356115
        ],
        [
            13.00545135010617,
            55.61565589738913
        ],
        [
            13.007845511941099,
            55.61586075685824
        ],
        [
            13.007591582853934,
            55.61655727301735
        ],
        [
            13.005959207575842,
            55.616803099055204
        ],
        [
            13.006176855576882,
            55.61713086502044
        ],
        [
            12.998667905276506,
            55.61850335438419
        ],
        [
            12.996745322661354,
            55.618421415788305
        ],
        [
            12.994895293286817,
            55.61903594849613
        ],
        [
            12.991703088957593,
            55.62145301813584
        ],
        [
            12.99126778934911,
            55.62251812121775
        ],
        [
            12.984992202749055,
            55.62161687816493
        ]
    ])

    db = get_db_backend()
    
    if (db == "postgresql"):
        convert_area_polygon_to_cs_postgresql(polygon, name)
    elif (db == "duckdb"):
        convert_area_polygon_to_cs_duckdb(polygon, name)

if __name__ == "__main__":
    main()
