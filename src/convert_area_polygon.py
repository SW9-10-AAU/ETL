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
    name = "Hirtshals Havn"
    polygon = Polygon([
        [
            9.957408600980926,
            57.596587579349006
        ],
        [
            9.962974237323579,
            57.59512597883409
        ],
        [
            9.962451528895201,
            57.594845859031096
        ],
        [
            9.959146662706189,
            57.59431272183227
        ],
        [
            9.960933988298933,
            57.59390608651191
        ],
        [
            9.964171408238371,
            57.59233372049468
        ],
        [
            9.964879593851009,
            57.592378904374726
        ],
        [
            9.965284271342796,
            57.59261385964672
        ],
        [
            9.962080574527675,
            57.59422235882076
        ],
        [
            9.962755037015029,
            57.59450248342611
        ],
        [
            9.965115655721206,
            57.59398741393963
        ],
        [
            9.9666837810054,
            57.592857850053406
        ],
        [
            9.967004150686904,
            57.592939179825095
        ],
        [
            9.966329688198641,
            57.59450248342611
        ],
        [
            9.968100152228743,
            57.5944030846195
        ],
        [
            9.969027538148964,
            57.59424043144102
        ],
        [
            9.971455603104687,
            57.59331871635575
        ],
        [
            9.972012034656444,
            57.59317413147778
        ],
        [
            9.971995173094797,
            57.59278555677011
        ],
        [
            9.975266316159491,
            57.591213142309726
        ],
        [
            9.97622742520403,
            57.59180958338297
        ],
        [
            9.972248096527665,
            57.593878977328956
        ],
        [
            9.97287197432803,
            57.59412295924855
        ],
        [
            9.976109394268406,
            57.59410488656991
        ],
        [
            9.97617684051707,
            57.594357903254064
        ],
        [
            9.976514071761244,
            57.59436693953165
        ],
        [
            9.982432480089301,
            57.5915565489708
        ],
        [
            9.983983743810825,
            57.59233372049468
        ],
        [
            9.980761865486926,
            57.59394905212915
        ],
        [
            9.980357187995025,
            57.594509303392414
        ],
        [
            9.977676199606435,
            57.59490689582745
        ],
        [
            9.975197549090439,
            57.59595507311832
        ],
        [
            9.970830404483877,
            57.596162897807744
        ],
        [
            9.969903018562775,
            57.59673215065715
        ],
        [
            9.959819804374092,
            57.59750017609474
        ],
        [
            9.957408600980926,
            57.596587579349006
        ]
    ])

    db = get_db_backend()
    
    if (db == "postgresql"):
        convert_area_polygon_to_cs_postgresql(polygon, name)
    elif (db == "duckdb"):
        convert_area_polygon_to_cs_duckdb(polygon, name)

if __name__ == "__main__":
    main()
