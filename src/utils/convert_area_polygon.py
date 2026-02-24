from shapely import Polygon, MultiPolygon
from db_setup.utils.db_utils import get_db_backend, get_db_path_or_url, get_db_schema
from core.ls_poly_to_cs import convert_polygon_to_cellstrings

# PostgreSQL implementation
def convert_area_polygon_to_cs_postgresql(polygon: Polygon | MultiPolygon, name: str):
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
    print("Converting polygon to cellstrings using hierarchical algorithm")
    cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstrings(polygon)
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
def convert_area_polygon_to_cs_duckdb(polygon: Polygon | MultiPolygon, name: str):
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

    print("Converting polygon to cellstrings using hierarchical algorithm")
    cellstring_z13, cellstring_z17, cellstring_z21 = (
        convert_polygon_to_cellstrings(polygon)
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
    name = "Danmark-EEZ-simple"
    polygon = Polygon([
        [
              3.3024145238983067,
              56.04914012225612
            ],
            [
              8.206169870410832,
              55.07261051548602
            ],
            [
              8.42367514787702,
              55.102788220723085
            ],
            [
              8.331400181679669,
              55.395828935511105
            ],
            [
              8.021619938016102,
              55.54154017435209
            ],
            [
              8.12733825496889,
              56.72292310321092
            ],
            [
              8.519974967313118,
              57.11611396537498
            ],
            [
              9.478250778702119,
              57.20559558871216
            ],
            [
              9.900074471263054,
              57.57853657212692
            ],
            [
              10.638274336624875,
              57.76535235049957
            ],
            [
              10.447133335215852,
              57.59620927241923
            ],
            [
              10.585545784511908,
              57.251984563192366
            ],
            [
              10.3614494380314,
              56.900915020051
            ],
            [
              10.38781043982263,
              56.58645902965074
            ],
            [
              10.90850677306156,
              56.53197767050426
            ],
            [
              10.95463622732808,
              56.31326261275049
            ],
            [
              10.697588372844791,
              56.13738433556159
            ],
            [
              10.242798395262867,
              56.16675848968802
            ],
            [
              10.335079601032874,
              55.942248767520795
            ],
            [
              9.867119393554901,
              55.61978464314416
            ],
            [
              9.913256440377523,
              55.55273399908853
            ],
            [
              10.29553874788985,
              55.65325958028805
            ],
            [
              10.559180593099654,
              55.560183263468815
            ],
            [
              10.868960419076274,
              55.38459939906642
            ],
            [
              10.809622473508085,
              55.129201537041894
            ],
            [
              11.112831305643056,
              55.18942232051447
            ],
            [
              11.185333064797504,
              55.309644034762016
            ],
            [
              11.066690426841745,
              55.649542783798324
            ],
            [
              11.082982490288288,
              55.70817827531528
            ],
            [
              11.442371059269249,
              55.90162334026314
            ],
            [
              12.32024321980569,
              56.14680461634791
            ],
            [
              12.629782819985877,
              56.050192221597314
            ],
            [
              12.554221286334226,
              55.980214609802374
            ],
            [
              12.520330870669738,
              55.90230513366092
            ],
            [
              12.609361522792156,
              55.70668243562892
            ],
            [
              12.700383025941953,
              55.585285585472704
            ],
            [
              12.418252947483563,
              55.62194764459125
            ],
            [
              12.211682600074814,
              55.47547133631218
            ],
            [
              12.738260101641327,
              55.202887367591885
            ],
            [
              13.06919294823482,
              55.291196230140905
            ],
            [
              12.89434805854302,
              55.558875147589845
            ],
            [
              13.058433639490078,
              55.65640291598929
            ],
            [
              12.628761432508078,
              56.09763197657171
            ],
            [
              12.51056997528633,
              56.25690650329392
            ],
            [
              11.139371763090793,
              57.814189443910806
            ],
            [
              10.695239420373468,
              58.19952940538204
            ],
            [
              10.03189598732493,
              58.264594791170936
            ],
            [
              3.3024145238983067,
              56.04914012225612
            ]
    ])

    db = get_db_backend()
    
    if (db == "postgresql"):
        convert_area_polygon_to_cs_postgresql(polygon, name)
    elif (db == "duckdb"):
        convert_area_polygon_to_cs_duckdb(polygon, name)

if __name__ == "__main__":
    main()
