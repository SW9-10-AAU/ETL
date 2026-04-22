from shapely import Polygon, MultiPolygon
from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_ls_schema,
)
from core.ls_poly_to_cs import convert_polygon_to_cellstrings


# PostgreSQL implementation
def convert_region_polygon_to_cs_postgresql(
    polygon: Polygon | MultiPolygon, name: str, skip_z21: bool = False
):
    """
    Converts a Polygon or MultiPolygon to CellStrings and inserts both into PostGIS tables.

    Args:
        polygon: A Shapely Polygon or MultiPolygon representing the region
        name: A unique identifier for this region
    """
    from db_setup.utils.connect import connect_to_postgres_db
    from psycopg import sql

    conn = connect_to_postgres_db()
    cur = conn.cursor()
    ls_schema = get_ls_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")

    # Insert region as polygon into table
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {ls_schema}.region_poly (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
            RETURNING region_id
        """
        ).format(ls_schema=sql.Identifier(ls_schema)),
        (name, polygon.wkb),
    )
    region_row = cur.fetchone()
    if region_row is None:
        raise RuntimeError("Failed to insert region geometry into PostGIS")
    region_id = int(region_row[0])
    conn.commit()
    print(f"Inserted region polygon (ID: {region_id}, Name: {name}) into PostGIS table")

    # Convert polygon to cellstring and insert into table
    print("Converting polygon to cellstrings")
    cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstrings(
        polygon, skip_z21=skip_z21
    )
    print(
        f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21)."
    )

    cur.execute(
        sql.SQL(
            """
            INSERT INTO {cs_schema}.region_cs (region_id, name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s, %s)
        """
        ).format(cs_schema=sql.Identifier(cs_schema)),
        (region_id, name, cellstring_z13, cellstring_z17, cellstring_z21),
    )
    print(f"Inserted region cellstrings (ID: {region_id}, Name: {name}) into PostGIS table")
    conn.commit()
    cur.close()

    print(
        f"Region ({name}, region_id: {region_id}) uploaded with geometry in '{ls_schema}' and cellstrings in '{cs_schema}'."
    )


# DuckDB implementation
def convert_region_polygon_to_cs_duckdb(
    polygon: Polygon | MultiPolygon, name: str, skip_z21: bool = False
):
    """
    Converts a Polygon or MultiPolygon to CellStrings and inserts both into DuckDB tables.

    Args:
        polygon: A Shapely Polygon or MultiPolygon representing the region
        name: A unique identifier for this region
        duckdb_path: Path to DuckDB database file
    """
    import duckdb
    import pyarrow as pa
    from db_setup.duckdb.pyarrow_schemas import REGION_CS_SCHEMA

    ls_schema = get_ls_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    duckdb_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(duckdb_path)

    if skip_z21:
        print("DuckDB requires z21 cells; ignoring skip_z21=True.")
        skip_z21 = False

    # Insert region polygon as geom from WKB
    conn.execute("LOAD SPATIAL;")
    region_row = conn.execute(
        f"""INSERT INTO {ls_schema}.region_poly (name, geom)
         VALUES (?, ST_GeomFromWKB(?))
         RETURNING region_id""",
        [name, polygon.wkb],
    ).fetchone()
    if region_row is None:
        raise RuntimeError("Failed to insert region geometry into DuckDB")

    region_id = int(region_row[0])
    print(f"Inserted region polygon (ID: {region_id}, Name: {name}) into DuckDB table")

    print("Converting polygon to cellstring(s)")
    _, _, cellstring_z21 = convert_polygon_to_cellstrings(polygon, skip_z21=skip_z21)
    print(f"Conversion succeeded with {len(cellstring_z21)} cells (zoom 21).")

    if cellstring_z21:
        arrow_table = pa.table(
            {
                "region_id": pa.array([region_id] * len(cellstring_z21), type=pa.int32()),
                "name": pa.array([name] * len(cellstring_z21), type=pa.string()),
                "cell_z21": pa.array(cellstring_z21, type=pa.uint64()),
            },
            schema=REGION_CS_SCHEMA,
        )
        conn.execute(f"INSERT INTO {cs_schema}.region_cs SELECT * FROM arrow_table")
        print(
            f"Inserted region cellstring(s) (ID: {region_id}, Name: {name}) into DuckDB table"
        )
    else:
        print("No region cells to insert into DuckDB table")

    conn.close()

    print(
        f"Region ({name}, region_id: {region_id}) uploaded with geometry in '{ls_schema}' and cellstring(s) in '{cs_schema}'."
    )


def main():
    """
    Add a name for the region, and replace the coordinates.
    You can draw on a map using these tools:
    - https://geojson.io/#map=6.47/55.777/10.723
    - https://www.keene.edu/campus/maps/tool/
    """
    regions = [
        (
            "small_region_high_traffic",
            [
                [11.302686710292562, 57.549104802914286],
                [11.302686710292562, 57.540218011472916],
                [11.320601618292642, 57.540218011472916],
                [11.320601618292642, 57.549104802914286],
                [11.302686710292562, 57.549104802914286],
            ],
        ),
        (
            "medium_region_high_traffic",
            [
                [11.276438684098508, 57.56958925461157],
                [11.276438684098508, 57.51939355360108],
                [11.347866356214155, 57.51939355360108],
                [11.347866356214155, 57.56958925461157],
                [11.276438684098508, 57.56958925461157],
            ],
        ),
        (
            "large_region_high_traffic",
            [
                [11.146654152435246, 57.63829370559395],
                [11.146654152435246, 57.44467119498023],
                [11.484234973055834, 57.44467119498023],
                [11.484234973055834, 57.63829370559395],
                [11.146654152435246, 57.63829370559395],
            ],
        ),
        (
            "small_region_low_traffic",
            [
                [10.902686710292562, 57.099104802914286],
                [10.902686710292562, 57.090218011472916],
                [10.920601618292642, 57.090218011472916],
                [10.920601618292642, 57.099104802914286],
                [10.902686710292562, 57.099104802914286],
            ],
        ),
        (
            "medium_region_low_traffic",
            [
                [10.876438684098508, 57.11958925461157],
                [10.876438684098508, 57.06939355360108],
                [10.947866356214155, 57.06939355360108],
                [10.947866356214155, 57.11958925461157],
                [10.876438684098508, 57.11958925461157],
            ],
        ),
        (
            "large_region_low_traffic",
            [
                [10.746654152435246, 57.18829370559395],
                [10.746654152435246, 56.99467119498023],
                [11.084234973055834, 56.99467119498023],
                [11.084234973055834, 57.18829370559395],
                [10.746654152435246, 57.18829370559395],
            ],
        ),
    ]

    db = get_db_backend()
    for name, coords in regions:
        polygon = Polygon(coords)
        if db == "postgresql":
            convert_region_polygon_to_cs_postgresql(polygon, name)
        elif db == "duckdb":
            convert_region_polygon_to_cs_duckdb(polygon, name)


if __name__ == "__main__":
    main()
