from shapely import Polygon, MultiPolygon
from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_ls_schema,
)
from core.ls_poly_to_cs import convert_polygon_to_cellstrings


# PostgreSQL implementation
def convert_area_polygon_to_cs_postgresql(
    polygon: Polygon | MultiPolygon,
    name: str,
    skip_z21: bool = False,
    zoom_levels: tuple[int, int, int] = (13, 17, 21)
):
    """
    Converts a Polygon or MultiPolygon to CellStrings and inserts both into PostGIS tables.

    Args:
        polygon: A Shapely Polygon or MultiPolygon representing the area
        name: A unique identifier for this area
        skip_z21: If True, skip the finest zoom level
        zoom_levels: Tuple of (zoom1, zoom2, zoom3) where zoom1 < zoom2 < zoom3.
                     Defaults to (13, 17, 21).
    """
    from db_setup.utils.connect import connect_to_postgres_db
    from psycopg import sql

    conn = connect_to_postgres_db()
    cur = conn.cursor()
    ls_schema = get_ls_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")

    # Insert area as polygon into table
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {ls_schema}.area_poly (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
            RETURNING area_id
        """
        ).format(ls_schema=sql.Identifier(ls_schema)),
        (name, polygon.wkb),
    )
    area_row = cur.fetchone()
    if area_row is None:
        raise RuntimeError("Failed to insert area geometry into PostGIS")
    area_id = int(area_row[0])
    conn.commit()
    print(f"Inserted area polygon (ID: {area_id}, Name: {name}) into PostGIS table")

    # Convert polygon to cellstring and insert into table
    print(f"Converting polygon to cellstrings at zoom levels {zoom_levels}")
    cellstring_z1, cellstring_z2, cellstring_z3 = convert_polygon_to_cellstrings(
        polygon, skip_z21=skip_z21, zoom_levels=zoom_levels
    )
    print(
        f"Conversion succeeded with {len(cellstring_z1)} cells (zoom {zoom_levels[0]}), "
        f"{len(cellstring_z2)} cells (zoom {zoom_levels[1]}), and "
        f"{len(cellstring_z3)} cells (zoom {zoom_levels[2]})."
    )

    cur.execute(
        sql.SQL(
            """
            INSERT INTO {cs_schema}.area_cs (area_id, name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s, %s)
        """
        ).format(cs_schema=sql.Identifier(cs_schema)),
        (area_id, name, cellstring_z1, cellstring_z2, cellstring_z3),
    )
    print(f"Inserted area cellstrings (ID: {area_id}, Name: {name}) into PostGIS table")
    conn.commit()
    cur.close()

    print(
        f"Area ({name}, area_id: {area_id}) uploaded with geometry in '{ls_schema}' and cellstrings in '{cs_schema}'."
    )


# DuckDB implementation
def convert_area_polygon_to_cs_duckdb(
    polygon: Polygon | MultiPolygon,
    name: str,
    skip_z21: bool = False,
    zoom_levels: tuple[int, int, int] = (13, 17, 21)
):
    """
    Converts a Polygon or MultiPolygon to CellStrings and inserts both into DuckDB tables.

    Args:
        polygon: A Shapely Polygon or MultiPolygon representing the area
        name: A unique identifier for this area
        skip_z21: If True, skip the finest zoom level (ignored for DuckDB)
        zoom_levels: Tuple of (zoom1, zoom2, zoom3). DuckDB only stores zoom3 cells.
                     Defaults to (13, 17, 21).
    """
    import duckdb
    import pyarrow as pa
    from db_setup.duckdb.pyarrow_schemas import AREA_CS_SCHEMA

    ls_schema = get_ls_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    duckdb_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(duckdb_path)

    if skip_z21:
        print(f"DuckDB requires finest zoom level (z{zoom_levels[2]}) cells; ignoring skip_z21=True.")
        skip_z21 = False

    # Insert area polygon as geom from WKB
    conn.execute("LOAD SPATIAL;")
    area_row = conn.execute(
        f"""INSERT INTO {ls_schema}.area_poly (name, geom)
         VALUES (?, ST_GeomFromWKB(?))
         RETURNING area_id""",
        [name, polygon.wkb],
    ).fetchone()
    if area_row is None:
        raise RuntimeError("Failed to insert area geometry into DuckDB")

    area_id = int(area_row[0])
    print(f"Inserted area polygon (ID: {area_id}, Name: {name}) into DuckDB table")

    print(f"Converting polygon to cellstring(s) at zoom levels {zoom_levels}")
    _, _, cellstring_finest = convert_polygon_to_cellstrings(
        polygon, skip_z21=skip_z21, zoom_levels=zoom_levels
    )
    print(f"Conversion succeeded with {len(cellstring_finest)} cells (zoom {zoom_levels[2]}).")

    if cellstring_finest:
        arrow_table = pa.table(
            {
                "area_id": pa.array([area_id] * len(cellstring_finest), type=pa.int32()),
                "name": pa.array([name] * len(cellstring_finest), type=pa.string()),
                "cell_z21": pa.array(cellstring_finest, type=pa.uint64()),
            },
            schema=AREA_CS_SCHEMA,
        )
        conn.execute(f"INSERT INTO {cs_schema}.area_cs SELECT * FROM arrow_table")
        print(
            f"Inserted area cellstring(s) (ID: {area_id}, Name: {name}) into DuckDB table"
        )
    else:
        print("No area cells to insert into DuckDB table")

    conn.close()

    print(
        f"Area ({name}, area_id: {area_id}) uploaded with geometry in '{ls_schema}' and cellstring(s) in '{cs_schema}'."
    )


def main():
    """
    Add a name for the area, and replace the coordinates.
    You can draw on a map using these tools:
    - https://geojson.io/#map=6.47/55.777/10.723
    - https://www.keene.edu/campus/maps/tool/

    To generate area coverage at zoom 19 instead of 21, pass zoom_levels=(13, 17, 19).
    This is useful for large areas like EEZ where z21 would be too granular.

    Example usage:
        # Default zoom levels (13, 17, 21)
        convert_area_polygon_to_cs_postgresql(polygon, name)

        # For large areas like EEZ, use z19 as the finest level
        convert_area_polygon_to_cs_postgresql(polygon, name, zoom_levels=(13, 17, 19))
    """
    name = "Hals-Egense"
    polygon = Polygon(
        [
            [10.296876838195601, 56.99190737430766],
            [10.28869531892542, 56.98396759632632],
            [10.299050054251637, 56.98341035533278],
            [10.300328416637655, 56.98724121873309],
            [10.309021280862993, 56.98473378907431],
            [10.308765608386068, 56.979439771840674],
            [10.324105957017338, 56.97748915469052],
            [10.326279173073317, 56.98731086714611],
            [10.296876838195601, 56.99190737430766],
        ]
    )

    # Set to True to use z19 instead of z21 for large areas
    use_z19_for_large_areas = False

    zoom_levels = (13, 17, 19) if use_z19_for_large_areas else (13, 17, 21)

    db = get_db_backend()

    if db == "postgresql":
        convert_area_polygon_to_cs_postgresql(polygon, name, zoom_levels=zoom_levels)
    elif db == "duckdb":
        convert_area_polygon_to_cs_duckdb(polygon, name, zoom_levels=zoom_levels)


if __name__ == "__main__":
    main()
