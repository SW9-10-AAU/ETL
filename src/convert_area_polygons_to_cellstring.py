from shapely import from_wkb
from core.ls_poly_to_cs import convert_polygon_to_cellstrings
from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_ls_schema,
)


# PostgreSQL implementation
def convert_area_polygons_to_cs_postgres():
    """
    Convert all area polygons in DB to cellstrings and upload to PostGIS.
    """
    from db_setup.utils.connect import connect_to_postgres_db
    from psycopg import sql

    ls_schema = get_ls_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")
    conn = connect_to_postgres_db()
    cur = conn.cursor()

    # Fetch all area polygons from benchmark.area_poly (not already converted)
    cur.execute(
        sql.SQL(
            """
            SELECT area_poly.area_id, area_poly.name, ST_AsBinary(area_poly.geom)
            FROM {ls_schema}.area_poly as area_poly
            LEFT JOIN {cs_schema}.area_cs AS area_cs ON area_poly.area_id = area_cs.area_id
            WHERE area_cs.area_id IS NULL
            ORDER BY area_poly.area_id;
        """
        ).format(
            ls_schema=sql.Identifier(ls_schema),
            cs_schema=sql.Identifier(cs_schema),
        )
    )

    rows = cur.fetchall()
    print(f"Fetched {len(rows)} area polygon(s) from {ls_schema}.area_poly")

    for row in rows:
        area_id, name, geom_wkb = row
        # from_wkb returns the correct type (Polygon or MultiPolygon)
        polygon = from_wkb(geom_wkb)

        # Convert polygon to cellstring and insert into table
        print("Converting polygon to cellstrings")
        cellstring_z13, cellstring_z17, cellstring_z21 = convert_polygon_to_cellstrings(
            polygon
        )

        print(
            f"Conversion of {name} succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21)."
        )

        cur.execute(
            sql.SQL(
                """
                INSERT INTO {cs_schema}.area_cs (area_id, name, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s, %s)
            """
            ).format(cs_schema=sql.Identifier(cs_schema)),
            (area_id, name, cellstring_z13, cellstring_z17, cellstring_z21),
        )
        conn.commit()
        print(f"Inserted area cellstrings for {name} into PostGIS table")

    print("Converted all area polygons to cellstrings and uploaded to PostGIS.")
    cur.close()


# DuckDB implementation
def convert_area_polygons_to_cs_duckdb():
    """
    Convert all area polygons in DB to cellstrings and upload to DuckDB.
    """
    import duckdb
    import pyarrow as pa
    from db_setup.duckdb.pyarrow_schemas import AREA_CS_SCHEMA

    db_path = get_db_path_or_url("duckdb")
    ls_schema = get_ls_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    conn = duckdb.connect(database=db_path)

    # Fetch all area polygons from area_poly table (not already converted)
    conn.execute("LOAD spatial;")
    rows = conn.execute(
        f"""
            SELECT area_poly.area_id, area_poly.name, ST_AsWKB(geom)
            FROM {ls_schema}.area_poly
            LEFT JOIN {cs_schema}.area_cs ON area_poly.area_id = area_cs.area_id
            WHERE area_cs.area_id IS NULL
            ORDER BY area_poly.area_id;
        """
    ).fetchall()
    print(f"Fetched {len(rows)} area polygon(s) from {ls_schema}.area_poly table")

    for row in rows:
        area_id, name, geom_wkb = row
        polygon = from_wkb(geom_wkb)

        # Convert polygon to cellstring and insert into table
        print("Converting polygon to cellstrings")
        _, _, cellstring_z21 = convert_polygon_to_cellstrings(polygon, skip_z21=False)
        print(
            f"Conversion of {name} succeeded with {len(cellstring_z21)} cells (zoom 21)."
        )

        if cellstring_z21:
            arrow_table = pa.table(
                {
                    "area_id": pa.array(
                        [area_id] * len(cellstring_z21), type=pa.int32()
                    ),
                    "name": pa.array([name] * len(cellstring_z21), type=pa.string()),
                    "cell_z21": pa.array(cellstring_z21, type=pa.uint64()),
                },
                schema=AREA_CS_SCHEMA,
            )
            conn.execute(f"INSERT INTO {cs_schema}.area_cs SELECT * FROM arrow_table")
            print(f"Inserted area cellstrings for {name} into DuckDB table")
        else:
            print(f"No area cells to insert for {name}")

    print("Converted all area polygons to cellstrings and uploaded to DuckDB.")
    conn.close()


def main():
    """
    Convert all area polygons in DB to cellstrings and upload to PostGIS.
    """
    db_backend = get_db_backend()
    if db_backend == "postgresql":
        convert_area_polygons_to_cs_postgres()
    elif db_backend == "duckdb":
        convert_area_polygons_to_cs_duckdb()


if __name__ == "__main__":
    main()
