from shapely import from_wkb
from core.ls_poly_to_cs import convert_polygon_to_cellstrings
from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_ls_schema,
)


# PostgreSQL implementation
def convert_region_polygons_to_cs_postgres():
    """
    Convert all region polygons in DB to cellstrings and upload to PostGIS.
    """
    from db_setup.utils.connect import connect_to_postgres_db
    from psycopg import sql

    ls_schema = get_ls_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")
    conn = connect_to_postgres_db()
    cur = conn.cursor()

    # Fetch all region polygons from benchmark.region_poly (not already converted)
    cur.execute(
        sql.SQL(
            """
            SELECT region_poly.region_id, region_poly.name, ST_AsBinary(region_poly.geom)
            FROM {ls_schema}.region_poly as region_poly
            LEFT JOIN {cs_schema}.region_cs AS region_cs ON region_poly.region_id = region_cs.region_id
            WHERE region_cs.region_id IS NULL
            ORDER BY region_poly.region_id;
        """
        ).format(
            ls_schema=sql.Identifier(ls_schema),
            cs_schema=sql.Identifier(cs_schema),
        )
    )

    rows = cur.fetchall()
    print(f"Fetched {len(rows)} region polygon(s) from {ls_schema}.region_poly")

    for row in rows:
        region_id, name, geom_wkb = row
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
                INSERT INTO {cs_schema}.region_cs (region_id, name, cellstring_z13, cellstring_z17, cellstring_z21)
                VALUES (%s, %s, %s, %s, %s)
            """
            ).format(cs_schema=sql.Identifier(cs_schema)),
            (region_id, name, cellstring_z13, cellstring_z17, cellstring_z21),
        )
        conn.commit()
        print(f"Inserted region cellstrings for {name} into PostGIS table")

    print("Converted all region polygons to cellstrings and uploaded to PostGIS.")
    cur.close()


# DuckDB implementation
def convert_region_polygons_to_cs_duckdb():
    """
    Convert all region polygons in DB to cellstrings and upload to DuckDB.
    """
    import duckdb
    import pyarrow as pa
    from db_setup.duckdb.pyarrow_schemas import REGION_CS_SCHEMA

    db_path = get_db_path_or_url("duckdb")
    ls_schema = get_ls_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    conn = duckdb.connect(database=db_path)

    # Fetch all region polygons from region_poly table (not already converted)
    conn.execute("LOAD spatial;")
    rows = conn.execute(
        f"""
            SELECT region_poly.region_id, region_poly.name, ST_AsWKB(geom)
            FROM {ls_schema}.region_poly
            LEFT JOIN {cs_schema}.region_cs ON region_poly.region_id = region_cs.region_id
            WHERE region_cs.region_id IS NULL
            ORDER BY region_poly.region_id;
        """
    ).fetchall()
    print(f"Fetched {len(rows)} region polygon(s) from {ls_schema}.region_poly table")

    for row in rows:
        region_id, name, geom_wkb = row
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
                    "region_id": pa.array(
                        [region_id] * len(cellstring_z21), type=pa.int32()
                    ),
                    "name": pa.array([name] * len(cellstring_z21), type=pa.string()),
                    "cell_z21": pa.array(cellstring_z21, type=pa.uint64()),
                },
                schema=REGION_CS_SCHEMA,
            )
            conn.execute(f"INSERT INTO {cs_schema}.region_cs SELECT * FROM arrow_table")
            print(f"Inserted region cellstrings for {name} into DuckDB table")
        else:
            print(f"No region cells to insert for {name}")

    print("Converted all region polygons to cellstrings and uploaded to DuckDB.")
    conn.close()


def main():
    """
    Convert all region polygons in DB to cellstrings and upload to PostGIS.
    """
    db_backend = get_db_backend()
    if db_backend == "postgresql":
        convert_region_polygons_to_cs_postgres()
    elif db_backend == "duckdb":
        convert_region_polygons_to_cs_duckdb()


if __name__ == "__main__":
    main()
