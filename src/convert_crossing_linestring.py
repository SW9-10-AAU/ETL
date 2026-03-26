from shapely import LineString

from core.ls_poly_to_cs import convert_linestring_to_cellids
from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_source_schema,
)


# PostgreSQL implementation
def convert_crossing_linestring_to_cs_postgres(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into PostGIS tables.
    """
    from psycopg import sql

    from db_setup.utils.connect import connect_to_postgres_db

    conn = connect_to_postgres_db()
    cur = conn.cursor()
    source_schema = get_source_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")

    # Insert crossing as linestring into table
    cur.execute(
        sql.SQL(
            """
            INSERT INTO {source_schema}.crossing_ls (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
            RETURNING crossing_id
        """
        ).format(source_schema=sql.Identifier(source_schema)),
        (name, linestring.wkb),
    )
    crossing_row = cur.fetchone()
    if crossing_row is None:
        raise RuntimeError("Failed to insert crossing geometry into PostGIS")
    crossing_id = int(crossing_row[0])
    conn.commit()
    print("Inserted crossing linestring into PostGIS table")

    # Convert crossing to cellstring and insert into table
    print("Converting crossing to cellstrings")
    cellstring_z13 = convert_linestring_to_cellids(linestring, 13)
    cellstring_z17 = convert_linestring_to_cellids(linestring, 17)
    cellstring_z21 = convert_linestring_to_cellids(linestring, 21)

    print(
        f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21)."
    )

    cur.execute(
        sql.SQL(
            """
            INSERT INTO {cs_schema}.crossing_cs (crossing_id, name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s, %s)
        """
        ).format(cs_schema=sql.Identifier(cs_schema)),
        (crossing_id, name, cellstring_z13, cellstring_z17, cellstring_z21),
    )
    print("Inserted crossing cellstrings into PostGIS table")
    conn.commit()
    cur.close()

    print(
        f"Crossing ({name}, crossing_id: {crossing_id}) uploaded with geometry in '{source_schema}' and cellstrings in '{cs_schema}'."
    )


# DuckDB implementation
def convert_crossing_linestring_to_cs_duckdb(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into DuckDB tables.
    """
    import duckdb
    import pyarrow as pa

    from db_setup.duckdb.pyarrow_schemas import CROSSING_CS_SCHEMA

    source_schema = get_source_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    db_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(db_path)

    # Insert crossing as linestring into table
    conn.execute("LOAD spatial;")
    crossing_row = conn.execute(
        f"""INSERT INTO {source_schema}.crossing_ls (name, geom)
           VALUES (?, ST_GeomFromWKB(?))
           RETURNING crossing_id""",
        [name, linestring.wkb],
    ).fetchone()
    if crossing_row is None:
        raise RuntimeError("Failed to insert crossing geometry into DuckDB")

    crossing_id = int(crossing_row[0])
    print("Inserted crossing linestring into DuckDB table")

    print("Converting crossing to cellstrings")
    cellstring_z21 = convert_linestring_to_cellids(linestring, 21)
    print(f"Conversion succeeded with {len(cellstring_z21)} cells (zoom 21).")

    if cellstring_z21:
        arrow_table = pa.table(
            {
                "crossing_id": pa.array(
                    [crossing_id] * len(cellstring_z21), type=pa.int32()
                ),
                "name": pa.array([name] * len(cellstring_z21), type=pa.string()),
                "cell_z21": pa.array(cellstring_z21, type=pa.uint64()),
            },
            schema=CROSSING_CS_SCHEMA,
        )
        conn.execute(f"INSERT INTO {cs_schema}.crossing_cs SELECT * FROM arrow_table")
        print("Inserted crossing cellstrings into DuckDB table")
    else:
        print("No crossing cells to insert into DuckDB table")
    conn.close()

    print(
        f"Crossing ({name}, crossing_id: {crossing_id}) uploaded with geometry in '{source_schema}' and cellstrings in '{cs_schema}'."
    )


def main():
    """
    Add a name for the crossing, and replace the coordinates.
    You can draw on a map using these tools:
    - https://geojson.io/#map=6.47/55.777/10.723
    - https://www.keene.edu/campus/maps/tool/
    """
    name = "Bornholm"
    linestring = LineString(
        [
            [14.376500767303753, 55.43940778019655],
            [14.713765894365793, 55.30487796624158],
        ]
    )

    db_backend = get_db_backend()
    if db_backend == "postgresql":
        convert_crossing_linestring_to_cs_postgres(linestring, name)
    elif db_backend == "duckdb":
        convert_crossing_linestring_to_cs_duckdb(linestring, name)


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
