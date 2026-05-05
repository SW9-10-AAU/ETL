from shapely import LineString

from core.ls_poly_to_cs import convert_linestring_to_cellids
from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_ls_schema,
)


# PostgreSQL implementation
def convert_passage_linestring_to_cs_postgres(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into PostGIS tables.
    """
    from psycopg import sql

    from db_setup.utils.connect import connect_to_postgres_db

    conn = connect_to_postgres_db()
    cur = conn.cursor()
    ls_schema = get_ls_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")

    # Insert passage as linestring into table
    cur.execute(
        sql.SQL("""
            INSERT INTO {ls_schema}.passage_ls (name, geom)
            VALUES (%s, ST_GeomFromWKB(%s, 4326))
            RETURNING passage_id
        """).format(ls_schema=sql.Identifier(ls_schema)),
        (name, linestring.wkb),
    )
    passage_row = cur.fetchone()
    if passage_row is None:
        raise RuntimeError("Failed to insert passage geometry into PostGIS")
    passage_id = int(passage_row[0])
    conn.commit()
    print("Inserted passage linestring into PostGIS table")

    # Convert passage to cellstring and insert into table
    print("Converting passage to cellstrings")
    cellstring_z13 = convert_linestring_to_cellids(linestring, 13)
    cellstring_z17 = convert_linestring_to_cellids(linestring, 17)
    cellstring_z21 = convert_linestring_to_cellids(linestring, 21)

    print(
        f"Conversion succeeded with {len(cellstring_z13)} cells (zoom 13), {len(cellstring_z17)} cells (zoom 17), and {len(cellstring_z21)} cells (zoom 21)."
    )

    cur.execute(
        sql.SQL("""
            INSERT INTO {cs_schema}.passage_cs (passage_id, name, cellstring_z13, cellstring_z17, cellstring_z21)
            VALUES (%s, %s, %s, %s, %s)
        """).format(cs_schema=sql.Identifier(cs_schema)),
        (passage_id, name, cellstring_z13, cellstring_z17, cellstring_z21),
    )
    print("Inserted passage cellstrings into PostGIS table")
    conn.commit()
    cur.close()

    print(
        f"Passage ({name}, passage_id: {passage_id}) uploaded with geometry in '{ls_schema}' and cellstrings in '{cs_schema}'."
    )


# DuckDB implementation
def convert_passage_linestring_to_cs_duckdb(linestring: LineString, name: str):
    """
    Converts a LineString to a CellString and inserts both into DuckDB tables.
    """
    import duckdb
    import pyarrow as pa

    from db_setup.duckdb.pyarrow_schemas import PASSAGE_CS_SCHEMA

    ls_schema = get_ls_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    db_path = get_db_path_or_url("duckdb")
    conn = duckdb.connect(db_path)

    # Insert passage as linestring into table
    conn.execute("LOAD spatial;")
    passage_row = conn.execute(
        f"""INSERT INTO {ls_schema}.passage_ls (name, geom)
           VALUES (?, ST_GeomFromWKB(?))
           RETURNING passage_id""",
        [name, linestring.wkb],
    ).fetchone()
    if passage_row is None:
        raise RuntimeError("Failed to insert passage geometry into DuckDB")

    passage_id = int(passage_row[0])
    print("Inserted passage linestring into DuckDB table")

    print("Converting passage to cellstrings")
    cellstring_z21 = convert_linestring_to_cellids(linestring, 21)
    print(f"Conversion succeeded with {len(cellstring_z21)} cells (zoom 21).")

    if cellstring_z21:
        arrow_table = pa.table(
            {
                "passage_id": pa.array(
                    [passage_id] * len(cellstring_z21), type=pa.int32()
                ),
                "name": pa.array([name] * len(cellstring_z21), type=pa.string()),
                "cell_z21": pa.array(cellstring_z21, type=pa.uint64()),
            },
            schema=PASSAGE_CS_SCHEMA,
        )
        conn.execute(f"INSERT INTO {cs_schema}.passage_cs SELECT * FROM arrow_table")
        print("Inserted passage cellstrings into DuckDB table")
    else:
        print("No passage cells to insert into DuckDB table")
    conn.close()

    print(
        f"Passage ({name}, passage_id: {passage_id}) uploaded with geometry in '{ls_schema}' and cellstrings in '{cs_schema}'."
    )


def main():
    """
    Add a name for the passage, and replace the coordinates.
    You can draw on a map using these tools:
    - https://geojson.io/#map=6.47/55.777/10.723
    - https://www.keene.edu/campus/maps/tool/
    """
    passages = [
        ("Skagen", [[10.6547341, 57.7469023], [11.6832043, 57.8390526]]),
        ("Kiel", [[10.6398103, 54.707243], [10.7675855, 54.518765]]),
        ("Storebælt Nord", [[10.6218303, 55.7653698], [10.8689216, 55.7433214]]),
        ("Storebælt Syd", [[10.7168396, 54.727667], [10.9955755, 54.7712645]]),
        ("Sundet Nord", [[12.2843156, 56.1277941], [12.4463371, 56.3009201]]),
        ("Sundet Syd", [[12.4472826, 55.2994701], [12.812782, 55.3828301]]),
        ("Bornholms Gate", [[14.2737822, 55.4560996], [14.7232539, 55.2377075]]),
        ("Kadetrenden", [[11.9681072, 54.5605233], [12.4027844, 54.3751052]]),
        ("Fynshoved", [[10.5992236, 55.6179156], [10.6111518, 55.7628973]]),
        ("Lillebælt Nord", [[10.0298125, 55.6977269], [10.1637184, 55.6449764]]),
        ("Lillebælt Syd", [[10.4195311, 54.8146529], [9.9537696, 54.7766516]]),
        ("Bornholm Syd", [[14.8267825, 55.0423356], [14.8352947, 54.4869928]]),
    ]

    db_backend = get_db_backend()
    for name, coords in passages:
        linestring = LineString(coords)
        if db_backend == "postgresql":
            convert_passage_linestring_to_cs_postgres(linestring, name)
        elif db_backend == "duckdb":
            convert_passage_linestring_to_cs_duckdb(linestring, name)


if __name__ == "__main__":
    main()
