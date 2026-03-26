import math
import os
import sys

from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_source_schema,
)
from prompt_utils import should_run_step, should_run_step_with_fallback


def _get_num_workers() -> int:
    """Determine the number of worker processes to use for parallel processing, based on the total number of CPU cores available."""

    num_cores = os.cpu_count() or 4

    if num_cores <= 16:
        return min(num_cores, 16)
    else:
        return math.floor(num_cores * 0.95)  # Use 95% of cores


def _ensure_schema_names(connection, backend: str, source_schema: str, cs_schema: str):
    schema_set = (
        [source_schema] if source_schema == cs_schema else [source_schema, cs_schema]
    )
    if backend == "duckdb":
        from db_setup.duckdb.create_duckdb_tables import create_duckdb_schema

        for schema_name in schema_set:
            create_duckdb_schema(connection, schema_name)
        return

    if backend == "postgresql":
        from db_setup.postgresql.create_postgresql_tables import (
            create_postgresql_schema,
        )

        for schema_name in schema_set:
            create_postgresql_schema(connection, schema_name)
        return

    raise ValueError(f"Unsupported database backend: {backend}")


def main():
    backend = get_db_backend()
    source_schema = get_source_schema(backend)
    cs_schema = get_cs_schema(backend)

    print(
        f"Running ETL with backend='{backend}', source_schema='{source_schema}', cs_schema='{cs_schema}'"
    )

    if not should_run_step("ETL_PROCEED", "Do you want to proceed?", default_yes=True):
        print("Aborting.")
        sys.exit(0)

    if backend == "duckdb":
        main_duckdb()
    elif backend == "postgresql":
        main_postgres()
    else:
        raise ValueError(f"Unsupported database backend: {backend}")


def main_duckdb():
    import duckdb

    from db_setup.duckdb.create_duckdb_points import create_duckdb_points
    from db_setup.duckdb.create_duckdb_tables import create_duckdb_tables
    from db_setup.duckdb.drop_duckdb_tables import drop_duckdb_tables
    from duckdb_construct_trajs_stops import construct_trajectories_and_stops
    from duckdb_transform_ls_to_cs import (
        transform_ls_trajectories_to_cs,
        transform_poly_stops_to_cs,
    )

    db_path = get_db_path_or_url("duckdb")
    source_schema = get_source_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    num_workers = _get_num_workers()
    connection = duckdb.connect(database=db_path)

    # Install and load spatial extension
    connection.execute("INSTALL spatial;")
    connection.execute("LOAD spatial;")

    should_drop_source_tables = should_run_step_with_fallback(
        env_var="ETL_DROP_LS",
        fallback_env_var="ETL_DROP",
        prompt_text="Do you want to drop source/LS tables?",
        default_yes=False,
    )
    should_drop_cs_tables = should_run_step_with_fallback(
        env_var="ETL_DROP_CS",
        fallback_env_var="ETL_DROP",
        prompt_text="Do you want to drop CellString tables?",
        default_yes=False,
    )
    if should_drop_source_tables or should_drop_cs_tables:
        drop_duckdb_tables(
            connection,
            source_schema,
            cs_schema,
            should_drop_source_tables,
            should_drop_cs_tables,
        )

    if should_run_step(
        "ETL_CREATE_SCHEMA", "Do you want to create/ensure schemas exist?"
    ):
        _ensure_schema_names(connection, "duckdb", source_schema, cs_schema)

    if should_run_step("ETL_CREATE_TABLES", "Do you want to create all tables?"):
        create_duckdb_tables(connection, source_schema, cs_schema)

    if should_run_step(
        "ETL_CREATE_POINTS",
        "Do you want to create points table?",
        default_yes=False,
    ):
        create_duckdb_points(connection, source_schema)

    if should_run_step(
        "ETL_CONSTRUCT", "Do you want to construct trajectories and stops?"
    ):
        construct_trajectories_and_stops(
            connection, source_schema, source_schema, num_workers
        )

    if should_run_step(
        "ETL_TRANSFORM", "Do you want to transform trajectories/stops to CellStrings?"
    ):
        transform_ls_trajectories_to_cs(
            connection, source_schema, cs_schema, num_workers, batch_size=2000
        )
        transform_poly_stops_to_cs(
            connection, source_schema, cs_schema, num_workers, batch_size=2000
        )

    connection.close()


def main_postgres():
    from db_setup.postgresql.create_postgresql_tables import (
        create_postgresql_points,
        create_postgresql_tables,
    )
    from db_setup.postgresql.drop_postgresql_tables import drop_postgresql_tables
    from db_setup.utils.connect import connect_to_postgres_db
    from pg_construct_trajs_stops import construct_trajectories_and_stops
    from pg_transform_ls_to_cs import (
        transform_ls_trajectories_to_cs,
        transform_poly_stops_to_cs,
    )

    source_schema = get_source_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")
    num_workers = min(os.cpu_count() or 4, 16)
    connection = connect_to_postgres_db()

    should_drop_source_tables = should_run_step_with_fallback(
        env_var="ETL_DROP_LS",
        fallback_env_var="ETL_DROP",
        prompt_text="Do you want to drop source/LS tables?",
        default_yes=False,
    )
    should_drop_cs_tables = should_run_step_with_fallback(
        env_var="ETL_DROP_CS",
        fallback_env_var="ETL_DROP",
        prompt_text="Do you want to drop CellString tables?",
        default_yes=False,
    )
    if should_drop_source_tables or should_drop_cs_tables:
        drop_postgresql_tables(
            connection,
            source_schema,
            cs_schema,
            should_drop_source_tables,
            should_drop_cs_tables,
        )

    if should_run_step(
        "ETL_CREATE_SCHEMA", "Do you want to create/ensure schemas exist?"
    ):
        _ensure_schema_names(connection, "postgresql", source_schema, cs_schema)

    if should_run_step("ETL_CREATE_TABLES", "Do you want to create all tables?"):
        create_postgresql_tables(connection, source_schema, cs_schema)

    if should_run_step(
        "ETL_CREATE_POINTS",
        "Do you want to create points materialized view?",
        default_yes=False,
    ):
        create_postgresql_points(connection, source_schema)

    if should_run_step(
        "ETL_CONSTRUCT", "Do you want to construct trajectories and stops?"
    ):
        construct_trajectories_and_stops(
            connection, source_schema, source_schema, num_workers
        )

    if should_run_step(
        "ETL_TRANSFORM", "Do you want to transform trajectories/stops to CellStrings?"
    ):
        transform_ls_trajectories_to_cs(
            connection, source_schema, cs_schema, num_workers, batch_size=2000
        )
        transform_poly_stops_to_cs(
            connection, source_schema, cs_schema, num_workers, batch_size=2000
        )

    connection.close()


if __name__ == "__main__":
    main()
