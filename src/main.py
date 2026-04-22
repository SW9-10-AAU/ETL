import math
import os
import sys

from db_setup.utils.db_utils import (
    get_cs_schema,
    get_db_backend,
    get_db_path_or_url,
    get_ls_schema,
)
from prompt_utils import should_run_step, should_run_step_with_fallback


def _get_num_workers() -> int:
    """Determine the number of worker processes to use for parallel processing, based on the total number of CPU cores available."""

    num_cores = os.cpu_count() or 4

    if num_cores <= 16:
        return min(num_cores, 16)
    else:
        return math.floor(num_cores * 0.95)  # Use 95% of cores


def _ensure_schema_names(connection, backend: str, ls_schema: str, cs_schema: str):
    schema_set = [ls_schema] if ls_schema == cs_schema else [ls_schema, cs_schema]
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
    ls_schema = get_ls_schema(backend)
    cs_schema = get_cs_schema(backend)

    print(
        f"Running ETL with backend='{backend}', ls_schema='{ls_schema}', cs_schema='{cs_schema}'"
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

    print("Connecting to DuckDB...")
    db_path = get_db_path_or_url("duckdb")
    ls_schema = get_ls_schema("duckdb")
    cs_schema = get_cs_schema("duckdb")
    num_workers = _get_num_workers()
    connection = None
    try:
        connection = duckdb.connect(database=db_path)
        print(f"Connected to DuckDB at '{db_path}'.")

        # Install and load spatial extension
        connection.execute("INSTALL spatial;")
        connection.execute("LOAD spatial;")
        print("Spatial extension installed and loaded.")
        print(f"{num_workers} workers available for parallel processing.")

        should_drop_ls_tables = should_run_step_with_fallback(
            env_var="ETL_DROP_LS",
            fallback_env_var="ETL_DROP",
            prompt_text="Do you want to drop LineString tables (points, trajectory_ls, stop_poly, region_poly, passage_ls)?",
            default_yes=False,
        )
        should_drop_cs_tables = should_run_step_with_fallback(
            env_var="ETL_DROP_CS",
            fallback_env_var="ETL_DROP",
            prompt_text="Do you want to drop CellString tables (trajectory_cs, stop_cs, region_cs, passage_cs)?",
            default_yes=False,
        )
        if should_drop_ls_tables or should_drop_cs_tables:
            drop_duckdb_tables(
                connection,
                ls_schema,
                cs_schema,
                should_drop_ls_tables,
                should_drop_cs_tables,
            )

        if should_run_step(
            "ETL_CREATE_SCHEMA", "Do you want to create/ensure schemas exist?"
        ):
            _ensure_schema_names(connection, "duckdb", ls_schema, cs_schema)

        if should_run_step("ETL_CREATE_TABLES", "Do you want to create all tables?"):
            create_duckdb_tables(connection, ls_schema, cs_schema)

        if should_run_step(
            "ETL_CREATE_POINTS",
            "Do you want to create points table?",
            default_yes=False,
        ):
            create_duckdb_points(connection, ls_schema)

        if should_run_step(
            "ETL_CONSTRUCT", "Do you want to construct trajectories and stops?"
        ):
            construct_trajectories_and_stops(
                connection, ls_schema, ls_schema, num_workers, batch_size=200
            )

        if should_run_step(
            "ETL_TRANSFORM",
            "Do you want to transform trajectories/stops to CellStrings?",
        ):
            transform_ls_trajectories_to_cs(
                connection, ls_schema, cs_schema, num_workers, batch_size=3000
            )
            transform_poly_stops_to_cs(
                connection, ls_schema, cs_schema, num_workers, batch_size=3000
            )
    except KeyboardInterrupt:
        print("\nETL interrupted. Shutting down DuckDB connection...")
        raise SystemExit(130)
    finally:
        if connection is not None:
            try:
                connection.close()
                print("DuckDB connection closed.")
            except Exception as close_error:
                print(
                    "Warning: failed to close DuckDB connection cleanly: "
                    f"{close_error}"
                )


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

    ls_schema = get_ls_schema("postgresql")
    cs_schema = get_cs_schema("postgresql")
    num_workers = min(os.cpu_count() or 4, 16)
    connection = connect_to_postgres_db()

    should_drop_ls_tables = should_run_step_with_fallback(
        env_var="ETL_DROP_LS",
        fallback_env_var="ETL_DROP",
        prompt_text="Do you want to drop LineString tables (points, trajectory_ls, stop_poly, region_poly, passage_ls)?",
        default_yes=False,
    )
    should_drop_cs_tables = should_run_step_with_fallback(
        env_var="ETL_DROP_CS",
        fallback_env_var="ETL_DROP",
        prompt_text="Do you want to drop CellString tables (trajectory_cs, stop_cs, region_cs, passage_cs)?",
        default_yes=False,
    )
    if should_drop_ls_tables or should_drop_cs_tables:
        drop_postgresql_tables(
            connection,
            ls_schema,
            cs_schema,
            should_drop_ls_tables,
            should_drop_cs_tables,
        )

    if should_run_step(
        "ETL_CREATE_SCHEMA", "Do you want to create/ensure schemas exist?"
    ):
        _ensure_schema_names(connection, "postgresql", ls_schema, cs_schema)

    if should_run_step("ETL_CREATE_TABLES", "Do you want to create all tables?"):
        create_postgresql_tables(connection, ls_schema, cs_schema)

    if should_run_step(
        "ETL_CREATE_POINTS",
        "Do you want to create points materialized view?",
        default_yes=False,
    ):
        create_postgresql_points(connection, ls_schema)

    if should_run_step(
        "ETL_CONSTRUCT", "Do you want to construct trajectories and stops?"
    ):
        construct_trajectories_and_stops(connection, ls_schema, ls_schema, num_workers)

    if should_run_step(
        "ETL_TRANSFORM", "Do you want to transform trajectories/stops to CellStrings?"
    ):
        transform_ls_trajectories_to_cs(
            connection, ls_schema, cs_schema, num_workers, batch_size=2000
        )
        transform_poly_stops_to_cs(
            connection, ls_schema, cs_schema, num_workers, batch_size=2000
        )

    connection.close()


if __name__ == "__main__":
    main()
