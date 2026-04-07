import json
from pathlib import Path
from typing import cast

from shapely import MultiPolygon, Polygon
from shapely.geometry import shape

from convert_area_polygon import (
    convert_area_polygon_to_cs_duckdb,
    convert_area_polygon_to_cs_postgresql,
)
from db_setup.utils.db_utils import get_db_backend


def convert_area_geojson_to_cs(
    geojson_path: str,
    name: str,
    skip_z21: bool = False,
    zoom_levels: tuple[int, int, int] = (13, 17, 21)
):
    """
    Loads a GeoJSON file and converts its geometry to cellstrings.

    Supports FeatureCollection (uses first feature) and single Feature.
    Handles Polygon and MultiPolygon geometries.

    Args:
        geojson_path: Path to GeoJSON file
        name: Name to store the area under in the database
        skip_z21: If True, skip the finest zoom level
        zoom_levels: Tuple of (zoom1, zoom2, zoom3) where zoom1 < zoom2 < zoom3.
                     Defaults to (13, 17, 21).
    """
    db_backend = get_db_backend()

    if db_backend == "duckdb" and skip_z21:
        print(f"DuckDB requires finest zoom level (z{zoom_levels[2]}) cells; overriding skip_z21=False.")
        skip_z21 = False

    # Load GeoJSON file
    geojson_file = Path(geojson_path)
    if not geojson_file.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")

    with open(geojson_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Extract geometry
    if data["type"] == "FeatureCollection":
        if not data["features"]:
            raise ValueError("FeatureCollection is empty")
        geometry_data = data["features"][0]["geometry"]
        print(f"Loaded FeatureCollection with {len(data['features'])} feature(s)")
    elif data["type"] == "Feature":
        geometry_data = data["geometry"]
        print("Loaded single Feature")
    else:
        raise ValueError(f"Unsupported GeoJSON type: {data['type']}")

    # Convert to Shapely geometry
    geometry = shape(geometry_data)
    print(f"Geometry type: {geometry.geom_type}")

    if geometry.geom_type not in ["Polygon", "MultiPolygon"]:
        raise ValueError(f"Expected Polygon or MultiPolygon, got {geometry.geom_type}")

    if geometry.geom_type == "MultiPolygon":
        multiPolygon: MultiPolygon = cast(MultiPolygon, geometry)
        num_polygons = len(multiPolygon.geoms)
        total_points = sum(len(poly.exterior.coords) for poly in multiPolygon.geoms)
        print(
            f"MultiPolygon contains {num_polygons} polygon(s) with {total_points} total points"
        )
        if db_backend == "postgresql":
            convert_area_polygon_to_cs_postgresql(
                multiPolygon, name, skip_z21=skip_z21, zoom_levels=zoom_levels
            )
        elif db_backend == "duckdb":
            convert_area_polygon_to_cs_duckdb(
                multiPolygon, name, skip_z21=skip_z21, zoom_levels=zoom_levels
            )
    else:
        poly: Polygon = cast(Polygon, geometry)
        print(f"Polygon contains {len(poly.exterior.coords)} points")
        if db_backend == "postgresql":
            convert_area_polygon_to_cs_postgresql(
                poly, name, skip_z21=skip_z21, zoom_levels=zoom_levels
            )
        elif db_backend == "duckdb":
            convert_area_polygon_to_cs_duckdb(
                poly, name, skip_z21=skip_z21, zoom_levels=zoom_levels
            )


def main():
    """
    Load Denmark EEZ from GeoJSON and convert to cellstrings.

    For large areas like EEZ, use zoom_levels=(13, 17, 19) instead of (13, 17, 21)
    to reduce the number of cells and improve CoverageByMMSI query performance.

    Example:
        # Use z19 for EEZ (large area)
        convert_area_geojson_to_cs(geojson_path, name, zoom_levels=(13, 17, 19))

        # Use z21 for smaller areas (default)
        convert_area_geojson_to_cs(geojson_path, name)
    """
    geojson_path = "./geojson/Denmark_EEZ_gml.geojson"
    name = "Denmark-EEZ"

    # Use z19 for large areas like EEZ to reduce cell count
    convert_area_geojson_to_cs(geojson_path, name, zoom_levels=(13, 17, 19))


if __name__ == "__main__":
    main()
