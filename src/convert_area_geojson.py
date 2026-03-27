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


def convert_area_geojson_to_cs(geojson_path: str, name: str, skip_z21: bool = False):
    """
    Loads a GeoJSON file and converts its geometry to cellstrings.

    Supports FeatureCollection (uses first feature) and single Feature.
    Handles Polygon and MultiPolygon geometries.

    Args:
        geojson_path: Path to GeoJSON file
        name: Name to store the area under in the database
    """
    db_backend = get_db_backend()

    if db_backend == "duckdb" and skip_z21:
        print("DuckDB requires z21 cells; overriding skip_z21=False.")
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
            convert_area_polygon_to_cs_postgresql(multiPolygon, name, skip_z21=skip_z21)
        elif db_backend == "duckdb":
            convert_area_polygon_to_cs_duckdb(multiPolygon, name, skip_z21=skip_z21)
    else:
        poly: Polygon = cast(Polygon, geometry)
        print(f"Polygon contains {len(poly.exterior.coords)} points")
        if db_backend == "postgresql":
            convert_area_polygon_to_cs_postgresql(poly, name, skip_z21=skip_z21)
        elif db_backend == "duckdb":
            convert_area_polygon_to_cs_duckdb(poly, name, skip_z21=skip_z21)


def main():
    """
    Load Denmark EEZ from GeoJSON and convert to cellstrings.
    """
    geojson_path = "./geojson/Denmark_EEZ_gml.geojson"
    name = "Denmark-EEZ"

    convert_area_geojson_to_cs(geojson_path, name, skip_z21=True)


if __name__ == "__main__":
    main()
