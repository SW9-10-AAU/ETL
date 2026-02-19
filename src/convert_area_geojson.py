from typing import cast
from dotenv import load_dotenv
import json
from pathlib import Path
from shapely import MultiPolygon, Polygon
from shapely.geometry import shape
from convert_area_polygon import convert_area_polygon_to_cs

def convert_area_geojson_to_cs(geojson_path: str, name: str):
    """
    Loads a GeoJSON file and converts its geometry to cellstrings.

    Supports FeatureCollection (uses first feature) and single Feature.
    Handles Polygon and MultiPolygon geometries.

    Args:
        geojson_path: Path to GeoJSON file
        name: Name to store the area under in the database
    """
    load_dotenv()

    # Load GeoJSON file
    geojson_file = Path(geojson_path)
    if not geojson_file.exists():
        raise FileNotFoundError(f"GeoJSON file not found: {geojson_path}")

    with open(geojson_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Extract geometry
    if data['type'] == 'FeatureCollection':
        if not data['features']:
            raise ValueError("FeatureCollection is empty")
        geometry_data = data['features'][0]['geometry']
        print(f"Loaded FeatureCollection with {len(data['features'])} feature(s)")
    elif data['type'] == 'Feature':
        geometry_data = data['geometry']
        print("Loaded single Feature")
    else:
        raise ValueError(f"Unsupported GeoJSON type: {data['type']}")

    # Convert to Shapely geometry
    geometry = shape(geometry_data)
    print(f"Geometry type: {geometry.geom_type}")

    # Validate geometry type
    if geometry.geom_type not in ['Polygon', 'MultiPolygon']:
        raise ValueError(f"Expected Polygon or MultiPolygon, got {geometry.geom_type}")

    # Handle MultiPolygon
    if geometry.geom_type == 'MultiPolygon':
        multiPolygon : MultiPolygon = cast(MultiPolygon, geometry)
        num_polygons = len(multiPolygon.geoms)
        total_points = sum(len(poly.exterior.coords) for poly in multiPolygon.geoms)
        print(f"MultiPolygon contains {num_polygons} polygon(s) with {total_points} total points")
        convert_area_polygon_to_cs(multiPolygon, name)
    else:
        poly : Polygon = cast(Polygon, geometry)
        print(f"Polygon contains {len(poly.exterior.coords)} points")
        convert_area_polygon_to_cs(poly, name)

    # Convert to cellstrings

def main():
    """
    Load Denmark EEZ from GeoJSON and convert to cellstrings.
    """
    geojson_path = "./src/Denmark_EEZ_gml.geojson"
    name = "Denmark-EEZ-new"

    convert_area_geojson_to_cs(geojson_path, name)

if __name__ == "__main__":
    main()
