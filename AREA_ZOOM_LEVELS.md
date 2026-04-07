# Area Coverage with Custom Zoom Levels

This document explains how to generate area cellstrings at custom zoom levels, particularly for large areas like EEZ (Exclusive Economic Zone) where using zoom level 19 instead of 21 significantly improves performance.

## Background

Previously, area coverage was generated at three hardcoded zoom levels: z13, z17, and z21. For large areas like EEZ, z21 generates an excessive number of cells, making CoverageByMMSI queries slow and computationally expensive.

The updated implementation allows you to specify custom zoom levels, making it possible to use z19 as the finest level for large areas, reducing cell count by ~93% while maintaining adequate spatial resolution.

## Changes Made

### Core Library Changes

1. **`src/core/cellstring_utils.py`**
   - Added generic `process_tiles_at_zoom()` function
   - Added generic `process_child_tiles()` function
   - Refactored `process_z13_tiles()`, `process_z17_tiles()`, `process_z21_tiles()` to use the generic functions
   - These functions are kept for backward compatibility

2. **`src/core/ls_poly_to_cs.py`**
   - Updated `convert_polygon_to_cellstrings()` to accept `zoom_levels` parameter
   - Default: `zoom_levels=(13, 17, 21)` for backward compatibility
   - Validates that zoom levels are strictly increasing
   - Example: `convert_polygon_to_cellstrings(polygon, zoom_levels=(13, 17, 19))`

### Application Changes

3. **`src/convert_area_polygon.py`**
   - Updated `convert_area_polygon_to_cs_postgresql()` to accept `zoom_levels` parameter
   - Updated `convert_area_polygon_to_cs_duckdb()` to accept `zoom_levels` parameter
   - Added example usage in `main()` function

4. **`src/convert_area_geojson.py`**
   - Updated `convert_area_geojson_to_cs()` to accept `zoom_levels` parameter
   - Updated `main()` to demonstrate z19 usage for EEZ

5. **`src/convert_area_polygons_to_cellstring.py`**
   - Updated batch conversion functions to accept `zoom_levels` parameter
   - Added documentation for using custom zoom levels

### Testing

6. **`tests/test_transform_ls_to_cs.py`**
   - Added `test_custom_zoom_levels_z19()` to verify z19 functionality
   - Added `test_custom_zoom_levels_validation()` to ensure zoom level validation works
   - All existing tests pass with backward compatibility maintained

7. **`examples/area_z19_example.py`**
   - Created example demonstrating the performance benefits of z19 vs z21
   - Shows 93.3% reduction in cell count for sample polygon

## Usage Examples

### Example 1: Generate Area Coverage at Z19 (EEZ)

```python
from shapely import Polygon
from core.ls_poly_to_cs import convert_polygon_to_cellstrings

# Define your EEZ polygon
eez_polygon = Polygon([...])

# Generate at z13, z17, z19 (recommended for large areas)
z13, z17, z19 = convert_polygon_to_cellstrings(
    eez_polygon,
    zoom_levels=(13, 17, 19)
)

print(f"z13: {len(z13)} cells")
print(f"z17: {len(z17)} cells")
print(f"z19: {len(z19)} cells")
```

### Example 2: Load GeoJSON and Convert at Z19

```python
from convert_area_geojson import convert_area_geojson_to_cs

# For large areas like EEZ, use z19
convert_area_geojson_to_cs(
    geojson_path="./geojson/Denmark_EEZ_gml.geojson",
    name="Denmark-EEZ",
    zoom_levels=(13, 17, 19)
)
```

### Example 3: Direct Polygon Conversion (PostgreSQL)

```python
from shapely import Polygon
from convert_area_polygon import convert_area_polygon_to_cs_postgresql

polygon = Polygon([...])

# Use z19 for large areas
convert_area_polygon_to_cs_postgresql(
    polygon,
    name="My-EEZ-Area",
    zoom_levels=(13, 17, 19)
)
```

## Performance Comparison

For a sample polygon representing a small area in Denmark:

| Zoom Level | Cell Count | Reduction |
|------------|------------|-----------|
| z13        | 1          | -         |
| z17        | 95         | -         |
| z21        | 17,185     | baseline  |
| z19        | 1,153      | 93.3%     |

**Key Takeaway**: Using z19 instead of z21 reduces cell count by ~93%, dramatically improving:
- CoverageByMMSI query performance
- Data storage requirements
- Processing time for trajectory/stop coverage analysis

## Database Schema Considerations

### PostgreSQL
- Table: `area_cs`
- Columns: `cellstring_z13`, `cellstring_z17`, `cellstring_z21`
- **Note**: Even when using z19, data is stored in the `cellstring_z21` column
- The column name is just a label - the actual zoom level is determined by the data

### DuckDB
- Table: `area_cs`
- Columns: `area_id`, `name`, `cell_z21` (one row per cell)
- **Note**: DuckDB only stores the finest zoom level cells
- Column name remains `cell_z21` but can contain z19 cells

## When to Use Z19 vs Z21

### Use Z19 for:
- ✅ Large areas like EEZ (Exclusive Economic Zones)
- ✅ Country-level or regional coverage
- ✅ Areas where ~150m spatial resolution is sufficient
- ✅ CoverageByMMSI queries on large areas
- ✅ Performance-critical applications

### Use Z21 for:
- ✅ Small, localized areas (ports, harbors)
- ✅ High-precision coverage requirements (~40m resolution)
- ✅ Detailed trajectory analysis
- ✅ Areas where computational cost is not a concern

## Zoom Level Details

| Zoom | Tile Size (approx) | Use Case |
|------|-------------------|----------|
| z13  | ~10 km           | Coarse regional coverage |
| z17  | ~1 km            | Mid-level coverage |
| z19  | ~150 m           | Large areas (EEZ) |
| z21  | ~40 m            | Small areas (ports) |

## Backward Compatibility

All existing code continues to work without modifications. The default zoom levels remain (13, 17, 21).

To use custom zoom levels, simply pass the `zoom_levels` parameter:
- `convert_polygon_to_cellstrings(poly, zoom_levels=(13, 17, 19))`
- `convert_area_polygon_to_cs_postgresql(poly, name, zoom_levels=(13, 17, 19))`
- `convert_area_geojson_to_cs(path, name, zoom_levels=(13, 17, 19))`

## Testing

Run the test suite to verify functionality:

```bash
python -m unittest tests.test_transform_ls_to_cs
```

Run the example to see performance comparison:

```bash
python examples/area_z19_example.py
```

## Implementation Notes

### Hierarchical Processing
The algorithm uses hierarchical tile processing for efficiency:
1. Process coarsest zoom level (z13) to identify fully/partially contained tiles
2. For fully contained tiles, automatically include all children at next zoom
3. For partially contained tiles, check each child individually
4. Repeat for each zoom level

This optimization significantly reduces the number of intersection tests required.

### Validation
Zoom levels must be strictly increasing: `zoom1 < zoom2 < zoom3`

The implementation validates this and raises a `ValueError` if the constraint is violated.

## Future Enhancements

Possible future improvements:
- Support for arbitrary number of zoom levels (not just 3)
- Environment variable configuration for default zoom levels
- Database migration tools to update existing z21 data to z19
- Performance profiling and benchmarking tools

## References

- Issue: "Area coverage EEZ (z19)"
- Implementation: PR #XXX
- Related: CoverageByMMSI queries, GetParentCellId for traj/stop aggregation
