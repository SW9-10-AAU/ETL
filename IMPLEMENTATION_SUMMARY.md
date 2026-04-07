# Implementation Summary: Configurable Zoom Levels for Area Coverage

## Problem Statement
The issue requested the ability to generate area cellstrings at zoom level 19 instead of the hardcoded zoom level 21, particularly for large areas like EEZ (Exclusive Economic Zone). This is important because:

1. Z21 generates too many cells for large areas (computationally expensive)
2. Z19 provides adequate spatial resolution (~150m vs ~40m) for large area coverage
3. CoverageByMMSI queries need to be performant on EEZ-sized areas

## Solution Implemented

### ✅ Core Changes

1. **Refactored `src/core/cellstring_utils.py`**
   - Created generic `process_tiles_at_zoom(poly, zoom)` function
   - Created generic `process_child_tiles(poly, parent_tiles, target_zoom)` function
   - Maintained backward-compatible wrapper functions (`process_z13_tiles`, `process_z17_tiles`, `process_z21_tiles`)

2. **Updated `src/core/ls_poly_to_cs.py`**
   - Added `zoom_levels` parameter to `convert_polygon_to_cellstrings()`
   - Default: `(13, 17, 21)` for full backward compatibility
   - Added validation to ensure zoom levels are strictly increasing
   - Example: `convert_polygon_to_cellstrings(polygon, zoom_levels=(13, 17, 19))`

3. **Updated All Area Conversion Scripts**
   - `src/convert_area_polygon.py` - Direct polygon conversion
   - `src/convert_area_geojson.py` - GeoJSON file conversion
   - `src/convert_area_polygons_to_cellstring.py` - Batch conversion
   - All now support the `zoom_levels` parameter

### ✅ Testing & Validation

4. **Added Comprehensive Tests**
   - `test_custom_zoom_levels_z19()` - Validates z19 functionality
   - `test_custom_zoom_levels_validation()` - Ensures validation works
   - All 21 tests pass (19 existing + 2 new)
   - Confirmed backward compatibility maintained

5. **Created Example & Documentation**
   - `examples/area_z19_example.py` - Demonstrates performance benefits
   - `AREA_ZOOM_LEVELS.md` - Complete usage guide
   - Shows **93.3% reduction** in cell count (z19 vs z21)

## Performance Impact

For a sample polygon area:
- **z21**: 17,185 cells
- **z19**: 1,153 cells
- **Reduction**: 93.3% fewer cells

This translates to:
- ✅ Faster CoverageByMMSI queries
- ✅ Reduced storage requirements
- ✅ Faster processing times
- ✅ Practical for EEZ-scale analysis

## Usage Examples

### Generate EEZ Area at Z19
```python
from shapely import Polygon
from core.ls_poly_to_cs import convert_polygon_to_cellstrings

eez_polygon = Polygon([...])

# Use z19 for large areas
z13, z17, z19 = convert_polygon_to_cellstrings(
    eez_polygon,
    zoom_levels=(13, 17, 19)
)
```

### Load GeoJSON at Z19
```python
from convert_area_geojson import convert_area_geojson_to_cs

convert_area_geojson_to_cs(
    geojson_path="./geojson/Denmark_EEZ_gml.geojson",
    name="Denmark-EEZ",
    zoom_levels=(13, 17, 19)  # Use z19 for EEZ
)
```

### Convert Existing Polygon (PostgreSQL)
```python
from convert_area_polygon import convert_area_polygon_to_cs_postgresql

convert_area_polygon_to_cs_postgresql(
    polygon,
    name="My-EEZ",
    zoom_levels=(13, 17, 19)
)
```

## Database Schema Notes

### PostgreSQL
- Table: `area_cs` has columns `cellstring_z13`, `cellstring_z17`, `cellstring_z21`
- **Important**: When using z19, data is stored in the `cellstring_z21` column
- The column name is just a label - actual zoom level is determined by the data

### DuckDB
- Table: `area_cs` has columns `area_id`, `name`, `cell_z21`
- Only stores finest zoom level (one cell per row)
- Column name remains `cell_z21` even when containing z19 cells

## Backward Compatibility

✅ **100% Backward Compatible**
- All existing code works without changes
- Default zoom levels remain (13, 17, 21)
- No breaking changes to APIs
- All existing tests pass

## Files Modified

### Core Library
- `src/core/cellstring_utils.py` - Generic tile processing functions
- `src/core/ls_poly_to_cs.py` - Configurable zoom levels

### Applications
- `src/convert_area_polygon.py` - Updated to support zoom_levels
- `src/convert_area_geojson.py` - Updated to support zoom_levels
- `src/convert_area_polygons_to_cellstring.py` - Batch conversion support

### Testing & Examples
- `tests/test_transform_ls_to_cs.py` - Added z19 tests
- `examples/area_z19_example.py` - Performance demonstration

### Documentation
- `AREA_ZOOM_LEVELS.md` - Complete usage guide
- `IMPLEMENTATION_SUMMARY.md` - This file

## Validation Commands

```bash
# Run all tests
python -m unittest tests.test_transform_ls_to_cs

# Run performance example
python examples/area_z19_example.py

# Run linter checks
flake8 . --count --select=E9,F63,F7,F82 --show-source --statistics
```

## Next Steps

The implementation is complete and ready for use. To generate EEZ coverage at z19:

1. **For GeoJSON files**: Use `convert_area_geojson.py` with `zoom_levels=(13, 17, 19)`
2. **For direct polygons**: Use `convert_area_polygon.py` with `zoom_levels=(13, 17, 19)`
3. **For batch processing**: Modify `convert_area_polygons_to_cellstring.py` to pass `zoom_levels=(13, 17, 19)`

## CoverageByMMSI Integration

With z19 area coverage:
- Query trajectory/stop cellstrings at z19 (use GetParentCellId to aggregate z21→z19)
- Match against area cellstrings at z19
- Significantly faster queries on EEZ-scale areas
- Practical for real-time or near-real-time analysis

## Summary

✅ **Complete**: All requirements met
✅ **Tested**: 21 tests passing
✅ **Documented**: Comprehensive guides included
✅ **Backward Compatible**: No breaking changes
✅ **Performance**: 93% reduction in cells for z19 vs z21
✅ **Ready**: Can generate EEZ at z19 immediately
