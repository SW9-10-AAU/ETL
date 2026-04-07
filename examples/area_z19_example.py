"""
Example: Generate area coverage at zoom 19 instead of zoom 21.

This example demonstrates how to use custom zoom levels to generate
area cellstrings at z19, which is useful for large areas like EEZ
where z21 would be too granular and computationally expensive.
"""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from shapely import Polygon
from core.ls_poly_to_cs import convert_polygon_to_cellstrings


def main():
    # Example polygon representing a small area in EEZ
    # This is just a sample - replace with actual EEZ coordinates
    eez_polygon = Polygon(
        [
            [10.296876838195601, 56.99190737430766],
            [10.28869531892542, 56.98396759632632],
            [10.299050054251637, 56.98341035533278],
            [10.300328416637655, 56.98724121873309],
            [10.309021280862993, 56.98473378907431],
            [10.308765608386068, 56.979439771840674],
            [10.324105957017338, 56.97748915469052],
            [10.326279173073317, 56.98731086714611],
            [10.296876838195601, 56.99190737430766],
        ]
    )

    print("=" * 70)
    print("Comparing area coverage generation at different zoom levels")
    print("=" * 70)

    # Generate at z13, z17, z21 (default)
    print("\n1. Default zoom levels (13, 17, 21):")
    z13_def, z17_def, z21 = convert_polygon_to_cellstrings(
        eez_polygon, zoom_levels=(13, 17, 21)
    )
    print(f"   - z13: {len(z13_def)} cells")
    print(f"   - z17: {len(z17_def)} cells")
    print(f"   - z21: {len(z21)} cells")

    # Generate at z13, z17, z19 (for large areas like EEZ)
    print("\n2. Custom zoom levels for EEZ (13, 17, 19):")
    z13_eez, z17_eez, z19 = convert_polygon_to_cellstrings(
        eez_polygon, zoom_levels=(13, 17, 19)
    )
    print(f"   - z13: {len(z13_eez)} cells")
    print(f"   - z17: {len(z17_eez)} cells")
    print(f"   - z19: {len(z19)} cells")

    # Show the reduction in cell count
    print(f"\n3. Cell count reduction at finest zoom level:")
    reduction_percent = (1 - len(z19) / len(z21)) * 100
    print(f"   - z21 has {len(z21)} cells")
    print(f"   - z19 has {len(z19)} cells")
    print(f"   - Reduction: {reduction_percent:.1f}%")
    print(
        f"   - This reduces computation time for CoverageByMMSI queries significantly!"
    )

    # Note about table schema
    print(f"\n4. Important note about database schema:")
    print(
        f"   - PostgreSQL table 'area_cs' has columns: cellstring_z13, cellstring_z17, cellstring_z21"
    )
    print(
        f"   - Even when using z19, data is stored in the cellstring_z21 column"
    )
    print(
        f"   - The column name is just a label - the actual zoom level is determined by the data"
    )

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
