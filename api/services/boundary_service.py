"""
Commune boundary service using cartiflette.

Fetches French commune boundaries with department-level caching for performance.
"""

import asyncio
from typing import Dict, Optional, Set
from cartiflette import carti_download


# Global cache for commune boundaries
# Key: INSEE code, Value: GeoJSON Feature dict
_boundary_cache: Dict[str, Dict] = {}

# Track which departments have been loaded
_dept_loaded: Set[str] = set()


def _load_department_sync(dept_code: str) -> None:
    """
    Load all communes for a department using cartiflette (synchronous).

    This function downloads commune boundaries for an entire department
    and caches each commune as a GeoJSON Feature.

    Args:
        dept_code: 2 or 3 digit department code (e.g., "92", "2A")
    """
    try:
        print(f"Downloading commune boundaries for department {dept_code}...")

        # Download using cartiflette (following generate_map_data.py pattern)
        gdf = carti_download(
            crs=4326,                              # WGS84 for web compatibility
            values=[dept_code],
            borders="COMMUNE",
            vectorfile_format="geojson",
            filter_by="DEPARTEMENT",
            source="EXPRESS-COG-CARTO-TERRITORY",
            year=2022
        )

        # Cache each commune as GeoJSON Feature
        for _, row in gdf.iterrows():
            insee_code = row['INSEE_COM']

            # Convert to GeoJSON Feature format
            feature = {
                "type": "Feature",
                "properties": {
                    "INSEE_COM": insee_code,
                    "NOM_COM": row.get('NOM', ''),
                    "POPULATION": row.get('POPULATION', 0)
                },
                "geometry": row['geometry'].__geo_interface__
            }

            _boundary_cache[insee_code] = feature

        _dept_loaded.add(dept_code)
        print(f"Cached {len(gdf)} communes from department {dept_code}")

    except Exception as e:
        print(f"Error loading department {dept_code}: {e}")
        # Don't raise - let the API continue without boundary data
        # Individual requests will return None if boundary not available


async def get_commune_boundary(insee: str) -> Optional[Dict]:
    """
    Fetch commune boundary GeoJSON using cartiflette with caching.

    Uses department-level caching strategy:
    - First request for a department downloads all communes in that department
    - Subsequent requests for same department are served from cache (< 10ms)
    - Boundaries are cached for the lifetime of the application

    Args:
        insee: 5-digit INSEE commune code (e.g., "92049" for Montrouge)

    Returns:
        GeoJSON Feature dict or None if not found
    """
    # Check cache first
    if insee in _boundary_cache:
        return _boundary_cache[insee]

    # Extract department code from INSEE
    # Regular departments: first 2 digits (e.g., "92" from "92049")
    # Corsica: first 3 characters (e.g., "2A" from "2A001")
    if insee.startswith('97'):
        # Overseas territories: first 3 digits
        dept_code = insee[:3]
    elif insee.startswith('2A') or insee.startswith('2B'):
        # Corsica
        dept_code = insee[:2]
    else:
        # Metropolitan France
        dept_code = insee[:2]

    # Load department if not cached
    if dept_code not in _dept_loaded:
        # Run synchronous cartiflette in thread pool to avoid blocking
        await asyncio.to_thread(_load_department_sync, dept_code)

    # Return from cache (may still be None if commune not found)
    return _boundary_cache.get(insee)


def get_cache_stats() -> Dict[str, int]:
    """
    Get statistics about the boundary cache.

    Returns:
        Dict with cache statistics (cached_communes, cached_departments)
    """
    return {
        "cached_communes": len(_boundary_cache),
        "cached_departments": len(_dept_loaded)
    }
