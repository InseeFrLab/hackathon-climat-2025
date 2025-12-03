"""
Climate router - Main API endpoint for climate projections.

Orchestrates geocoding, climate generation, and boundary services
to provide complete climate projection data for French addresses.
"""

from fastapi import APIRouter, HTTPException, Query, Request
from api.services.geocoding import geocode_address, AddressNotFoundException
from api.services.climate_generator import generate_climate_projections
from api.services.boundary_service import get_commune_boundary, get_cache_stats


router = APIRouter()


@router.get("/climate")
async def get_climate(
    request: Request,  # Add Request to access app.state
    address: str = Query(
        ...,
        min_length=3,
        max_length=200,
        description="French address to geocode and retrieve climate projections for"
    )
):
    """
    Get climate projections for a French address using ML model.

    This endpoint:
    1. Geocodes the address using API Adresse
    2. Generates ML-based climate projections (2030, 2050, 2100)
    3. Fetches commune boundary GeoJSON

    Args:
        address: Address query string (e.g., "88 avenue henri verdier, Montrouge")

    Returns:
        JSON with geocoding, climate projections, and commune boundary

    Raises:
        HTTPException 404: Address not found or low confidence
        HTTPException 500: Internal server error
    """
    try:
        # Step 1: Geocode address
        geocoding = await geocode_address(address)

        # Step 2: Generate climate data using ML model
        climate = await generate_climate_projections(
            lat=geocoding.coordinates[1],  # latitude
            lon=geocoding.coordinates[0],  # longitude
            temp_max_model=request.app.state.temp_max_model
        )

        # Step 3: Fetch commune boundary (may be None if not available)
        boundary = await get_commune_boundary(geocoding.insee)

        # Step 4: Return unified response
        return {
            "geocoding": {
                "query": address,
                "coordinates": list(geocoding.coordinates),
                "commune": geocoding.commune,
                "insee": geocoding.insee,
                "label": geocoding.label,
                "score": geocoding.score
            },
            "climate": climate,
            "commune_boundary": boundary
        }

    except AddressNotFoundException as e:
        raise HTTPException(
            status_code=404,
            detail=str(e)
        )
    except Exception as e:
        # Log the error (in production, use proper logging)
        print(f"Error in /api/climate endpoint: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )


@router.get("/cache/stats")
async def cache_statistics():
    """
    Get cache statistics for the boundary service.

    Returns information about how many communes and departments
    have been cached.

    Returns:
        JSON with cache statistics
    """
    return get_cache_stats()
