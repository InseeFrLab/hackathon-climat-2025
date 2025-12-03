"""
Geocoding service using API Adresse (official French address geocoding API).

Provides address geocoding, validation, and extraction of commune information.
"""

import httpx
from typing import Tuple
from pydantic import BaseModel


class AddressNotFoundException(Exception):
    """Raised when an address cannot be found or has low confidence score."""
    pass


class GeocodingResult(BaseModel):
    """Result of geocoding an address."""
    coordinates: Tuple[float, float]  # (lon, lat)
    commune: str
    insee: str
    label: str
    score: float


async def geocode_address(address: str) -> GeocodingResult:
    """
    Geocode a French address using API Adresse.

    Args:
        address: Address query string

    Returns:
        GeocodingResult with coordinates, commune, INSEE code, etc.

    Raises:
        AddressNotFoundException: If address not found or score too low
        httpx.HTTPError: If API call fails
    """
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(
                "https://api-adresse.data.gouv.fr/search/",
                params={"q": address, "limit": 1},
                timeout=5.0
            )
            response.raise_for_status()
            data = response.json()

            # Check if any results returned
            if not data.get("features"):
                raise AddressNotFoundException(
                    f"No results found for address: {address}"
                )

            feature = data["features"][0]
            properties = feature["properties"]
            score = properties["score"]

            # Validate confidence score
            if score < 0.5:
                raise AddressNotFoundException(
                    f"Low confidence match (score: {score:.2f}). "
                    "Please provide a more specific address (include street number, city, or postal code)."
                )

            # Extract coordinates (GeoJSON format: [lon, lat])
            coordinates = tuple(feature["geometry"]["coordinates"])

            return GeocodingResult(
                coordinates=coordinates,
                commune=properties.get("city", ""),
                insee=properties.get("citycode", ""),
                label=properties.get("label", ""),
                score=score
            )

        except httpx.HTTPError as e:
            # Re-raise with more context
            raise Exception(f"Error calling API Adresse: {str(e)}") from e
