"""
Climate projection generator.

Generates synthetic but realistic climate projections based on location (latitude).
Uses IPCC-aligned warming scenarios and French climate patterns.
"""

import math
from typing import Dict, List


def _get_base_temperature(lat: float) -> float:
    """
    Calculate base annual average temperature from latitude.

    France spans roughly 42°N (Corsica) to 51°N (northern border).
    Returns base temperature in Celsius.
    """
    if lat > 49.0:
        # Northern France (Lille, etc.) - more oceanic, cooler
        return 11.5
    elif lat > 48.0:
        # Paris region - temperate
        return 12.5
    elif lat > 45.0:
        # Central France (Lyon area) - transitional
        return 13.5
    else:
        # Southern France (Marseille, Toulouse, etc.) - Mediterranean
        return 14.5


def _get_base_precipitation(lat: float) -> int:
    """
    Calculate base annual precipitation from latitude.

    Returns precipitation in mm/year.
    Northern/western France has more oceanic influence (higher precipitation).
    Southern/eastern France has more Mediterranean influence (lower precipitation).
    """
    if lat > 49.0:
        return 700  # Northern France
    elif lat > 48.0:
        return 650  # Paris region
    elif lat > 45.0:
        return 600  # Central France
    else:
        return 550  # Southern France


def generate_monthly_temps(base_temp: float, warming: float) -> List[float]:
    """
    Generate 12 monthly temperatures with seasonal variation.

    Uses cosine function to create smooth seasonal cycle:
    - January (month 0): Coldest
    - July (month 6): Warmest

    Args:
        base_temp: Annual average base temperature (°C)
        warming: Additional warming to add (°C)

    Returns:
        List of 12 monthly temperatures in °C
    """
    monthly = []

    for month in range(12):
        # Cosine function for seasonal variation
        # At month=0 (Jan): cos(0) = 1 → multiply by -7 = -7°C below base
        # At month=6 (Jul): cos(π) = -1 → multiply by -7 = +7°C above base
        angle = 2 * math.pi * month / 12
        seasonal_offset = -7 * math.cos(angle)

        # Combine base, warming, and seasonal variation
        temp = base_temp + warming + seasonal_offset

        monthly.append(round(temp, 1))

    return monthly


def generate_climate_projections(lat: float, lon: float) -> Dict[str, dict]:
    """
    Generate synthetic climate projections for a location.

    Creates realistic projections for 2030, 2050, and 2100 based on:
    - Latitude-based climate zones
    - IPCC warming scenarios (RCP4.5/RCP8.5)
    - French climate patterns

    Args:
        lat: Latitude
        lon: Longitude (currently unused, reserved for future enhancements)

    Returns:
        Dictionary with keys "2030", "2050", "2100", each containing:
        - monthly_temp: List of 12 monthly temperatures
        - temp_min: Annual minimum temperature
        - temp_max: Annual maximum temperature
        - annual_precip: Annual precipitation in mm
    """
    # Get base climate from latitude
    base_temp = _get_base_temperature(lat)
    base_precip = _get_base_precipitation(lat)

    projections = {}

    # Generate for each projection year
    # (year, warming°C, precipitation_factor)
    scenarios = [
        (2030, 0.8, 0.95),   # Early warming, slight precipitation decline
        (2050, 1.8, 0.90),   # Moderate warming, noticeable precipitation decline
        (2100, 3.5, 0.80),   # Significant warming, major precipitation decline
    ]

    for year, warming, precip_factor in scenarios:
        monthly = generate_monthly_temps(base_temp, warming)

        projections[str(year)] = {
            "monthly_temp": monthly,
            "temp_min": round(min(monthly) - 3.0, 1),  # Extreme cold below January avg
            "temp_max": round(max(monthly) + 4.0, 1),  # Extreme heat above July avg
            "annual_precip": int(base_precip * precip_factor)
        }

    return projections
