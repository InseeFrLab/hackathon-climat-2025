"""
Climate projection generator.

Generates synthetic but realistic climate projections based on location (latitude).
Uses IPCC-aligned warming scenarios and French climate patterns.
"""

import math
from typing import Dict, List

import polars as pl
import datetime


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


async def generate_climate_projections(lat: float, lon: float, temp_max_model) -> Dict[str, dict]:
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

    # Years to predict
    years = [2093, 2095, 2099]
    
    projections = {}
    for year in years:
        # Create input dataframe: January 1st of target year
        input_df = pl.DataFrame({
            "x": [lon],
            "y": [lat],
            "year": [datetime.date(year, 1, 1)]
        })

        # Get prediction from model
        prediction = temp_max_model.predict(input_df)
        
        # Extract the predicted value (assuming it returns array-like)
        temp_max_pred = float(prediction[0]) if hasattr(prediction, '__getitem__') else float(prediction)
        
        projections[str(year)] = {
            "temp_max": round(temp_max_pred, 1),
            # You can add other fields here if needed
            "prediction_date": f"{year}-01-01"
        }

        print(projections)
    
    return projections
