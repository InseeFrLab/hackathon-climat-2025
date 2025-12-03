"""
Spatial aggregation functions for joining and aggregating climate data to communes.
"""

import pandas as pd
import geopandas as gpd


def join_climate_to_communes(
    climate_gdf: gpd.GeoDataFrame,
    communes_gdf: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """
    Perform spatial join to assign climate grid points to communes.

    Parameters
    ----------
    climate_gdf : gpd.GeoDataFrame
        Climate data with Point geometries (from load_census_year_climate)
    communes_gdf : gpd.GeoDataFrame
        Commune boundaries with Polygon geometries (from load_commune_boundaries)

    Returns
    -------
    gpd.GeoDataFrame
        Climate data with added 'insee' column indicating which commune each point belongs to
        Points that don't intersect any commune are dropped

    Examples
    --------
    >>> climate_joined = join_climate_to_communes(climate_gdf, communes_gdf)
    >>> print(f"Matched {len(climate_joined)} / {len(climate_gdf)} grid points to communes")
    """
    print("Performing spatial join: climate points → communes...")
    print(f"Climate data: {len(climate_gdf)} rows")
    print(f"Communes: {len(communes_gdf)} polygons")

    # Ensure CRS match
    if climate_gdf.crs != communes_gdf.crs:
        print(f"Converting communes CRS from {communes_gdf.crs} to {climate_gdf.crs}")
        communes_gdf = communes_gdf.to_crs(climate_gdf.crs)

    # Spatial join
    joined = gpd.sjoin(
        climate_gdf,
        communes_gdf[['insee', 'nom', 'geometry']],  # Only keep needed columns
        how='left',
        predicate='intersects'
    )

    # Count points before/after dropping NaNs
    n_before = len(joined)
    n_null = joined['insee'].isna().sum()

    # Drop points that don't intersect any commune
    joined_clean = joined.dropna(subset=['insee'])

    n_after = len(joined_clean)
    match_rate = n_after / n_before * 100

    print(f"Spatial join complete:")
    print(f"  - Matched: {n_after} grid points ({match_rate:.1f}%)")
    print(f"  - Unmatched: {n_null} grid points (likely ocean/border)")
    print(f"  - Unique communes: {joined_clean['insee'].nunique()}")

    return joined_clean


def aggregate_climate_by_commune(
    climate_communes_gdf: gpd.GeoDataFrame,
    variable_prefix: str = None
) -> pd.DataFrame:
    """
    Aggregate climate features from grid points to commune level.

    Parameters
    ----------
    climate_communes_gdf : gpd.GeoDataFrame
        Climate data with 'insee' column (output of join_climate_to_communes)
    variable_prefix : str, optional
        If provided, only aggregate columns starting with this prefix
        (e.g., 'tasAdjust' will aggregate tasAdjust_yearly, tasAdjust_winter, etc.)

    Returns
    -------
    pd.DataFrame
        Aggregated climate data by commune and year
        Columns: year, insee, {climate features}, n_grid_points

    Examples
    --------
    >>> climate_agg = aggregate_climate_by_commune(climate_joined, 'tasAdjust')
    >>> print(climate_agg.head())
    """
    print("Aggregating climate data to commune level...")
    print(f"Input: {len(climate_communes_gdf)} rows")

    # Identify columns to aggregate
    # Skip: time, x, y, lat, lon, geometry, insee, nom, index_right
    skip_cols = ['time', 'x', 'y', 'lat', 'lon', 'geometry', 'insee', 'nom', 'index_right', 'year']

    # Get numeric columns to aggregate
    numeric_cols = climate_communes_gdf.select_dtypes(include=['number']).columns.tolist()
    agg_cols = [col for col in numeric_cols if col not in skip_cols]

    # Filter by variable prefix if provided
    if variable_prefix:
        agg_cols = [col for col in agg_cols if col.startswith(variable_prefix)]

    if not agg_cols:
        raise ValueError(f"No columns to aggregate. Available numeric columns: {numeric_cols}")

    print(f"Aggregating columns: {agg_cols}")

    # Group by year and insee, compute mean of all climate features
    grouped = climate_communes_gdf.groupby(['year', 'insee'])

    # Aggregate: mean of climate features + count of grid points
    agg_dict = {col: 'mean' for col in agg_cols}
    agg_dict['x'] = 'size'  # Count of grid points (use any column for counting)

    aggregated = grouped.agg(agg_dict).reset_index()

    # Rename 'x' to 'n_grid_points'
    aggregated = aggregated.rename(columns={'x': 'n_grid_points'})

    print(f"Aggregation complete:")
    print(f"  - Output: {len(aggregated)} rows (commune-year combinations)")
    print(f"  - Unique communes: {aggregated['insee'].nunique()}")
    print(f"  - Unique years: {sorted(aggregated['year'].unique())}")
    print(f"  - Columns: {len(aggregated.columns)}")

    return aggregated


if __name__ == '__main__':
    # Test functions
    print("=" * 80)
    print("Testing spatial aggregation functions...")
    print("=" * 80)

    from data_loaders import load_census_year_climate, load_commune_boundaries

    # Test with one year of data
    print("\n1. Loading test data...")
    try:
        climate_gdf = load_census_year_climate('tasAdjust', census_years=[2010])
        communes_gdf = load_commune_boundaries()

        print("\n2. Testing join_climate_to_communes()...")
        joined = join_climate_to_communes(climate_gdf, communes_gdf)
        print(f"Joined shape: {joined.shape}")
        print(f"Columns: {joined.columns.tolist()}")

        print("\n3. Testing aggregate_climate_by_commune()...")
        aggregated = aggregate_climate_by_commune(joined, 'tasAdjust')
        print(f"Aggregated shape: {aggregated.shape}")
        print(f"Columns: {aggregated.columns.tolist()}")
        print(aggregated.head())

    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
