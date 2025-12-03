"""
Master data integration pipeline to combine climate and population data.
"""

import os
import pandas as pd

from data_loaders import load_census_year_climate, load_commune_boundaries, load_population_data
from spatial_aggregation import join_climate_to_communes, aggregate_climate_by_commune


def create_dataset(
    climate_vars: list[str] = ['tasAdjust'],  # Start with one, expand to all 4
    census_years: list[int] = [1968, 1975, 1982, 1990, 1999, 2010],
    cache: bool = True,
    climate_urls: dict = None
) -> pd.DataFrame:
    """
    Create integrated climate-population dataset at commune level.

    This is the master pipeline function that orchestrates all data loading,
    spatial joining, aggregation, and merging operations.

    Parameters
    ----------
    climate_vars : list[str]
        Climate variables to include (default: ['tasAdjust'])
        Options: 'tasAdjust', 'tasmaxAdjust', 'tasminAdjust', 'prAdjust'
    census_years : list[int]
        Census years to include (default: [1968, 1975, 1982, 1990, 1999, 2010])
    cache : bool
        If True, save intermediate and final results to parquet (default: True)
    climate_urls : dict, optional
        Custom URLs for climate variables {variable_name: url}

    Returns
    -------
    pd.DataFrame
        Integrated dataset with ~210k rows × 25+ columns
        Columns: year, insee, pop, {climate features}, n_grid_points

    Examples
    --------
    >>> # Start with one variable for testing
    >>> df = create_dataset(climate_vars=['tasAdjust'], census_years=[2010])
    >>> print(df.shape)

    >>> # Full dataset with all variables
    >>> df = create_dataset(
    ...     climate_vars=['tasAdjust', 'tasmaxAdjust', 'tasminAdjust', 'prAdjust']
    ... )
    >>> df.to_parquet('data/processed/dataset.parquet')
    """
    print("=" * 80)
    print("CLIMATE-POPULATION DATA INTEGRATION PIPELINE")
    print("=" * 80)
    print(f"Climate variables: {climate_vars}")
    print(f"Census years: {census_years}")
    print(f"Caching: {cache}")
    print("")

    # -------------------------------------------------------------------------
    # Step 1: Load commune boundaries (once, reuse for all variables)
    # -------------------------------------------------------------------------
    print("\n" + "=" * 80)
    print("STEP 1: Loading commune boundaries")
    print("=" * 80)

    communes_gdf = load_commune_boundaries()

    # -------------------------------------------------------------------------
    # Step 2: Process each climate variable
    # -------------------------------------------------------------------------
    climate_dfs = []

    for i, var in enumerate(climate_vars, 1):
        print(f"\n" + "=" * 80)
        print(f"STEP 2.{i}: Processing climate variable '{var}'")
        print("=" * 80)



        # Get URL for this variable if provided
        url = climate_urls.get(var) if climate_urls else None

        # Step 2a: Load climate data
        print(f"\n[2.{i}.a] Loading {var} climate data...")
        climate_gdf = load_census_year_climate(
            variable=var,
            census_years=census_years,
            url=url
        )

        # Step 2b: Spatial join
        print(f"\n[2.{i}.b] Joining {var} to communes...")
        climate_joined = join_climate_to_communes(climate_gdf, communes_gdf)

        # Step 2c: Aggregate to commune level
        print(f"\n[2.{i}.c] Aggregating {var} by commune...")
        climate_agg = aggregate_climate_by_commune(climate_joined, variable_prefix=var)

        # For the first variable, keep n_grid_points
        # For subsequent variables, drop it (it's the same)
        if i > 1 and 'n_grid_points' in climate_agg.columns:
            climate_agg = climate_agg.drop(columns=['n_grid_points'])

        climate_dfs.append(climate_agg)

        # Cache intermediate result
        if cache:
            cache_path = f"data/processed/{var}_by_commune.parquet"
            climate_agg.to_parquet(cache_path, index=False)
            print(f"Cached to: {cache_path}")

    # -------------------------------------------------------------------------
    # Step 3: Merge all climate variables
    # -------------------------------------------------------------------------
    print(f"\n" + "=" * 80)
    print("STEP 3: Merging all climate variables")
    print("=" * 80)

    # Start with first variable
    climate_merged = climate_dfs[0]
    print(f"Starting with {climate_vars[0]}: {climate_merged.shape}")

    # Merge each additional variable
    for i, (var, df) in enumerate(zip(climate_vars[1:], climate_dfs[1:]), 2):
        print(f"Merging {var}: {df.shape}")
        climate_merged = climate_merged.merge(
            df,
            on=['year', 'insee'],
            how='outer'  # Outer join to keep all communes
        )
        print(f"After merge: {climate_merged.shape}")

    print(f"\nMerged climate data shape: {climate_merged.shape}")
    print(f"Columns: {climate_merged.columns.tolist()}")

    # Cache merged climate
    if cache:
        cache_path = "data/processed/climate_by_commune.parquet"
        climate_merged.to_parquet(cache_path, index=False)
        print(f"Cached merged climate to: {cache_path}")

    # -------------------------------------------------------------------------
    # Step 4: Load population data
    # -------------------------------------------------------------------------
    print(f"\n" + "=" * 80)
    print("STEP 4: Loading population data")
    print("=" * 80)

    pop_df = load_population_data(census_years=census_years)

    # -------------------------------------------------------------------------
    # Step 5: Merge climate + population
    # -------------------------------------------------------------------------
    print(f"\n" + "=" * 80)
    print("STEP 5: Merging climate and population data")
    print("=" * 80)

    print(f"Climate data: {climate_merged.shape}")
    print(f"Population data: {pop_df.shape}")

    final_df = climate_merged.merge(
        pop_df,
        on=['year', 'insee'],
        how='left'  # Keep all climate data, add population where available
    )

    print(f"Final merged shape: {final_df.shape}")

    # Check for missing values
    n_missing_pop = final_df['pop'].isna().sum()
    if n_missing_pop > 0:
        pct_missing = n_missing_pop / len(final_df) * 100
        print(f"Warning: {n_missing_pop} rows ({pct_missing:.1f}%) missing population data")

    # -------------------------------------------------------------------------
    # Step 6: Final dataset summary
    # -------------------------------------------------------------------------
    print(f"\n" + "=" * 80)
    print("FINAL DATASET SUMMARY")
    print("=" * 80)

    print(f"Shape: {final_df.shape}")
    print(f"Columns ({len(final_df.columns)}): {final_df.columns.tolist()}")
    print(f"")
    print(f"Years: {sorted(final_df['year'].unique())}")
    print(f"Communes: {final_df['insee'].nunique()}")
    print(f"Missing values:")
    print(final_df.isnull().sum())
    print("")
    print(f"Sample data:")
    print(final_df.head(10))

    # -------------------------------------------------------------------------
    # Step 7: Save final dataset
    # -------------------------------------------------------------------------
    if cache:
        final_path = "data/processed/dataset.parquet"
        final_df.to_parquet(final_path, index=False)
        print(f"\n{'=' * 80}")
        print(f"FINAL DATASET SAVED TO: {final_path}")
        print(f"{'=' * 80}")

    return final_df


if __name__ == '__main__':
    # Test pipeline with one variable and one year
    print("Testing pipeline with single variable and year...")

    try:
        # Test 1: Single variable, single year
        print("\n\nTEST 1: Single variable (tasAdjust), single year (2010)")
        print("=" * 80)
        df_test = create_dataset(
            climate_vars=['tasAdjust'],
            census_years=[2010],
            cache=True
        )

        print("\n\nTest 1 complete!")
        print(f"Final shape: {df_test.shape}")
        print(f"Expected: ~35k rows (communes) × ~8 columns")

    except Exception as e:
        print(f"Error in Test 1: {e}")
        import traceback
        traceback.print_exc()

    # Test 2: Single variable, all years (commented out for initial testing)
    # print("\n\nTEST 2: Single variable (tasAdjust), all years")
    # print("=" * 80)
    # df_test2 = create_dataset(
    #     climate_vars=['tasAdjust'],
    #     census_years=[1968, 1975, 1982, 1990, 1999, 2010],
    #     cache=True
    # )
    # print(f"\nFinal shape: {df_test2.shape}")
    # print(f"Expected: ~210k rows × ~8 columns")
