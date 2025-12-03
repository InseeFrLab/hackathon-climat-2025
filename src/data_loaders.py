"""
Data loaders for climate, population, and administrative boundary data.
"""

import os
import tempfile
import zipfile
import requests

import xarray as xr
import pandas as pd
import polars as pl
import geopandas as gpd
from shapely.geometry import Point
import s3fs


# Climate data URLs - using the same model/scenario for consistency
CLIMATE_URLS = {
    'tasAdjust': "https://object.files.data.gouv.fr/meteofrance-drias/SocleM-Climat-2025/EMULATEUR/METROPOLE/ALPX-12/MPI-ESM1-2-LR/r10i1p1f1/CNRM-ALADIN63-emul-CNRM-UNET11-tP22/historical/day/tasAdjust/version-hackathon-102025/tasAdjust_FR-Metro_MPI-ESM1-2-LR_historical_r10i1p1f1_CNRM_CNRM-ALADIN63-emul-CNRM-UNET11-tP22_v1-r1_MF-CDFt-ANASTASIA-SAFRAN-1985-2014_day_18500101-20141231.nc",
    # Note: Need to find URLs for tasmax, tasmin, pr - may need to use different model
}


def load_census_year_climate(
    variable: str,
    census_years: list[int] = [1968, 1975, 1982, 1990, 1999, 2010],
    url: str = None
) -> gpd.GeoDataFrame:
    """
    Load climate data with monthly aggregations for census years.

    Parameters
    ----------
    variable : str
        Variable name: 'tasAdjust', 'tasmaxAdjust', 'tasminAdjust', or 'prAdjust'
    census_years : list[int]
        Years to include (default: [1968, 1975, 1982, 1990, 1999, 2010])
    url : str, optional
        NetCDF file URL (if None, uses default from CLIMATE_URLS)

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with monthly aggregations (12 months per variable)
        Columns: year, x, y, lat, lon, {var}_jan, {var}_feb, {var}_mar, {var}_apr,
                 {var}_may, {var}_jun, {var}_jul, {var}_aug, {var}_sep, {var}_oct,
                 {var}_nov, {var}_dec, geometry

    Examples
    --------
    >>> gdf = load_census_year_climate('tasAdjust', [1968, 1975])
    >>> print(gdf.shape)  # (~19k grid points × 2 years, ~38k rows)
    >>> print(gdf.columns)  # Shows year, x, y, lat, lon, tasAdjust_jan, ..., tasAdjust_dec
    """
    if url is None:
        if variable in CLIMATE_URLS:
            url = CLIMATE_URLS[variable]
        else:
            raise ValueError(
                f"No default URL for variable '{variable}'. "
                f"Available: {list(CLIMATE_URLS.keys())}. "
                "Please provide a URL explicitly."
            )

    print(f"Loading {variable} from NetCDF...")
    print(f"Census years: {census_years}")

    # Open dataset with lazy loading
    ds = xr.open_dataset(url, engine='h5netcdf', chunks={'time': 365})

    # Filter for years in dataset range
    available_years = ds.time.dt.year.values
    min_year, max_year = available_years.min(), available_years.max()

    # Check which census years are available
    valid_years = [y for y in census_years if min_year <= y <= max_year]
    if not valid_years:
        raise ValueError(
            f"No census years available in dataset range {min_year}-{max_year}. "
            f"Requested: {census_years}"
        )

    if len(valid_years) < len(census_years):
        missing = set(census_years) - set(valid_years)
        print(f"Warning: Years {missing} not in dataset ({min_year}-{max_year}), skipping")

    print(f"Processing years: {valid_years}")

    # Filter time to include only census years
    filtered = ds.sel(time=ds.time.dt.year.isin(valid_years))
    var_data = filtered[variable]

    print(f"Computing monthly aggregations...")
    # Monthly aggregation (ME = Month End)
    monthly = var_data.resample(time='ME').mean()
    monthly_df = monthly.to_dataframe().reset_index()

    # Drop lat/lon if they exist (we'll add them back later from dataset coordinates)
    if 'lat' in monthly_df.columns:
        monthly_df = monthly_df.drop(columns=['lat', 'lon'])

    # Extract month and year
    monthly_df['month'] = monthly_df['time'].dt.month
    monthly_df['year'] = monthly_df['time'].dt.year

    # Month names for column naming
    month_map = {
        1: 'jan', 2: 'feb', 3: 'mar', 4: 'apr', 5: 'may', 6: 'jun',
        7: 'jul', 8: 'aug', 9: 'sep', 10: 'oct', 11: 'nov', 12: 'dec'
    }
    monthly_df['month_name'] = monthly_df['month'].map(month_map)

    # Pivot months to columns
    print(f"Pivoting months to columns...")
    monthly_pivot = monthly_df.pivot_table(
        index=['year', 'x', 'y'],
        columns='month_name',
        values=variable,
        aggfunc='first'
    ).reset_index()

    # Rename month columns with variable prefix
    month_cols = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
    for col in month_cols:
        if col in monthly_pivot.columns:
            monthly_pivot = monthly_pivot.rename(columns={col: f'{variable}_{col}'})

    merged = monthly_pivot

    # Extract lat/lon for each x,y location
    print(f"Adding geographic coordinates...")
    unique_xy = merged[['x', 'y']].drop_duplicates()

    lat_lon_map = []
    for _, row in unique_xy.iterrows():
        x_val = row['x']
        y_val = row['y']

        lat = float(ds['lat'].sel(x=x_val, y=y_val, method='nearest').values)
        lon = float(ds['lon'].sel(x=x_val, y=y_val, method='nearest').values)

        lat_lon_map.append({'x': x_val, 'y': y_val, 'lat': lat, 'lon': lon})

    coords_df = pd.DataFrame(lat_lon_map)
    merged = merged.merge(coords_df, on=['x', 'y'], how='left')

    # Create Point geometries
    print(f"Creating GeoDataFrame...")
    geometry = [Point(lon, lat) for lon, lat in zip(merged['lon'], merged['lat'])]
    gdf = gpd.GeoDataFrame(merged, geometry=geometry, crs='EPSG:4326')

    ds.close()

    print(f"Loaded {len(gdf)} rows with {len(gdf.columns)} columns")
    return gdf


def load_commune_boundaries(
    cache_dir: str = "data/external"
) -> gpd.GeoDataFrame:
    """
    Download and load French commune boundaries from OpenStreetMap.

    Parameters
    ----------
    cache_dir : str
        Directory to cache the shapefile (default: 'data/external')

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with commune boundaries
        Columns: insee, nom, geometry (Polygon), CRS=EPSG:4326

    Examples
    --------
    >>> communes = load_commune_boundaries()
    >>> print(f"Loaded {len(communes)} communes")
    """
    shp_path = os.path.join(cache_dir, "communes-20220101.shp")

    # Check if already cached
    if os.path.exists(shp_path):
        print(f"Loading cached commune boundaries from {shp_path}")
        return gpd.read_file(shp_path)

    print("Downloading commune boundaries from OpenStreetMap...")
    url = "https://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20220101-shp.zip"

    # Download to temp directory
    local_zip = os.path.join(tempfile.gettempdir(), "communes.zip")
    resp = requests.get(url)
    with open(local_zip, "wb") as f:
        f.write(resp.content)

    print("Extracting shapefile...")
    with zipfile.ZipFile(local_zip, "r") as z:
        z.extractall(path=cache_dir)

    # Find the .shp file
    shp_files = [f for f in os.listdir(cache_dir) if f.endswith(".shp")]
    if len(shp_files) == 0:
        raise RuntimeError(f"No .shp file found in {cache_dir}")

    shp_path = os.path.join(cache_dir, shp_files[0])

    print(f"Loading shapefile from {shp_path}")
    gdf_comm = gpd.read_file(shp_path)

    print(f"Loaded {len(gdf_comm)} communes")
    print(f"CRS: {gdf_comm.crs}")

    return gdf_comm


def load_population_data(
    s3_path: str = "/nicolastlm/2025_hackathon/pop.parquet",
    census_years: list[int] = [1968, 1975, 1982, 1990, 1999, 2010]
) -> pd.DataFrame:
    """
    Load population data from S3 parquet file.

    Parameters
    ----------
    s3_path : str
        Path to parquet file in S3 (relative to working directory)
    census_years : list[int]
        Years to include (default: [1968, 1975, 1982, 1990, 1999, 2010])

    Returns
    -------
    pd.DataFrame
        DataFrame with columns: CODGEO (INSEE code), year, pop

    Examples
    --------
    >>> pop = load_population_data()
    >>> print(pop.head())
    """
    print(f"Loading population data from S3: {s3_path}")

    # Create filesystem object
    fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': "https://minio.lab.sspcloud.fr"}, anon=True)
 
    FILE_PATH_S3 = s3_path

    print(f"Reading from: {FILE_PATH_S3}")

    # Read parquet file using polars for efficiency
    with fs.open(FILE_PATH_S3, 'rb') as f:
        df_pl = pl.read_parquet(f)

    # Filter for census years
    df_filtered = df_pl.filter(pl.col('year').is_in(census_years))

    # Select only needed columns and convert to pandas
    df = df_filtered.select(['CODGEO', 'year', 'pop']).to_pandas()

    # Rename CODGEO to insee for consistency
    df = df.rename(columns={'CODGEO': 'insee'})

    print(f"Loaded {len(df)} rows for {len(census_years)} census years")
    print(f"Communes: {df['insee'].nunique()}")

    return df


if __name__ == '__main__':
    # Test functions
    print("=" * 80)
    print("Testing data loaders...")
    print("=" * 80)

    # Test 1: Load climate for one year
    print("\n1. Testing load_census_year_climate() with 1 year...")
    try:
        gdf_climate = load_census_year_climate('tasAdjust', census_years=[2010])
        print(f"Success! Shape: {gdf_climate.shape}")
        print(f"Columns: {gdf_climate.columns.tolist()}")
        print(gdf_climate.head())
    except Exception as e:
        print(f"Error: {e}")

    # Test 2: Load commune boundaries
    print("\n2. Testing load_commune_boundaries()...")
    try:
        gdf_communes = load_commune_boundaries()
        print(f"Success! Loaded {len(gdf_communes)} communes")
        print(f"Columns: {gdf_communes.columns.tolist()[:5]}")  # First 5 columns
    except Exception as e:
        print(f"Error: {e}")

    # Test 3: Load population data
    print("\n3. Testing load_population_data()...")
    try:
        df_pop = load_population_data(census_years=[2010])
        print(f"Success! Shape: {df_pop.shape}")
        print(df_pop.head())
    except Exception as e:
        print(f"Error: {e}")
