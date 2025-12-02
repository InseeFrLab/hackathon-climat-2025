import os
import tempfile
import zipfile
import requests

import xarray as xr
import numpy as np
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

# 1) Charger ton NetCDF et générer les points
data = xr.open_dataset(
    "https://object.files.data.gouv.fr/meteofrance-drias/SocleM-Climat-2025/EMULATEUR/METROPOLE/ALPX-12/MPI-ESM1-2-LR/r10i1p1f1/CNRM-ALADIN63-emul-CNRM-UNET11-tP22/historical/day/tasAdjust/version-hackathon-102025/tasAdjust_FR-Metro_MPI-ESM1-2-LR_historical_r10i1p1f1_CNRM_CNRM-ALADIN63-emul-CNRM-UNET11-tP22_v1-r1_MF-CDFt-ANASTASIA-SAFRAN-1985-2014_day_18500101-20141231.nc",
    engine="h5netcdf"
)

tas = data["tasAdjust"]  # ton champ de température (Kelvin)

years_to_keep = [1968, 1975, 1982, 1990, 1999, 2010]

tas = tas.sel(time=tas.time.dt.year.isin(years_to_keep))

# --- Agrégation par mois et par année ---
monthly_min  = tas.groupby(["time.year", "time.month"]).min()
monthly_max  = tas.groupby(["time.year", "time.month"]).max()
monthly_mean = tas.groupby(["time.year", "time.month"]).mean()

monthly_stats = xr.Dataset({
    "tas_min":  monthly_min,
    "tas_max":  monthly_max,
    "tas_mean": monthly_mean
})

lons = monthly_stats["lon"].values  # shape (nlat, nlon)
lats = monthly_stats["lat"].values  # same shape
tas_mean = monthly_stats["tas_mean"].values

# Aplatir
lons_flat = lons.ravel()
lats_flat = lats.ravel()

# Créer GeoDataFrame des points
df_pts = pd.DataFrame({
    "lon": lons_flat,
    "lat": lats_flat,
    "tas_mean": tas_mean[:, :, -1, 7].ravel()
})
gdf_pts = gpd.GeoDataFrame(
    df_pts,
    geometry=[Point(xy) for xy in zip(df_pts.lon, df_pts.lat)],
    crs="EPSG:4326"
).dropna(subset='tas_mean')

# 2) Télécharger et charger le shapefile des communes
url = "https://osm13.openstreetmap.fr/~cquest/openfla/export/communes-20220101-shp.zip"
local_zip = os.path.join(tempfile.gettempdir(), "communes.zip")
resp = requests.get(url)
with open(local_zip, "wb") as f:
    f.write(resp.content)

with zipfile.ZipFile(local_zip, "r") as z:
    z.extractall(path=tempfile.gettempdir())

# repérer le .shp
# ici on suppose qu’un fichier .shp existe dans le zip
shp_files = [f for f in os.listdir(tempfile.gettempdir()) if f.endswith(".shp")]
if len(shp_files) == 0:
    raise RuntimeError("No .shp file found in the archive")
shp_path = os.path.join(tempfile.gettempdir(), shp_files[0])

gdf_comm = gpd.read_file(shp_path)

# 3) S'assurer que les CRS sont compatibles (souvent WGS84 / EPSG:4326 dans cet export OSM) :contentReference[oaicite:1]{index=1}
print("CRS communes:", gdf_comm.crs)
print("CRS points:", gdf_pts.crs)

# 4) Jointure spatiale points → communes
# On peut faire un sjoin avec predicate='within' ou 'intersects'
points_plot = gpd.sjoin(
    gdf_pts,
    gdf_comm,
    how="left",
    predicate="intersects"
).dropna(subset=['insee'])

# 5) Moyenne par commune
# On suppose que le code INSEE de la commune est dans une colonne, par exemple "insee"
# res = (
#     points_in_comm
#     .dropna(subset=["insee"])  # en cas de points sans commune
#     .groupby("insee")["tas"]
#     .mean()
#     .reset_index(name="tas_mean")
# )

# print(res.head())

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature

# Créer la figure
fig, ax = plt.subplots(figsize=(12, 10), subplot_kw={'projection': ccrs.PlateCarree()})

# Tracer les points colorés par la température moyenne
scatter = ax.scatter(
    points_plot['lon'],
    points_plot['lat'],
    c=points_plot['tas_mean'],    # << couleur = température
    cmap='coolwarm',
    s=40,
    alpha=0.85,
    transform=ccrs.PlateCarree(),
    label='Points CORDEX'
)

# Ajouter la barre de couleur
cbar = plt.colorbar(scatter, ax=ax, orientation='vertical', shrink=0.7, pad=0.02)
cbar.set_label("Température moyenne (K)", fontsize=12)

# Ajouter les éléments cartographiques
ax.coastlines(resolution='10m', linewidth=0.5)
ax.add_feature(cfeature.BORDERS, linewidth=0.5)
ax.add_feature(cfeature.LAND, facecolor='lightgray', alpha=0.3)
ax.add_feature(cfeature.OCEAN, facecolor='lightblue', alpha=0.3)

# Ajouter une grille
ax.gridlines(draw_labels=True, dms=True, x_inline=False, y_inline=False,
             linewidth=0.5, alpha=0.5)

# Ajuster l'étendue autour de la France métropolitaine
ax.set_extent([-5.5, 9.5, 41, 51.5], crs=ccrs.PlateCarree())

plt.title('Température moyenne des points CORDEX – Août 2010', fontsize=14, pad=20)
plt.legend()
plt.tight_layout()
plt.savefig("test_temperature.png", dpi=150)
plt.show()