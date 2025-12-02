import xarray as xr

data = xr.open_dataset("https://object.files.data.gouv.fr/meteofrance-drias/SocleM-Climat-2025/EMULATEUR/METROPOLE/ALPX-12/MPI-ESM1-2-LR/r10i1p1f1/CNRM-ALADIN63-emul-CNRM-UNET11-tP22/historical/day/tasAdjust/version-hackathon-102025/tasAdjust_FR-Metro_MPI-ESM1-2-LR_historical_r10i1p1f1_CNRM_CNRM-ALADIN63-emul-CNRM-UNET11-tP22_v1-r1_MF-CDFt-ANASTASIA-SAFRAN-1985-2014_day_18500101-20141231.nc",  engine="h5netcdf")

print(data)