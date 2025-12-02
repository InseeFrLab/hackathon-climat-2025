import os
import s3fs

fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': 'https://'+'object.files.data.gouv.fr'}, anon=True)

url = "meteofrance-drias/SocleM-Climat-2025%2FRCM%2FEURO-CORDEX%2FEUR-12%2FCMCC-CM2-SR5%2Fr1i1p1f1%2FCNRM-ALADIN64E1%2Fhistorical%2Fday%2FprAdjust%2Fversion-hackathon-102025%2F".replace("%2F", "/")
root = url
files = []
for dirpath, dirnames, filenames in fs.walk(root):
    for f in filenames:
        files.append(f"{dirpath}/{f}")

remote_file = files[0]
local_file = os.path.basename(remote_file)

# fs.get(remote_file, local_file)
# print("Téléchargé :", local_file)


# Source - https://stackoverflow.com/a
# Posted by developer colasanti, modified by community. See post 'Timeline' for change history
# Retrieved 2025-12-02, License - CC BY-SA 4.0

import xarray as xr
import pandas as pd # only needed if you want to do pandas stuff
dataset = xr.open_dataset(local_file)


import fsspec
import xarray as xr

bucket = 'https://'+'object.files.data.gouv.fr/' 

fs = fsspec.filesystem("https")
with fs.open(bucket + remote_file, mode="rb") as f:
    ds = xr.open_dataset(f) 