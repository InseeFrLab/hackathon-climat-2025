import os
import s3fs

fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': 'https://'+'object.files.data.gouv.fr'}, anon=True)


url = "meteofrance-drias/SocleM-Climat-2025%2FCPRCM%2FMETROPOLE%2FALPX-3%2FCNRM-ESM2-1%2Fr1i1p1f2%2FCNRM-AROME46t1%2Fhistorical%2Fday%2F".replace("%2F", "/")
names_dir = ["tasAdjust", "prAdjust", "tasminAdjust", "tasmaxAdjust"]
roots = [f"{url.rstrip('/')}/{nd}/version-hackathon-102025/" for nd in names_dir]
files = []
for root in roots:
    for dirpath, dirnames, filenames in fs.walk(root):
        for f in filenames:
            files.append(f"{dirpath}/{f}")

remote_file = files[0]
