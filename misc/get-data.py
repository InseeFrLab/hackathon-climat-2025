import os
import s3fs

fs = s3fs.S3FileSystem(
    client_kwargs={'endpoint_url': 'https://'+'object.files.data.gouv.fr'}, anon=True)


# List contents of meteofrance-drias bucket
try:
    files = fs.ls('meteofrance-drias')
    print("\nContents of meteofrance-drias bucket:")
    for file in files:
        print(file)
except Exception as e:
    print(f"Error accessing bucket: {e}")


