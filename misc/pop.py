import polars as pl
import requests
from io import BytesIO
import os
import s3fs

# Fetch the Excel file from the URL
url = "https://www.insee.fr/fr/statistiques/fichier/3698339/base-pop-historiques-1876-2022.xlsx"
response = requests.get(url)
data = BytesIO(response.content)

# Read the Excel file, skipping the first 5 rows
df = pl.read_excel(data,   read_options={"header_row": 5})

# Convert all columns after 'LIBGEO' to a new column 'year' and put the values in a column named 'pop'
# Assuming the columns after 'LIBGEO' are year columns
year_columns = df.columns[4:]

# Melt the DataFrame to convert year columns to rows
df_melted = df.melt(id_vars=['CODGEO', 'REG', 'DEP', 'LIBGEO'], value_vars=year_columns, variable_name='year', value_name='pop')

# Extract the last 4 digits of the year into 'rp_type, update 'year'
df_melted = df_melted.with_columns(
    rp_type=pl.col('year').str.head(-4),
    year=pl.col('year').str.slice(-4).cast(pl.Int32)
    )

# Display the resulting DataFrame
print(df_melted.head())

# Create filesystem object
S3_ENDPOINT_URL = "https://" + os.environ["AWS_S3_ENDPOINT"]
fs = s3fs.S3FileSystem(client_kwargs={'endpoint_url': S3_ENDPOINT_URL})

FILE_KEY_OUT_S3 = "2025_hackathon/pop.parquet"
FILE_PATH_OUT_S3 = os.environ['AWS_WORKING_DIRECTORY_PATH'] + FILE_KEY_OUT_S3

with fs.open(FILE_PATH_OUT_S3, 'w') as file_out:
    df_melted.write_parquet(file_out)

