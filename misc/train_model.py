import os
import tempfile
import zipfile
import requests

import xarray as xr
import numpy as np
import polars as pl
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from tqdm import tqdm

# 1) Charger ton NetCDF et générer les points
ds = xr.open_dataset(
    "https://object.files.data.gouv.fr/meteofrance-drias/SocleM-Climat-2025/CPRCM/METROPOLE/ALPX-3/CNRM-ESM2-1/r1i1p1f2/CNRM-AROME46t1/ssp370/day/tasmaxAdjust/version-hackathon-102025/tasmaxAdjust_FR-Metro_CNRM-ESM2-1_ssp370_r1i1p1f2_CNRM-MF_CNRM-AROME46t1_v1-r1_MF-CDFt-ANASTASIA-ALPX-3-1991-2020_day_20900101-20991231.nc",
    engine="h5netcdf"
)

ds = ds.sel(time=ds.time.dt.year.isin([2093, 2095, 2099]))

df = ds.to_dataframe().reset_index()

da = ds["tasmaxAdjust"]

time = da["time"].values.astype("datetime64[D]")
y = da["y"].values
x = da["x"].values

# Création du DataFrame long en polars
pl_df = pl.DataFrame({
    "date": np.repeat(time, len(y) * len(x)),
    "y": np.tile(np.repeat(y, len(x)), len(time)),
    "x": np.tile(x, len(time) * len(y)),
    "target": da.values.reshape(-1)
})

pl_df = pl_df.with_columns(pl.col("date").cast(pl.Date))
pl_df = pl_df.drop_nans()


result = (
    pl_df
    .with_columns(
        pl.date(pl.col("date").dt.year(), 1, 1).alias("year")
    )
    .group_by("year", "y", "x")
    .agg([
        pl.col("target").max().alias("max_temp"),
        (pl.col("target") > 308).sum().alias("days_above_308K")
    ])
    .sort("year", "y", "x")
)

from cwhpp import create_price_model_pipeline
import lightgbm

col_to_predict = "max_temp"

cols = set(["days_above_308K", "max_temp"])
cols.remove(col_to_predict)
result = result.drop(cols)


from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    result.drop(col_to_predict), result[col_to_predict], test_size=0.15, random_state=42)

y_train = y_train.to_numpy()
y_test = y_test.to_numpy()




class PatchedLGBMRegressor(lightgbm.LGBMRegressor):

    @property
    def feature_names_in_(self):
        return self._feature_name

    @feature_names_in_.setter
    def feature_names_in_(self, x):
        self._feature_name = x






model = create_price_model_pipeline(
    model=PatchedLGBMRegressor(verbose=-1),
    presence_coordinates=True,
    convert_to_pandas_before_fit=False
)

params = {
    "coord_rotation__coordinates_names": ("x", "y"),
    "coord_rotation__number_axis": 11,
    "date_conversion__transaction_date_name": "year",
    "date_conversion__reference_date": "2093-01-01",
    "price_model__seed": 20230516,
    # "price_model__verbose": 0,
    "price_model__n_estimators": 10000,
    "price_model__learning_rate": 0.2,
    "price_model__num_leaves": 1023,
    "price_model__max_depth": 15,
    "price_model__max_bins": 3000,
    "price_model__min_child_samples": 75,
    "price_model__lambda_l2": 20,
    "price_model__min_gain_to_split": 0.0006,
    "price_model__bagging_fraction": 1,
    "price_model__bagging_freq": 0,
    "price_model__feature_fraction": 0.4
}

model.set_params(**params)

preprocessing_pipeline = model[:-1]




preprocessing_pipeline.fit(X_train, y_train)
X_test_transformed = preprocessing_pipeline.transform(X_test)
X_train_transformed = preprocessing_pipeline.transform(X_train)



eval_set = [
    (X_train_transformed, y_train),
    (X_test_transformed, y_test)
]
eval_names = ["Train", "Validation"]


callbacks = [
    lightgbm.log_evaluation(period=50),
    lightgbm.early_stopping(stopping_rounds=25)
]
gradient_boosting = model[-1]
gradient_boosting.fit(
    X=X_train_transformed,
    y=y_train,
    eval_set=eval_set,
    eval_names=eval_names, 
    callbacks=callbacks
)

pred = model.predict(X_test)
pred_train = model.predict(X_train) 


from sklearn.metrics import r2_score

r2_score(y_test, pred)
