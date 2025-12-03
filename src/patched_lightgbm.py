import lightgbm

class PatchedLGBMRegressor(lightgbm.LGBMRegressor):

    @property
    def feature_names_in_(self):
        return self._feature_name

    @feature_names_in_.setter
    def feature_names_in_(self, x):
        self._feature_name = x