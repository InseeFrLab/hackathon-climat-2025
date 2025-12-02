from netCDF4 import Dataset


def walktree(top):
    yield top.groups.values()
    for value in top.groups.values():
        yield from walktree(value)


rootgrp = Dataset("tasAdjust_FR-Metro_MPI-ESM1-2-LR_historical_r10i1p1f1_CNRM_CNRM-ALADIN63-emul-CNRM-UNET11-tP22_v1-r1_MF-CDFt-ANASTASIA-SAFRAN-1985-2014_day_18500101-20141231.nc", "r")

print(rootgrp.dimensions)
rootgrp.close()