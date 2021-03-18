import pandas as pd
import numpy as np


def main_land_fcas_requirement(system_variables):
    sv = system_variables
    rhs = max([sv['gen1'] + sv['gen2']])
    return rhs


rhs_functions = pd.DataFrame(
    np.array([
        ['F_I+LREG_0210', 'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO', main_land_fcas_requirement]
    ])
)