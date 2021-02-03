from nemosis import data_fetch_methods
import pandas as pd
import numpy as np

raw_data_cache = 'nem_data'


def get_fleet_dispatch(start_time, end_time, fleet_units):

    dispatch_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHLOAD', raw_data_cache,
                                                             select_columns=['DUID', 'SETTLEMENTDATE', 'TOTALCLEARED',
                                                                             'INTERVENTION'])

    dispatch_data = dispatch_data[dispatch_data['DUID'].isin(fleet_units)]

    dispatch_data = dispatch_data[dispatch_data['INTERVENTION'] == '0']

    dispatch_data = dispatch_data.groupby('SETTLEMENTDATE', as_index=False).aggregate({'TOTALCLEARED': 'sum'})

    dispatch_data = dispatch_data.rename(columns={'TOTALCLEARED': 'fleet_dispatch'})

    return dispatch_data