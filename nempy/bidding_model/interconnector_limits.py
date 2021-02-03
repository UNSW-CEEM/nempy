from nemosis import data_fetch_methods
import pandas as pd

raw_data_cache = 'nem_data'


def get_interconnector_limits(start_time, end_time):

    dispatch_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHINTERCONNECTORRES', raw_data_cache,
                                                             select_columns=['SETTLEMENTDATE', 'INTERCONNECTORID', 'IMPORTLIMIT', 'EXPORTLIMIT'])

    dispatch_data['IMPORTLIMIT'] = pd.to_numeric(dispatch_data['IMPORTLIMIT'])
    dispatch_data['EXPORTLIMIT'] = pd.to_numeric(dispatch_data['EXPORTLIMIT'])

    dispatch_data_1 = dispatch_data.copy()

    dispatch_data['INTERCONNECTORID'] = dispatch_data['INTERCONNECTORID'] + ' IMPORTLIMIT'
    dispatch_data = dispatch_data.pivot_table(values='IMPORTLIMIT', index='SETTLEMENTDATE', columns='INTERCONNECTORID')
    dispatch_data = dispatch_data.reset_index().fillna('0.0')

    dispatch_data_1['INTERCONNECTORID'] = dispatch_data_1['INTERCONNECTORID'] + ' EXPORTLIMIT'
    dispatch_data_1 = dispatch_data_1.pivot_table(values='EXPORTLIMIT', index='SETTLEMENTDATE', columns='INTERCONNECTORID')
    dispatch_data_1 = dispatch_data_1.reset_index().fillna('0.0')

    dispatch_data = pd.merge(dispatch_data, dispatch_data_1, on='SETTLEMENTDATE')

    return dispatch_data