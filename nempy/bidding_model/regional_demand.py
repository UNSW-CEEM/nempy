from nemosis import data_fetch_methods
import pandas as pd

raw_data_cache = 'nem_data'

def get_regional_demand(start_time, end_time):

    dispatch_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHREGIONSUM', raw_data_cache,
                                                             select_columns=['SETTLEMENTDATE', 'REGIONID', 'TOTALDEMAND'])

    dispatch_data['TOTALDEMAND'] = pd.to_numeric(dispatch_data['TOTALDEMAND'])

    dispatch_data = dispatch_data.pivot_table(values='TOTALDEMAND', index='SETTLEMENTDATE', columns='REGIONID')

    dispatch_data = dispatch_data.reset_index().fillna('0.0')

    dispatch_data = dispatch_data.rename(columns={'QLD1': 'qld', 'NSW1': 'nsw', 'VIC1': 'vic', 'SA1': 'sa',
                                                  'TAS1': 'tas'})

    dispatch_data.columns = [col + '-demand' if col != 'SETTLEMENTDATE' else col for col in dispatch_data.columns]

    return dispatch_data