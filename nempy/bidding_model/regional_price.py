from nemosis import data_fetch_methods
import pandas as pd

raw_data_cache = 'nem_data'

def get_regional_prices(start_time, end_time):

    dispatch_data = data_fetch_methods.dynamic_data_compiler(start_time, end_time, 'DISPATCHPRICE', raw_data_cache,
                                                             select_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP'])

    dispatch_data['RRP'] = pd.to_numeric(dispatch_data['RRP'])

    dispatch_data = dispatch_data.pivot_table(values='RRP', index='SETTLEMENTDATE', columns='REGIONID')

    dispatch_data = dispatch_data.reset_index().fillna('0.0')

    dispatch_data = dispatch_data.rename(columns={'QLD1': 'qld', 'NSW1': 'nsw', 'VIC1': 'vic', 'SA1': 'sa',
                                                  'TAS1': 'tas'})

    dispatch_data.columns = [col + '-energy' if col != 'SETTLEMENTDATE' else col for col in dispatch_data.columns]

    return dispatch_data