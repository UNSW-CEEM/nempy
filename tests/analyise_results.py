import pandas as pd
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, \
    constraints
import sqlite3

interval = '2021/12/30 22:45:00'

con = sqlite3.connect('D:/nempy_2021/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

# prices = pd.read_csv('prices.csv')

dispatch = pd.read_csv('unit_dispatch.csv')

historical_prices = mms_db_manager.DISPATCHPRICE.get_data(interval)

# prices = pd.merge(prices, historical_prices,
#                   left_on=['region'],
#                   right_on=['REGIONID'])

historical_dispatch = mms_db_manager.DISPATCHLOAD.get_data(interval)

dispatch = pd.merge(dispatch, historical_dispatch,
                  left_on=['unit'],
                  right_on=['DUID'])

dispatch_errors = dispatch[dispatch['service'] == 'energy'].copy()

dispatch_errors['error'] = dispatch_errors['dispatch'] - dispatch['TOTALCLEARED']

dispatch_errors = dispatch_errors[dispatch_errors['error'].abs() > 1.0]

dispatch_errors.to_csv('dispatch_errors.csv')

x=1