import pickle
import sqlite3

from nempy.historical_inputs import loaders

import os
cwd = os.getcwd()

print(cwd)

con = sqlite3.connect('historical.db')
historical_inputs = loaders.HistoricalInputs(
    market_management_system_database_connection=con,
    nemde_xml_cache_folder='historical_xml_files')

interval_with_violations = \
    historical_inputs.find_intervals_with_violations(limit=1000000,
                                                     start_year=2019, start_month=2,
                                                     end_year=2019, end_month=2)

with open('interval_with_violations.pickle', 'wb') as f:
    pickle.dump(interval_with_violations, f, pickle.HIGHEST_PROTOCOL)
