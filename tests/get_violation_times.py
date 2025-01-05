import pickle

from nempy.historical_inputs import xml_cache

import os
cwd = os.getcwd()

print(cwd)

historical_inputs = xml_cache.XMLCacheManager('D:/nempy_2024_07/xml_cache')

interval_with_violations = \
    historical_inputs.find_intervals_with_violations(limit=1000000,
                                                     start_year=2024, start_month=7,
                                                     end_year=2024, end_month=7)

with open('interval_with_violations_2024_07.pickle', 'wb') as f:
    pickle.dump(interval_with_violations, f, pickle.HIGHEST_PROTOCOL)
