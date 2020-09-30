import pickle

from nempy.historical_inputs import xml_cache

import os
cwd = os.getcwd()

print(cwd)

historical_inputs = xml_cache.XMLCacheManager('test_files/historical_xml_files')

interval_with_violations = \
    historical_inputs.find_intervals_with_violations(limit=1000000,
                                                     start_year=2019, start_month=2,
                                                     end_year=2019, end_month=2)

with open('interval_with_violations.pickle', 'wb') as f:
    pickle.dump(interval_with_violations, f, pickle.HIGHEST_PROTOCOL)
