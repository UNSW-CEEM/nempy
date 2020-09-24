import sqlite3
import pickle
from nempy.historical_inputs import mms_db, xml_cache


running_for_first_time = False

con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')

if running_for_first_time:
    mms_db_manager.populate(start_year=2020, start_month=8, end_year=2020, end_month=8)
    xml_cache_manager.populate(start_year=2020, start_month=8, end_year=2020, end_month=8)

get_violation_intervals = True

if get_violation_intervals:
    interval_with_fast_start_violations = \
        xml_cache_manager.find_intervals_with_violations(limit=100000, start_year=2019, start_month=1,
                                                         end_year=2019, end_month=12)

    with open('interval_with_violations.pickle', 'wb') as f:
        pickle.dump(interval_with_fast_start_violations, f, pickle.HIGHEST_PROTOCOL)

con.close()
