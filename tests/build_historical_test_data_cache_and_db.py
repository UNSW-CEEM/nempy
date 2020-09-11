import sqlite3
import pickle
from nempy.historical import historical_inputs_from_mms_db, historical_inputs_from_xml, inputs


running_for_first_time = True

con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
market_management_system_db_interface = \
    historical_inputs_from_mms_db.DBManager(connection=con)

nemde_xml_file_cache_interface = \
    historical_inputs_from_xml.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')

if running_for_first_time:
    inputs.build_market_management_system_database(market_management_system_db_interface, start_year=2019,
                                                   start_month=1, end_year=2019, end_month=12)
    inputs.build_xml_inputs_cache(nemde_xml_file_cache_interface, start_year=2019, start_month=1, end_year=2019,
                                  end_month=12)

get_violation_intervals = False

if get_violation_intervals:
    interval_with_fast_start_violations = \
        inputs.find_intervals_with_violations(nemde_xml_file_cache_interface, limit=1, start_year=2019, start_month=2,
                                              end_year=2019, end_month=2)

    with open('interval_with_fast_start_violations.pickle', 'wb') as f:
        pickle.dump(interval_with_fast_start_violations, f, pickle.HIGHEST_PROTOCOL)

con.close()
