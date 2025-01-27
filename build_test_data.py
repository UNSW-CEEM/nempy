import sqlite3
from nempy.historical_inputs import mms_db, xml_cache
from nempy.historical_inputs import loaders

con = sqlite3.connect('D:/nempy_2024_07/market_management_system.db')
mms_db_manager = mms_db.DBManager(connection=con)
xml_cache_manager = xml_cache.XMLCacheManager('D:/nempy_2024_07/xml_cache')
# mms_db_manager.populate(start_year=2019, start_month=1, end_year=2019, end_month=1)
# mms_db_manager._create_sample_database('2024/07/10 12:05:00')
# xml_cache_manager.populate(start_year=2019, start_month=1, end_year=2019, end_month=1)
# con.execute("VACUUM")
# con.close()

inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
inputs_loader.set_interval('2024/07/10 12:05:00')
print(inputs_loader.xml.get_file_path())