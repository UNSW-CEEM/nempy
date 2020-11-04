import sqlite3
from nempy.historical_inputs import mms_db, xml_cache


con = sqlite3.connect('market_management_system.db')
mms_db_manager = mms_db.DBManager(connection=con)
# xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
mms_db_manager.populate(start_year=2019, start_month=1, end_year=2019, end_month=1)
mms_db_manager._create_sample_database('2019/01/10 12:05:00')
# xml_cache_manager.populate(start_year=2019, start_month=1, end_year=2019, end_month=1)