from datetime import datetime, timedelta

from nempy import historical_spot_market_inputs
from nempy.historical import historical_inputs_from_xml, historical_interconnectors, units


class HistoricalInputs:
    def __init__(self, market_management_system_database_connection, nemde_xml_cache_folder):
        self.nemde_xml_cache_folder = nemde_xml_cache_folder
        self.mms_db = \
            historical_spot_market_inputs.DBManager(connection=market_management_system_database_connection)
        self.xml_inputs = None

    def load_interval(self, interval):
        self.xml_inputs = historical_inputs_from_xml.XMLInputs(self.nemde_xml_cache_folder, interval)

    def build_market_management_system_database(self, start_year, start_month, end_year, end_month):
        self.mms_db.create_tables()

        # Download data were inputs are needed on a monthly basis.
        finished = False
        for year in range(start_year, end_year + 1):
            for month in range(start_month, 13):
                if year == end_year and month == end_month + 1:
                    finished = True
                    break

                self.mms_db.DISPATCHINTERCONNECTORRES.add_data(year=year, month=month)
                self.mms_db.DISPATCHREGIONSUM.add_data(year=year, month=month)
                self.mms_db.DISPATCHLOAD.add_data(year=year, month=month)
                self.mms_db.BIDPEROFFER_D.add_data(year=year, month=month)
                self.mms_db.BIDDAYOFFER_D.add_data(year=year, month=month)
                self.mms_db.DISPATCHCONSTRAINT.add_data(year=year, month=month)
                self.mms_db.DISPATCHPRICE.add_data(year=year, month=month)

            if finished:
                break

            start_month = 1

        # Download data where inputs are just needed from the latest month.
        self.mms_db.INTERCONNECTOR.set_data(year=end_year, month=end_month)
        self.mms_db.LOSSFACTORMODEL.set_data(year=end_year, month=end_month)
        self.mms_db.LOSSMODEL.set_data(year=end_year, month=end_month)
        self.mms_db.DUDETAILSUMMARY.create_table_in_sqlite_db()
        self.mms_db.DUDETAILSUMMARY.set_data(year=end_year, month=end_month)
        self.mms_db.DUDETAIL.set_data(year=end_year, month=end_month)
        self.mms_db.INTERCONNECTORCONSTRAINT.set_data(year=end_year, month=end_month)
        self.mms_db.GENCONDATA.set_data(year=end_year, month=end_month)
        self.mms_db.SPDCONNECTIONPOINTCONSTRAINT.set_data(year=end_year, month=end_month)
        self.mms_db.SPDREGIONCONSTRAINT.set_data(year=end_year, month=end_month)
        self.mms_db.SPDINTERCONNECTORCONSTRAINT.set_data(year=end_year, month=end_month)
        self.mms_db.INTERCONNECTOR.set_data(year=end_year, month=end_month)
        self.mms_db.MNSP_INTERCONNECTOR.create_table_in_sqlite_db()
        self.mms_db.MNSP_INTERCONNECTOR.set_data(year=end_year, month=end_month)
        self.mms_db.DUDETAIL.create_table_in_sqlite_db()
        self.mms_db.DUDETAIL.set_data(year=end_year, month=end_month)

    def build_xml_inputs_cache(self, start_year, start_month, end_year, end_month):
        start = datetime(year=start_year, month=start_month, day=1)
        if end_month == 12:
            end_month = 0
            end_year += 1
        end = datetime(year=end_year, month=end_month + 1, day=1)
        download_date = start
        while download_date <= end:
            print(download_date)
            historical_inputs_from_xml.XMLInputs(self.nemde_xml_cache_folder,
                                                 download_date.isoformat().replace('T', ' ').replace('-', '/'))
            download_date += timedelta(days=1)

    def get_unit_inputs(self, interval):
        return units.HistoricalUnits(self.mms_db, self.xml_inputs, interval)

    def get_interconnector_inputs(self, interval):
        return historical_interconnectors.HistoricalInterconnectors(self.mms_db, self.xml_inputs, interval)

    def find_intervals_with_violations(self, limit, start_year, start_month, end_year, end_month):
        start = datetime(year=start_year, month=start_month, day=1)
        end = datetime(year=end_year, month=end_month + 1, day=1)
        check_time = start
        intervals = {}
        while check_time <= end and len(intervals) < limit:
            print(check_time)
            time_as_str = check_time.isoformat().replace('T', ' ').replace('-', '/')
            xml_inputs = historical_inputs_from_xml.XMLInputs(self.nemde_xml_cache_folder, time_as_str)
            violations = xml_inputs.get_non_intervention_violations()
            for violation_type, violation_value in violations.items():
                if violation_value > 0.0:
                    if time_as_str not in intervals:
                        intervals[time_as_str] = []
                    intervals[time_as_str].append(violation_type)
            check_time += timedelta(minutes=5)
        return intervals

