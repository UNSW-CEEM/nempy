from datetime import datetime, timedelta


def build_market_management_system_database(market_management_system_database, start_year, start_month, end_year,
                                            end_month):

    mms_db = market_management_system_database
    mms_db.create_tables()

    # Download data were inputs are needed on a monthly basis.
    finished = False
    for year in range(start_year, end_year + 1):
        for month in range(start_month, 13):
            if year == end_year and month == end_month + 1:
                finished = True
                break

            mms_db.DISPATCHINTERCONNECTORRES.add_data(year=year, month=month)
            mms_db.DISPATCHREGIONSUM.add_data(year=year, month=month)
            mms_db.DISPATCHLOAD.add_data(year=year, month=month)
            mms_db.BIDPEROFFER_D.add_data(year=year, month=month)
            mms_db.BIDDAYOFFER_D.add_data(year=year, month=month)
            mms_db.DISPATCHCONSTRAINT.add_data(year=year, month=month)
            mms_db.DISPATCHPRICE.add_data(year=year, month=month)

        if finished:
            break

        start_month = 1

    # Download data where inputs are just needed from the latest month.
    mms_db.INTERCONNECTOR.set_data(year=end_year, month=end_month)
    mms_db.LOSSFACTORMODEL.set_data(year=end_year, month=end_month)
    mms_db.LOSSMODEL.set_data(year=end_year, month=end_month)
    mms_db.DUDETAILSUMMARY.create_table_in_sqlite_db()
    mms_db.DUDETAILSUMMARY.set_data(year=end_year, month=end_month)
    mms_db.DUDETAIL.set_data(year=end_year, month=end_month)
    mms_db.INTERCONNECTORCONSTRAINT.set_data(year=end_year, month=end_month)
    mms_db.GENCONDATA.set_data(year=end_year, month=end_month)
    mms_db.SPDCONNECTIONPOINTCONSTRAINT.set_data(year=end_year, month=end_month)
    mms_db.SPDREGIONCONSTRAINT.set_data(year=end_year, month=end_month)
    mms_db.SPDINTERCONNECTORCONSTRAINT.set_data(year=end_year, month=end_month)
    mms_db.INTERCONNECTOR.set_data(year=end_year, month=end_month)
    mms_db.MNSP_INTERCONNECTOR.create_table_in_sqlite_db()
    mms_db.MNSP_INTERCONNECTOR.set_data(year=end_year, month=end_month)
    mms_db.DUDETAIL.create_table_in_sqlite_db()
    mms_db.DUDETAIL.set_data(year=end_year, month=end_month)


def build_xml_inputs_cache(nemde_xml_cache_manager, start_year, start_month, end_year, end_month):
    start = datetime(year=start_year, month=start_month, day=1)
    if end_month == 12:
        end_month = 0
        end_year += 1
    end = datetime(year=end_year, month=end_month + 1, day=1)
    download_date = start
    while download_date <= end:
        print(download_date)
        download_date_str = download_date.isoformat().replace('T', ' ').replace('-', '/')
        nemde_xml_cache_manager.download_data(download_date_str)
        download_date += timedelta(days=1)


def find_intervals_with_violations(nemde_xml_cache_manager, limit, start_year, start_month, end_year, end_month):
    start = datetime(year=start_year, month=start_month, day=1)
    end = datetime(year=end_year, month=end_month + 1, day=1)
    check_time = start
    intervals = {}
    while check_time <= end and len(intervals) < limit:
        print(check_time)
        time_as_str = check_time.isoformat().replace('T', ' ').replace('-', '/')
        nemde_xml_cache_manager.load_interval(time_as_str)
        violations = nemde_xml_cache_manager.get_non_intervention_violations()
        for violation_type, violation_value in violations.items():
            if violation_value > 0.0:
                if time_as_str not in intervals:
                    intervals[time_as_str] = []
                intervals[time_as_str].append(violation_type)
        check_time += timedelta(minutes=5)
    return intervals


class RawInputsLoader:
    def __init__(self, nemde_xml_cache_manager, market_management_system_database):
        self.xml = nemde_xml_cache_manager
        self.mms_db = market_management_system_database
        self.interval = None

    def set_interval(self, interval):
        self.interval = interval
        self.xml.load_interval(interval)

    def get_unit_initial_conditions_dataframe(self):
        return self.xml.get_unit_initial_conditions_dataframe()

    def get_unit_volume_bids(self):
        return self.xml.get_unit_volume_bids()

    def get_unit_price_bids(self):
        return self.mms_db.BIDDAYOFFER_D.get_data(self.interval)

    def get_unit_details(self):
        return self.mms_db.DUDETAILSUMMARY.get_data(self.interval)

    def get_agc_enablement_limits(self):
        return self.mms_db.DISPATCHLOAD.get_data(self.interval)

    def get_UGIF_values(self):
        return self.xml.get_UGIF_values()

    def get_violations(self):
        return self.xml.get_violations()

    def get_constraint_violation_prices(self):
        return self.xml.get_constraint_violation_prices()

    def get_constraint_rhs(self):
        return self.xml.get_constraint_rhs()

    def get_constraint_type(self):
        return self.xml.get_constraint_type()

    def get_constraint_region_lhs(self):
        return self.xml.get_constraint_region_lhs()

    def get_constraint_unit_lhs(self):
        return self.xml.get_constraint_unit_lhs()

    def get_constraint_interconnector_lhs(self):
        return self.xml.get_constraint_interconnector_lhs()

    def get_market_interconnectors(self):
        return self.mms_db.MNSP_INTERCONNECTOR.get_data(self.interval)

    def get_market_interconnector_link_bid_availability(self):
        return self.xml.get_market_interconnector_link_bid_availability()

    def get_interconnector_constraint_parameters(self):
        return self.mms_db.INTERCONNECTORCONSTRAINT.get_data(self.interval)

    def get_interconnector_definitions(self):
        return self.mms_db.INTERCONNECTOR.get_data()

    def get_regional_loads(self):
        return self.mms_db.DISPATCHREGIONSUM.get_data(self.interval)

    def get_interconnector_loss_segments(self):
        return self.mms_db.LOSSMODEL.get_data(self.interval)

    def get_interconnector_loss_paramteters(self):
        return self.mms_db.LOSSFACTORMODEL.get_data(self.interval)

    def get_unit_fast_start_parameters(self):
        return self.xml.get_unit_fast_start_parameters()
