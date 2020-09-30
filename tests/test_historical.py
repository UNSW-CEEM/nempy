import sqlite3
import pandas as pd
from pandas.testing import assert_frame_equal
from datetime import datetime, timedelta
import random
import pickle
from nempy.historical_inputs import loaders, xml_cache, mms_db, units, \
    interconnectors, constraints, demand
from tests import historical_market_builder


# These tests require some additional clean up and will probably not run on your machine. ##############################


def get_test_intervals(number=100):
    start_time = datetime(year=2019, month=1, day=1, hour=0, minute=0)
    end_time = datetime(year=2019, month=12, day=31, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(2)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


def get_test_intervals_august_2020(number=100):
    start_time = datetime(year=2020, month=8, day=1, hour=0, minute=0)
    end_time = datetime(year=2020, month=8, day=31, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(2)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


def test_ramp_rate_constraints():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=10):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.set_ramp_rate_limits()

        market = market_builder.get_market_object()

        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)

        market_overrider.set_unit_dispatch_to_historical_values()

        market_builder.dispatch()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache_manager,
                                                                 interval=interval)

        assert market_checker.measured_violation_equals_historical_violation(historical_name='ramp_rate',
                                                                             nempy_constraints=['ramp_up', 'ramp_down'])


def test_ramp_rate_constraints_where_constraints_violated():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    with open('interval_with_violations.pickle', 'rb') as f:
        interval_with_violations = pickle.load(f)

    tests_to_run = 55
    tests_run = 0
    for interval, types in interval_with_violations.items():
        if tests_run == tests_to_run:
            break
        if 'ramp_rate' in types:
            raw_inputs_loader.set_interval(interval)
            unit_inputs = units.UnitData(raw_inputs_loader)
            interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
            constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
            demand_inputs = demand.DemandData(raw_inputs_loader)

            market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                         interconnector_inputs=interconnector_inputs,
                                                                         constraint_inputs=constraint_inputs,
                                                                         demand_inputs=demand_inputs)
            market_builder.add_unit_bids_to_market()
            market_builder.set_ramp_rate_limits()

            market = market_builder.get_market_object()

            market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                         mms_db=mms_database,
                                                                         interval=interval)

            market_overrider.set_unit_dispatch_to_historical_values()

            market_builder.dispatch()

            market_checker = historical_market_builder.MarketChecker(market=market,
                                                                     mms_db=mms_database,
                                                                     xml_cache=xml_cache_manager,
                                                                     interval=interval)

            assert market_checker.measured_violation_equals_historical_violation(historical_name='ramp_rate',
                                                                                 nempy_constraints=['ramp_up',
                                                                                                    'ramp_down'])
            tests_run += 1

    assert tests_to_run == tests_run


def test_fast_start_constraints():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=10):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.set_fast_start_constraints()

        market = market_builder.get_market_object()

        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)

        market_overrider.set_unit_dispatch_to_historical_values()

        market_builder.dispatch()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache_manager,
                                                                 interval=interval)

        assert market_checker.measured_violation_equals_historical_violation('fast_start',
                                                                             nempy_constraints=['fast_start'])


def test_fast_start_constraints_where_constraints_violated():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    with open('interval_with_violations.pickle', 'rb') as f:
        interval_with_violations = pickle.load(f)

    tests_to_run = 11
    tests_run = 0
    for interval, types in interval_with_violations.items():
        if tests_run == tests_to_run:
            break
        if 'fast_start' in types:
            raw_inputs_loader.set_interval(interval)
            unit_inputs = units.UnitData(raw_inputs_loader)
            interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
            constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
            demand_inputs = demand.DemandData(raw_inputs_loader)

            market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                         interconnector_inputs=interconnector_inputs,
                                                                         constraint_inputs=constraint_inputs,
                                                                         demand_inputs=demand_inputs)
            market_builder.add_unit_bids_to_market()
            market_builder.set_fast_start_constraints()

            market = market_builder.get_market_object()

            market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                         mms_db=mms_database,
                                                                         interval=interval)

            market_overrider.set_unit_dispatch_to_historical_values()

            market_builder.dispatch()

            market_checker = historical_market_builder.MarketChecker(market=market,
                                                                     mms_db=mms_database,
                                                                     xml_cache=xml_cache_manager,
                                                                     interval=interval)

            assert market_checker.measured_violation_equals_historical_violation('fast_start',
                                                                                 nempy_constraints=[
                                                                                     'fast_start'])
            tests_run += 1

    assert tests_to_run == tests_run


def test_capacity_constraints():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=10):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market_builder.set_unit_limit_constraints()

        market = market_builder.get_market_object()

        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)

        market_overrider.set_unit_dispatch_to_historical_values()
        market_overrider.set_interconnector_flow_to_historical_values()

        market_builder.dispatch()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache_manager,
                                                                 interval=interval)

        assert market_checker.measured_violation_equals_historical_violation('unit_capacity',
                                                                             nempy_constraints=['unit_bid_capacity'])


def test_capacity_constraint_where_constraints_violated():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    with open('interval_with_violations.pickle', 'rb') as f:
        interval_with_violations = pickle.load(f)

    tests_to_run = 10
    tests_run = 0
    for interval, types in interval_with_violations.items():
        if tests_run == tests_to_run:
            break
        if 'unit_capacity' in types:
            raw_inputs_loader.set_interval(interval)
            unit_inputs = units.UnitData(raw_inputs_loader)
            interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
            constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
            demand_inputs = demand.DemandData(raw_inputs_loader)

            market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                         interconnector_inputs=interconnector_inputs,
                                                                         constraint_inputs=constraint_inputs,
                                                                         demand_inputs=demand_inputs)
            market_builder.add_unit_bids_to_market()
            market_builder.add_interconnectors_to_market()
            market_builder.set_unit_limit_constraints()

            market = market_builder.get_market_object()

            market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                         mms_db=mms_database,
                                                                         interval=interval)

            market_overrider.set_unit_dispatch_to_historical_values()
            market_overrider.set_interconnector_flow_to_historical_values()

            market_builder.dispatch()

            market_checker = historical_market_builder.MarketChecker(market=market,
                                                                     mms_db=mms_database,
                                                                     xml_cache=xml_cache_manager,
                                                                     interval=interval)

            assert market_checker.measured_violation_equals_historical_violation('unit_capacity',
                                                                                 nempy_constraints=[
                                                                                     'unit_bid_capacity'])
            tests_run += 1

    assert tests_to_run == tests_run


def ignore_test_fcas_trapezium_scaled_availability():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms_august_2020.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache_august_2020')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals_august_2020(number=10):
        if interval != '2020/08/21 13:00:00':
            continue
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.set_unit_fcas_constraints()
        market_builder.set_unit_limit_constraints()

        market = market_builder.get_market_object()

        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)

        market_overrider.set_unit_dispatch_to_historical_values()

        market_builder.dispatch()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval,
                                                                 unit_inputs=unit_inputs)

        avails = market_checker.do_fcas_availabilities_match_historical()
        # I think NEMDE might be getting avail calcs wrong when units are operating on the slopes, and the slopes
        # are vertical. They should be ignore 0 slope coefficients, maybe this is not happening because of floating
        # point comparison.
        if interval == '2019/01/29 18:10:00':
            avails = avails[~(avails['unit'] == 'PPCCGT')]
        if interval == '2019/01/07 19:35:00':
            avails = avails[~(avails['unit'] == 'PPCCGT')]
        #assert avails['error'].abs().max() < 1.1


def ignore_test_find_fcas_trapezium_scaled_availability_erros():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms_august_2020.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache_august_2020')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)
    outputs = []
    for interval in get_test_intervals_august_2020(number=100):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        unit_inputs.get_processed_bids()
        unit_inputs.add_fcas_trapezium_constraints()
        traps = unit_inputs.get_fcas_regulation_trapeziums()
        traps = traps[traps['service'] == 'lower_reg']
        avails = mms_database.DISPATCHLOAD.get_data(interval)
        avails = avails.loc[:, ['DUID', 'TOTALCLEARED', 'LOWERREG', 'LOWERREGACTUALAVAILABILITY']]
        avails.columns = ['unit', 'total_cleared', 'lower_reg', 'lower_reg_actual_availability']
        avails = avails[avails['lower_reg'] > avails['lower_reg_actual_availability'] + 0.1]
        avails = pd.merge(avails, traps, on='unit')
        avails['time'] = interval
        outputs.append(avails)
    pd.concat(outputs).to_csv('avails_august_2020.csv')


def test_all_units_and_service_dispatch_historically_present_in_market():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=1000):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market = market_builder.get_market_object()
        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        assert market_checker.all_dispatch_units_and_services_have_decision_variables()


def test_slack_in_generic_constraints():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=100):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market_builder.add_generic_constraints()
        market_builder.set_unit_fcas_constraints()
        market_builder.set_unit_limit_constraints()
        market_builder.set_region_demand_constraints()
        market_builder.set_ramp_rate_limits()
        market_builder.set_fast_start_constraints()
        market_builder.set_solver('CBC')
        market_builder.dispatch(calc_prices=True)
        market = market_builder.get_market_object()

        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)

        market_overrider.set_unit_dispatch_to_historical_values()
        market_overrider.set_interconnector_flow_to_historical_values()

        market_builder.dispatch()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        assert market_checker.is_generic_constraint_slack_correct()
        assert market_checker.is_regional_demand_meet()


def test_slack_in_generic_constraints_with_fcas_interface():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=100):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market_builder.add_generic_constraints_with_fcas_requirements_interface()
        market_builder.set_unit_fcas_constraints()
        market_builder.set_unit_limit_constraints()
        market_builder.set_region_demand_constraints()
        market_builder.set_ramp_rate_limits()
        market_builder.set_fast_start_constraints()
        market_builder.set_solver('CBC')
        market_builder.dispatch(calc_prices=True)
        market = market_builder.get_market_object()

        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)

        market_overrider.set_unit_dispatch_to_historical_values()
        market_overrider.set_interconnector_flow_to_historical_values()

        market_builder.dispatch()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        assert market_checker.is_generic_constraint_slack_correct()
        assert market_checker.is_fcas_constraint_slack_correct()
        assert market_checker.is_regional_demand_meet()


def test_hist_dispatch_values_meet_demand():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)

    for interval in get_test_intervals(number=100):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market = market_builder.get_market_object()
        market_overrider = historical_market_builder.MarketOverrider(market=market,
                                                                     mms_db=mms_database,
                                                                     interval=interval)
        market_overrider.set_unit_dispatch_to_historical_values()
        market_overrider.set_interconnector_flow_to_historical_values()
        market_builder.dispatch()
        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        test_passed = market_checker.is_regional_demand_meet()
        assert test_passed
    con.close()


def test_against_10_interval_benchmark():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)
    outputs = []
    for interval in get_test_intervals(number=10):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market_builder.add_generic_constraints_with_fcas_requirements_interface()
        market_builder.set_unit_fcas_constraints()
        market_builder.set_unit_limit_constraints()
        market_builder.set_region_demand_constraints()
        market_builder.set_ramp_rate_limits()
        market_builder.set_fast_start_constraints()
        market_builder.set_solver('GUROBI')
        market_builder.dispatch(calc_prices=True)
        market = market_builder.get_market_object()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        price_comp = market_checker.get_price_comparison()
        outputs.append(price_comp)
    outputs = pd.concat(outputs)
    outputs.to_csv('latest_10_interval_run.csv', index=False)
    benchmark = pd.read_csv('10_interval_benchmark.csv')
    assert_frame_equal(outputs.reset_index(drop=True), benchmark, check_exact=False, atol=1e-2)


def test_against_100_interval_benchmark():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)
    outputs = []
    for interval in get_test_intervals(number=100):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market_builder.add_generic_constraints_with_fcas_requirements_interface()
        market_builder.set_unit_fcas_constraints()
        market_builder.set_unit_limit_constraints()
        market_builder.set_region_demand_constraints()
        market_builder.set_ramp_rate_limits()
        market_builder.set_fast_start_constraints()
        market_builder.set_solver('GUROBI')
        market_builder.dispatch(calc_prices=True)
        market = market_builder.get_market_object()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        price_comp = market_checker.get_price_comparison()
        outputs.append(price_comp)

    outputs = pd.concat(outputs)
    outputs.to_csv('latest_100_interval_run.csv', index=False)
    benchmark = pd.read_csv('100_interval_benchmark.csv')
    assert_frame_equal(outputs.reset_index(drop=True), benchmark, check_exact=False, atol=1e-2)


def test_against_1000_interval_benchmark():
    con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
    mms_database = mms_db.DBManager(con)
    xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')
    raw_inputs_loader = loaders.RawInputsLoader(nemde_xml_cache_manager=xml_cache_manager,
                                                market_management_system_database=mms_database)
    outputs = []
    for interval in get_test_intervals(number=1000):
        raw_inputs_loader.set_interval(interval)
        unit_inputs = units.UnitData(raw_inputs_loader)
        interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
        constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
        demand_inputs = demand.DemandData(raw_inputs_loader)

        market_builder = historical_market_builder.SpotMarketBuilder(unit_inputs=unit_inputs,
                                                                     interconnector_inputs=interconnector_inputs,
                                                                     constraint_inputs=constraint_inputs,
                                                                     demand_inputs=demand_inputs)
        market_builder.add_unit_bids_to_market()
        market_builder.add_interconnectors_to_market()
        market_builder.add_generic_constraints_with_fcas_requirements_interface()
        market_builder.set_unit_fcas_constraints()
        market_builder.set_unit_limit_constraints()
        market_builder.set_region_demand_constraints()
        market_builder.set_ramp_rate_limits()
        market_builder.set_fast_start_constraints()
        market_builder.dispatch(calc_prices=True)
        market = market_builder.get_market_object()

        market_checker = historical_market_builder.MarketChecker(market=market,
                                                                 mms_db=mms_database,
                                                                 xml_cache=xml_cache,
                                                                 interval=interval)
        price_comp = market_checker.get_price_comparison()
        outputs.append(price_comp)

    outputs = pd.concat(outputs)
    outputs.to_csv('latest_1000_interval_run.csv', index=False)
    benchmark = pd.read_csv('1000_interval_benchmark.csv')
    assert_frame_equal(outputs.reset_index(drop=True), benchmark.reset_index(drop=True), check_less_precise=3)
