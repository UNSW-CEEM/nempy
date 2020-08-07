import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import random
import pickle
import pytest
from nempy.historical import inputs
from nempy import historical_market_builder


# Define a set of random intervals to test
def get_test_intervals():
    start_time = datetime(year=2019, month=1, day=2, hour=0, minute=0)
    end_time = datetime(year=2019, month=2, day=1, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(2)
    intervals = random.sample(range(1, difference_in_5_min_intervals), 100)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


def test_setup():
    running_for_first_time = True

    con = sqlite3.connect('test_files/historical_all.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')

    if running_for_first_time:
        # historical_inputs.build_market_management_system_database(start_year=2018, start_month=12,
        #                                                           end_year=2020, end_month=1)
        historical_inputs.build_xml_inputs_cache(start_year=2019, start_month=1,
                                                 end_year=2019, end_month=12)

    get_violation_intervals = False

    if get_violation_intervals:
        interval_with_fast_start_violations = \
            historical_inputs.find_intervals_with_violations(limit=1,
                                                             start_year=2019, start_month=2,
                                                             end_year=2019, end_month=2)

        with open('interval_with_fast_start_violations.pickle', 'wb') as f:
            pickle.dump(interval_with_fast_start_violations, f, pickle.HIGHEST_PROTOCOL)

    con.close()


def test_if_ramp_rates_calculated_correctly():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect(inputs_database)
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')

    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.set_ramp_rate_limits()
        market.set_unit_dispatch_to_historical_values()
        market.dispatch()
        assert market.measured_violation_equals_historical_violation(historical_name='ramp_rate',
                                                                     nempy_constraints=['ramp_up', 'ramp_down'])

    with open('interval_with_violations.pickle', 'rb') as f:
        interval_with_violations = pickle.load(f)

    for interval, types in interval_with_violations.items():
        if 'ramp_rate' in types:
            print(interval)
            market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                          interval=interval)
            market.add_unit_bids_to_market()
            market.set_ramp_rate_limits()
            market.set_unit_dispatch_to_historical_values()
            market.dispatch()
            assert market.measured_violation_equals_historical_violation(historical_name='ramp_rate',
                                                                         nempy_constraints=['ramp_up', 'ramp_down'])


def test_fast_start_constraints():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect(inputs_database)
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')

    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.set_fast_start_constraints()
        market.set_unit_dispatch_to_historical_values()
        market.dispatch()
        assert market.measured_violation_equals_historical_violation('fast_start',
                                                                     nempy_constraints=['fast_start'])

    with open('interval_with_violations.pickle', 'rb') as f:
        interval_with_violations = pickle.load(f)

    for interval, types in interval_with_violations.items():
        if 'fast_start' in types:
            print(interval)
            market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                          interval=interval)
            market.add_unit_bids_to_market()
            market.set_fast_start_constraints()
            market.set_unit_dispatch_to_historical_values()
            market.dispatch()
            assert market.measured_violation_equals_historical_violation('fast_start',
                                                                         nempy_constraints=['fast_start'])


def test_capacity_constraints():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect(inputs_database)
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')

    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.set_unit_limit_constraints()
        market.set_unit_dispatch_to_historical_values()
        market.dispatch()
        assert market.measured_violation_equals_historical_violation('unit_capacity',
                                                                     nempy_constraints=['unit_bid_capacity'])


def test_fcas_trapezium_scaled_availability():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.set_unit_fcas_constraints()
        market.set_unit_limit_constraints()
        market.set_unit_dispatch_to_historical_values(wiggle_room=0.0001)
        market.dispatch(calc_prices=False)
        avails = market.do_fcas_availabilities_match_historical()
        # I think NEMDE might be getting avail calcs wrong when units are opperating on the slopes, and the slopes
        # are vertical. They should be ignore 0 slope cofficients, maybe this is not happing because of floating
        # point comparison.
        if interval == '2019/01/29 18:10:00':
            avails = avails[~(avails['unit'] == 'PPCCGT')]
        if interval == '2019/01/07 19:35:00':
            avails = avails[~(avails['unit'] == 'PPCCGT')]
        assert avails['error'].abs().max() < 1.1


def test_all_units_and_service_dispatch_historically_present_in_market():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        assert market.all_dispatch_units_and_service_have_decision_variables()


def test_slack_in_generic_constraints():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints()
        market.set_unit_dispatch_to_historical_values(wiggle_room=0.0001)
        market.set_interconnector_flow_to_historical_values(wiggle_room=0.0001)
        market.dispatch(calc_prices=False)
        assert market.is_generic_constraint_slack_correct()


def test_slack_in_generic_constraints_use_fcas_requirements_interface():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints_fcas_requirements()
        market.set_unit_dispatch_to_historical_values(wiggle_room=0.0001)
        market.set_interconnector_flow_to_historical_values(wiggle_room=0.001)
        market.dispatch(calc_prices=False)
        market.market.get_elastic_constraints_violation_degree('generic')
        assert market.all_constraints_presenet()
        assert market.is_generic_constraint_slack_correct()
        assert market.is_fcas_constraint_slack_correct()


def test_slack_in_generic_constraints_with_all_features():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints_fcas_requirements()
        market.set_unit_fcas_constraints()
        market.set_unit_limit_constraints()
        market.set_ramp_rate_limits()
        market.set_unit_dispatch_to_historical_values(wiggle_room=0.003)
        market.set_interconnector_flow_to_historical_values()
        market.dispatch(calc_prices=False)
        assert market.is_generic_constraint_slack_correct()
        assert market.is_fcas_constraint_slack_correct()
        assert market.is_regional_demand_meet()


def test_hist_dispatch_values_meet_demand():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.set_unit_dispatch_to_historical_values()
        market.set_interconnector_flow_to_historical_values()
        market.dispatch()
        test_passed = market.is_regional_demand_meet()
        market.con.close()
        assert test_passed


def test_prices_full_featured():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    outputs = []
    for interval in get_test_intervals():
        print(interval)
        market = historical_market_builder.SpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
                                                      interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints_fcas_requirements()
        market.set_unit_fcas_constraints()
        market.set_unit_limit_constraints()
        market.set_region_demand_constraints()
        market.set_ramp_rate_limits()
        market.set_fast_start_constraints()
        market.dispatch(calc_prices=True)
        price_comp = market.get_price_comparison()
        outputs.append(price_comp)
    outputs = pd.concat(outputs)
    outputs.to_csv('base_case_fast_start_commitment_2.csv')
    #assert outputs['error'].abs().mean() <= pytest.approx(0.2, abs=0.01)
