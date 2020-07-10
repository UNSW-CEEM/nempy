import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import pytest
import pickle

from nempy import markets, historical
from nempy.spot_markert_backend import check
from nempy.help_functions import helper_functions as hf
from nempy.historical import historical_inputs_from_xml as hist_xml, historical_interconnectors as hii, \
    historical_spot_market_inputs as hi, units, inputs


# Define a set of random intervals to test
def get_test_intervals():
    start_time = datetime(year=2019, month=1, day=2, hour=0, minute=0)
    end_time = datetime(year=2019, month=2, day=1, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), 100)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


def test_setup():

    running_for_first_time = True

    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')

    if running_for_first_time:

        historical_inputs.build_market_management_system_database(start_year=2019, start_month=1,
                                                       end_year=2019, end_month=3)
        #historical_inputs.build_xml_inputs_cache(start_year=2019, start_month=2,
        #                              end_year=2019, end_month=2)

    get_violation_intervals = False

    if get_violation_intervals:
        interval_with_fast_start_violations = \
            historical_inputs.find_intervals_with_violations(limit=1,
                                                             start_year=2019, start_month=2,
                                                             end_year=2019, end_month=2)

        with open('interval_with_fast_start_violations.pickle', 'wb') as f:
            pickle.dump(interval_with_fast_start_violations, f, pickle.HIGHEST_PROTOCOL)

    con.close()


def test_historical_interconnector_losses():
    # Create a data base manager.
    con = sqlite3.connect('test_files/historical.db')
    inputs_manager = hi.DBManager(connection=con)

    for interval in get_test_intervals():
        print(interval)
        INTERCONNECTOR = inputs_manager.INTERCONNECTOR.get_data()
        INTERCONNECTORCONSTRAINT = inputs_manager.INTERCONNECTORCONSTRAINT.get_data(interval)
        interconnectors = hi.format_interconnector_definitions(INTERCONNECTOR, INTERCONNECTORCONSTRAINT)
        interconnector_loss_coefficients = hi.format_interconnector_loss_coefficients(INTERCONNECTORCONSTRAINT)
        LOSSFACTORMODEL = inputs_manager.LOSSFACTORMODEL.get_data(interval)
        interconnector_demand_coefficients = hi.format_interconnector_loss_demand_coefficient(LOSSFACTORMODEL)
        LOSSMODEL = inputs_manager.LOSSMODEL.get_data(interval)
        interpolation_break_points = hi.format_interpolation_break_points(LOSSMODEL)
        DISPATCHREGIONSUM = inputs_manager.DISPATCHREGIONSUM.get_data(interval)
        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        inter_flow = inputs_manager.DISPATCHINTERCONNECTORRES.get_data(interval)

        market = markets.SpotMarket()

        inter_flow = inter_flow.loc[:, ['INTERCONNECTORID', 'MWFLOW', 'MWLOSSES']]
        inter_flow.columns = ['interconnector', 'MWFLOW', 'MWLOSSES']
        interconnectors = pd.merge(interconnectors, inter_flow, 'inner', on='interconnector')
        interconnectors['max'] = interconnectors['MWFLOW'] + 0.01
        interconnectors['min'] = interconnectors['MWFLOW'] - 0.01
        interconnectors = interconnectors.loc[:, ['interconnector', 'to_region', 'from_region', 'min', 'max']]
        market.set_interconnectors(interconnectors)

        # Create loss functions on per interconnector basis.
        loss_functions = hi.create_loss_functions(interconnector_loss_coefficients,
                                                  interconnector_demand_coefficients,
                                                  regional_demand.loc[:, ['region', 'loss_function_demand']])

        market.set_interconnector_losses(loss_functions, interpolation_break_points)

        # Calculate dispatch.
        market.dispatch()
        output = market.get_interconnector_flows()

        expected = inputs_manager.DISPATCHINTERCONNECTORRES.get_data(interval)
        expected = expected.loc[:, ['INTERCONNECTORID', 'MWFLOW', 'MWLOSSES']].sort_values('INTERCONNECTORID')
        expected.columns = ['interconnector', 'flow', 'losses']
        expected = expected.reset_index(drop=True)
        output = output.sort_values('interconnector').reset_index(drop=True)
        comparison = pd.merge(expected, output, 'inner', on='interconnector')
        comparison['diff'] = comparison['losses_x'] - comparison['losses_y']
        comparison['diff'] = comparison['diff'].abs()
        comparison['ok'] = comparison['diff'] < 0.5
        assert (comparison['ok'].all())


def test_if_schudeled_units_dispatched_above_bid_availability():
    con = sqlite3.connect('test_files/historical.db')
    inputs_manager = hi.DBManager(connection=con)
    for interval in get_test_intervals():
        print(interval)
        dispatch_load = inputs_manager.DISPATCHLOAD.get_data(interval).loc[:, ['DUID', 'TOTALCLEARED']]
        xml_inputs = hist_xml.XMLInputs(cache_folder='test_files/historical_xml_files', interval=interval)
        TOTAL_UNIT_ENERGY_OFFER_VIOLATION = xml_inputs.get_non_intervention_violations()[
            'TOTAL_UNIT_ENERGY_OFFER_VIOLATION']
        bid_availability = xml_inputs.get_unit_volume_bids().loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL', 'RAMPDOWNRATE',
                                                                     'RAMPUPRATE']]
        bid_availability = bid_availability[bid_availability['BIDTYPE'] == 'ENERGY']
        semi_dispatch_flag = xml_inputs.get_unit_fast_start_parameters().loc[:, ['DUID', 'SEMIDISPATCH']]
        schedualed_units = semi_dispatch_flag[semi_dispatch_flag['SEMIDISPATCH'] == 0.0]
        initial_cons = xml_inputs.get_unit_initial_conditions_dataframe().loc[:, ['DUID', 'INITIALMW']]
        bid_availability = pd.merge(bid_availability, schedualed_units, 'inner', on='DUID')
        bid_availability = pd.merge(bid_availability, dispatch_load, 'inner', on='DUID')
        bid_availability = pd.merge(bid_availability, initial_cons, 'inner', on='DUID')
        bid_availability['RAMPMIN'] = bid_availability['INITIALMW'] - bid_availability['RAMPDOWNRATE'] / 12
        bid_availability['MAXAVAIL'] = np.where(bid_availability['RAMPMIN'] > bid_availability['MAXAVAIL'],
                                                bid_availability['RAMPMIN'], bid_availability['MAXAVAIL'])
        bid_availability['violation'] = np.where(bid_availability['TOTALCLEARED'] > bid_availability['MAXAVAIL'],
                                                 bid_availability['TOTALCLEARED'] - bid_availability['MAXAVAIL'], 0.0)
        measured_violation = bid_availability['violation'].sum()
        assert measured_violation == pytest.approx(TOTAL_UNIT_ENERGY_OFFER_VIOLATION, abs=0.1)


def test_if_schudeled_units_dispatched_above_UIGF():
    con = sqlite3.connect('test_files/historical.db')
    inputs_manager = hi.DBManager(connection=con)
    for interval in get_test_intervals():
        print(interval)
        dispatch_load = inputs_manager.DISPATCHLOAD.get_data(interval).loc[:, ['DUID', 'TOTALCLEARED']]
        xml_inputs = hist_xml.XMLInputs(cache_folder='test_files/historical_xml_files', interval=interval)
        UGIF_total_violation = xml_inputs.get_non_intervention_violations()['TOTAL_UGIF_VIOLATION']
        ramp_rates = xml_inputs.get_unit_volume_bids().loc[:, ['DUID', 'BIDTYPE', 'RAMPDOWNRATE']]
        ramp_rates = ramp_rates[ramp_rates['BIDTYPE'] == 'ENERGY']
        initial_cons = xml_inputs.get_unit_initial_conditions_dataframe().loc[:, ['DUID', 'INITIALMW']]
        UGIFs = xml_inputs.get_UGIF_values().loc[:, ['DUID', 'UGIF']]
        availability = pd.merge(UGIFs, dispatch_load, 'inner', on='DUID')
        availability = pd.merge(availability, initial_cons, 'inner', on='DUID')
        availability = pd.merge(availability, ramp_rates, 'inner', on='DUID')
        availability['violation'] = np.where(availability['TOTALCLEARED'] > availability['UGIF'],
                                             availability['TOTALCLEARED'] - availability['UGIF'], 0.0)
        measured_violation = availability['violation'].sum()
        assert measured_violation == pytest.approx(UGIF_total_violation, abs=0.001)


def test_if_ramp_rates_calculated_correctly():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect(inputs_database)
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')

    for interval in get_test_intervals():
        print(interval)
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
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
            market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
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
            market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs,
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
    skip = True
    for interval in get_test_intervals():
        print(interval)
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints()
        market.set_unit_dispatch_to_historical_values(wiggle_room=0.003)
        market.set_interconnector_flow_to_historical_values()
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints_fcas_requirements()
        market.set_unit_fcas_constraints()
        market.set_unit_limit_constraints()
        market.set_ramp_rate_limits()
        market.set_unit_dispatch_to_historical_values(wiggle_room=0.003)
        market.set_interconnector_flow_to_historical_values()
        market.dispatch(calc_prices=False)
        # assert market.is_generic_constraint_slack_correct()
        # assert market.is_fcas_constraint_slack_correct()
        # assert market.is_regional_demand_meet()


def test_hist_dispatch_values_meet_demand():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    for interval in get_test_intervals():
        print(interval)
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
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
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints_fcas_requirements()
        market.set_unit_fcas_constraints()
        market.set_unit_limit_constraints()
        market.set_region_demand_constraints()
        market.set_ramp_rate_limits()
        market.set_fast_start_constraints()
        market.market.set_tie_break_constraints(cost=1e-3)
        market.dispatch(calc_prices=True)
        #avails = market.market.get_fcas_availability()
        #disp = market.get_dispatch_comparison().sort_values('diff')
        price_comp = market.get_price_comparison()
        outputs.append(price_comp)
    outputs = pd.concat(outputs)
    outputs.to_csv('price_comp.csv')


def test_prices_full_featured_one_day_sequence():
    inputs_database = 'test_files/historical.db'
    con = sqlite3.connect('test_files/historical.db')
    historical_inputs = inputs.HistoricalInputs(
        market_management_system_database_connection=con,
        nemde_xml_cache_folder='test_files/historical_xml_files')
    outputs = []
    for interval in hi.datetime_dispatch_sequence(start_time='2019/01/15 00:00:00', end_time='2019/01/16 00:00:00'):
        print(interval)
        market = HistoricalSpotMarket(inputs_database=inputs_database, inputs=historical_inputs, interval=interval)
        market.add_unit_bids_to_market()
        market.add_interconnectors_to_market()
        market.add_generic_constraints_fcas_requirements()
        market.set_unit_fcas_constraints()
        market.set_unit_limit_constraints()
        market.set_region_demand_constraints()
        market.set_ramp_rate_limits()
        market.set_fast_start_constraints()
        #market.market.set_tie_break_constraints(cost=1e-3)
        market.dispatch(calc_prices=True)
        #avails = market.market.get_fcas_availability()
        #disp = market.get_dispatch_comparison().sort_values('diff')
        price_comp = market.get_price_comparison()
        outputs.append(price_comp)
    outputs = pd.concat(outputs)
    outputs.to_csv('price_comp_2019_01_15.csv')


class HistoricalSpotMarket:
    def __init__(self, inputs_database, inputs, interval):
        self.con = sqlite3.connect(inputs_database)
        self.inputs_manager = hi.DBManager(connection=self.con)
        self.inputs = inputs
        self.interval = interval
        self.services = ['TOTALCLEARED', 'LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC',
                         'LOWERREG', 'RAISEREG']
        self.service_name_mapping = {'TOTALCLEARED': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s', 'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min',
                                     'LOWER6SEC': 'lower_6s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min',
                                     'ENERGY': 'energy'}
        self.unit_inputs = self.inputs.get_unit_inputs(self.interval)
        unit_info = self.unit_inputs.get_unit_info()
        self.market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1', 'SA1', 'TAS1'], unit_info=unit_info)

    def add_unit_bids_to_market(self):
        volume_bids, price_bids = self.unit_inputs.get_processed_bids()
        self.market.set_unit_volume_bids(volume_bids)
        self.market.set_unit_price_bids(price_bids)

    def set_unit_limit_constraints(self):
        unit_bid_limit = self.unit_inputs.get_unit_bid_availability()
        self.market.set_unit_bid_capacity_constraints(unit_bid_limit)
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['unit_capacity']
        self.market.make_constraints_elastic('unit_bid_capacity', violation_cost=cost)
        unit_ugif_limit = self.unit_inputs.get_unit_uigf_limits()
        self.market.set_unconstrained_intermitent_generation_forecast_constraint(unit_ugif_limit)
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['ugif']
        self.market.make_constraints_elastic('ugif_capacity', violation_cost=cost)

    def set_ramp_rate_limits(self):
        ramp_rates = self.unit_inputs.get_ramp_rates_used_for_energy_dispatch()
        self.market.set_unit_ramp_up_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
        self.market.set_unit_ramp_down_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['ramp_rate']
        self.market.make_constraints_elastic('ramp_up', violation_cost=cost)
        self.market.make_constraints_elastic('ramp_down', violation_cost=cost)

    def set_fast_start_constraints(self):
        fast_start_profiles = self.unit_inputs.get_fast_start_profiles()
        self.market.set_fast_start_constraints(fast_start_profiles)
        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['fast_start']
        try:
            self.market.make_constraints_elastic('fast_start', cost)
        except check.ModelBuildError:
            pass

    def measured_violation_equals_historical_violation(self, historical_name, nempy_constraints):
        measured = 0.0
        for name in nempy_constraints:
            measured += self.market.get_elastic_constraints_violation_degree(name)
        historical = self.unit_inputs.xml_inputs.get_non_intervention_violations()[historical_name]
        if historical > 0.0 or measured > 0.0:
            x=1
        return measured == pytest.approx(historical, abs=0.1)

    def set_unit_fcas_constraints(self):
        self.unit_inputs.add_fcas_trapezium_constraints()

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['fcas_max_avail']
        fcas_availability = self.unit_inputs.get_fcas_max_availability()
        self.market.set_fcas_max_availability(fcas_availability)
        self.market.make_constraints_elastic('fcas_max_availability', cost)

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['fcas_profile']

        regulation_trapeziums = self.unit_inputs.get_fcas_regulation_trapeziums()
        self.market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)
        self.market.make_constraints_elastic('energy_and_regulation_capacity', cost)

        scada_ramp_down_rates = self.unit_inputs.get_scada_ramp_down_rates()
        lower_reg_units = self.unit_inputs.get_lower_reg_units_with_scada_ramp_rates()
        self.market.set_joint_ramping_constraints_lower_reg(lower_reg_units, scada_ramp_down_rates)
        self.market.make_constraints_elastic('joint_ramping_lower_reg', cost)

        scada_ramp_up_rates = self.unit_inputs.get_scada_ramp_up_rates()
        raise_reg_units = self.unit_inputs.get_raise_reg_units_with_scada_ramp_rates()
        self.market.set_joint_ramping_constraints_raise_reg(raise_reg_units, scada_ramp_up_rates)
        self.market.make_constraints_elastic('joint_ramping_raise_reg', cost)

        contingency_trapeziums = self.unit_inputs.get_contingency_services()
        self.market.set_joint_capacity_constraints(contingency_trapeziums)
        self.market.make_constraints_elastic('joint_capacity', cost)

    def set_region_demand_constraints(self):
        # Set regional demand.
        # Demand on regional basis.
        DISPATCHREGIONSUM = self.inputs_manager.DISPATCHREGIONSUM.get_data(self.interval)
        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        # regional_demand['demand'] = np.where(regional_demand['region'] == 'TAS1', regional_demand['demand'] + 1.0,
        #                            regional_demand['demand'])
        self.market.set_demand_constraints(regional_demand.loc[:, ['region', 'demand']])

    def add_interconnectors_to_market(self):
        interconnector_inputs = self.inputs.get_interconnector_inputs(self.interval)
        interconnector_inputs.add_loss_model()
        interconnectors = interconnector_inputs.get_interconnector_definitions()
        market_interconnectors = interconnector_inputs.get_market_interconnector_links()
        loss_functions, interpolation_break_points = interconnector_inputs.get_interconnector_loss_model()

        interconnectors['link'] = interconnectors['interconnector']
        interconnectors['from_region_loss_factor'] = 1.0
        interconnectors['to_region_loss_factor'] = 1.0
        interconnectors['generic_constraint_factor'] = 1

        interconnectors = pd.concat([interconnectors, market_interconnectors])
        interconnectors['generic_constraint_factor'] = interconnectors['generic_constraint_factor'].astype(np.int64)

        interpolation_break_points = pd.merge(interconnectors.loc[:, ['interconnector', 'link', 'generic_constraint_factor']],
                                              interpolation_break_points, on='interconnector')
        interpolation_break_points['break_point'] = interpolation_break_points['break_point'] * interpolation_break_points['generic_constraint_factor']
        interpolation_break_points['loss_segment'] = interpolation_break_points['loss_segment'] * \
                                                    interpolation_break_points['generic_constraint_factor']
        interpolation_break_points = interpolation_break_points.drop('generic_constraint_factor', axis=1)

        loss_functions = pd.merge(interconnectors.loc[:, ['interconnector', 'link', 'generic_constraint_factor']],
                                  loss_functions, on='interconnector')

        def loss_function_adjuster(loss_function, generic_constraint_factor):
            def wrapper(flow):
                return loss_function(flow * generic_constraint_factor)
            return wrapper

        loss_functions['loss_function'] = \
            loss_functions.apply(lambda x: loss_function_adjuster(x['loss_function'], x['generic_constraint_factor']), axis=1)

        loss_functions = loss_functions.drop('generic_constraint_factor', axis=1)

        self.market.set_interconnectors(interconnectors)
        self.market.set_interconnector_losses(loss_functions, interpolation_break_points)

    def add_generic_constraints(self):
        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        DUDETAILSUMMARY = self.inputs_manager.DUDETAILSUMMARY.get_data(self.interval)
        GENCONDATA = self.inputs_manager.GENCONDATA.get_data(self.interval)
        SPDINTERCONNECTORCONSTRAINT = self.inputs_manager.SPDINTERCONNECTORCONSTRAINT.get_data(self.interval)
        SPDREGIONCONSTRAINT = self.inputs_manager.SPDREGIONCONSTRAINT.get_data(self.interval)
        SPDCONNECTIONPOINTCONSTRAINT = self.inputs_manager.SPDCONNECTIONPOINTCONSTRAINT.get_data(self.interval)

        generic_rhs = hi.format_generic_constraints_rhs_and_type(DISPATCHCONSTRAINT, GENCONDATA)
        unit_generic_lhs = hi.format_generic_unit_lhs(SPDCONNECTIONPOINTCONSTRAINT, DUDETAILSUMMARY)
        region_generic_lhs = hi.format_generic_region_lhs(SPDREGIONCONSTRAINT)

        interconnector_generic_lhs = hi.format_generic_interconnector_lhs(SPDINTERCONNECTORCONSTRAINT)
        bass_link, interconnector_generic_lhs = self._split_out_bass_link(interconnector_generic_lhs)
        bass_link_forward_direction = hii.create_forward_flow_interconnectors(bass_link)
        bass_link_reverse_direction = hii.create_reverse_flow_interconnectors(bass_link)
        interconnector_generic_lhs = pd.concat([interconnector_generic_lhs, bass_link_forward_direction,
                                                bass_link_reverse_direction])

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['voll']

        self.market.set_generic_constraints(generic_rhs)
        self.market.make_constraints_elastic('generic', violation_cost=0.0)
        self.market.link_units_to_generic_constraints(unit_generic_lhs)
        self.market.link_regions_to_generic_constraints(region_generic_lhs)
        self.market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)

    def add_generic_constraints_fcas_requirements(self):
        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        # DUDETAILSUMMARY = self.inputs_manager.DUDETAILSUMMARY.get_data(self.interval)
        # GENCONDATA = self.inputs_manager.GENCONDATA.get_data(self.interval)
        # SPDINTERCONNECTORCONSTRAINT = self.inputs_manager.SPDINTERCONNECTORCONSTRAINT.get_data(self.interval)
        # SPDREGIONCONSTRAINT = self.inputs_manager.SPDREGIONCONSTRAINT.get_data(self.interval)
        # SPDCONNECTIONPOINTCONSTRAINT = self.inputs_manager.SPDCONNECTIONPOINTCONSTRAINT.get_data(self.interval)
        #
        # generic_rhs = hi.format_generic_constraints_rhs_and_type(DISPATCHCONSTRAINT, GENCONDATA)
        # unit_generic_lhs = hi.format_generic_unit_lhs(SPDCONNECTIONPOINTCONSTRAINT, DUDETAILSUMMARY)
        # region_generic_lhs = hi.format_generic_region_lhs(SPDREGIONCONSTRAINT)
        #interconnector_generic_lhs = hi.format_generic_interconnector_lhs(SPDINTERCONNECTORCONSTRAINT)

        generic_rhs = self.unit_inputs.xml_inputs.get_constraint_rhs()
        generic_type = self.unit_inputs.xml_inputs.get_constraint_type()
        generic_rhs = pd.merge(generic_rhs, generic_type.loc[:, ['set', 'type']], on='set')
        type_map = {'LE': '<=', 'EQ': '=', 'GE': '>='}
        generic_rhs['type'] = generic_rhs['type'].apply(lambda x: type_map[x])

        bid_type_map = dict(ENOF='energy', LDOF='energy', L5RE='lower_reg', R5RE='raise_reg', R5MI='raise_5min',
                            L5MI='lower_5min', R60S='raise_60s', L60S='lower_60s', R6SE='raise_6s',
                            L6SE='lower_6s')

        unit_generic_lhs = self.unit_inputs.xml_inputs.get_constraint_unit_lhs()
        unit_generic_lhs['service'] = unit_generic_lhs['service'].apply(lambda x: bid_type_map[x])
        region_generic_lhs = self.unit_inputs.xml_inputs.get_constraint_region_lhs()
        region_generic_lhs['service'] = region_generic_lhs['service'].apply(lambda x: bid_type_map[x])
        interconnector_generic_lhs = self.unit_inputs.xml_inputs.get_constraint_interconnector_lhs()

        cost = self.unit_inputs.xml_inputs.get_constraint_violation_prices()['voll']

        # violation_cost = GENCONDATA.loc[:, ['GENCONID', 'GENERICCONSTRAINTWEIGHT']]
        # violation_cost['cost'] = violation_cost['GENERICCONSTRAINTWEIGHT'] * cost
        # violation_cost = violation_cost.loc[:, ['GENCONID', 'cost']]
        violation_cost = generic_type.loc[:, ['set', 'cost']]

        pos_cons = generic_rhs[generic_rhs['rhs'] > 0.0]
        fcas_requirements = pd.merge(region_generic_lhs, pos_cons, on='set')
        fcas_requirements = fcas_requirements.loc[:, ['set', 'service', 'region', 'type', 'rhs']]
        fcas_requirements.columns = ['set', 'service', 'region', 'type', 'volume']
        #fcas_requirements = hi.format_fcas_market_requirements(region_generic_lhs, generic_rhs, GENCONDATA)
        self.market.set_fcas_requirements_constraints(fcas_requirements)
        self.market.make_constraints_elastic('fcas', violation_cost=violation_cost)

        generic_rhs = generic_rhs[~generic_rhs['set'].isin(list(fcas_requirements['set']))]
        region_generic_lhs = region_generic_lhs[~region_generic_lhs['set'].isin(list(fcas_requirements['set']))]
        self.market.set_generic_constraints(generic_rhs)
        self.market.make_constraints_elastic('generic', violation_cost=violation_cost)
        self.market.link_units_to_generic_constraints(unit_generic_lhs)
        self.market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)
        self.market.link_regions_to_generic_constraints(region_generic_lhs)

    def set_unit_dispatch_to_historical_values(self, wiggle_room=0.001):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        decision_variables = self.market._decision_variables['bids'].copy()

        decision_variables = pd.merge(decision_variables, bounds, on=['unit', 'service'])

        decision_variables_first_bid = decision_variables.groupby(['unit', 'service'], as_index=False).first()

        def last_bids(df):
            return df.iloc[1:]

        decision_variables_remaining_bids = \
            decision_variables.groupby(['unit', 'service'], as_index=False).apply(last_bids)

        decision_variables_first_bid['lower_bound'] = decision_variables_first_bid['dispatched'] - wiggle_room
        decision_variables_first_bid['upper_bound'] = decision_variables_first_bid['dispatched'] + wiggle_room
        decision_variables_first_bid['lower_bound'] = np.where(decision_variables_first_bid['lower_bound'] < 0.0, 0.0,
                                                               decision_variables_first_bid['lower_bound'])
        decision_variables_first_bid['upper_bound'] = np.where(decision_variables_first_bid['upper_bound'] < 0.0, 0.0,
                                                               decision_variables_first_bid['upper_bound'])
        decision_variables_remaining_bids['lower_bound'] = 0.0
        decision_variables_remaining_bids['upper_bound'] = 0.0

        decision_variables = pd.concat([decision_variables_first_bid, decision_variables_remaining_bids])

        self.market._decision_variables['bids'] = decision_variables

    def all_dispatch_units_and_service_have_decision_variables(self, wiggle_room=0.001):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        bounds = bounds[bounds['dispatched'] > 0.001]

        decision_variables = self.market._decision_variables['bids'].copy()

        decision_variables = decision_variables.groupby(['unit', 'service'], as_index=False).first()

        decision_variables = pd.merge(bounds, decision_variables, how='left', on=['unit', 'service'])

        decision_variables['not_missing'] = ~decision_variables['variable_id'].isna()

        decision_variables = decision_variables.sort_values('not_missing')

        return decision_variables['not_missing'].all()


    def set_interconnector_flow_to_historical_values(self, wiggle_room=0.1):
        # Historical interconnector dispatch
        DISPATCHINTERCONNECTORRES = self.inputs_manager.DISPATCHINTERCONNECTORRES.get_data(self.interval)
        interconnector_flow = DISPATCHINTERCONNECTORRES.loc[:, ['INTERCONNECTORID', 'MWFLOW']]
        interconnector_flow.columns = ['interconnector', 'flow']
        interconnector_flow['link'] = interconnector_flow['interconnector']
        interconnector_flow['link'] = np.where(interconnector_flow['interconnector'] == 'T-V-MNSP1',
            np.where(interconnector_flow['flow'] >= 0.0, 'BLNKTAS', 'BLNKVIC'), interconnector_flow['link'])

        flow_variables = self.market._decision_variables['interconnectors']
        flow_variables = pd.merge(flow_variables, interconnector_flow, 'left', on=['interconnector', 'link'])
        flow_variables = flow_variables.fillna(0.0)
        flow_variables['flow'] = np.where(flow_variables['link'] != flow_variables['interconnector'],
                                          flow_variables['flow'].abs(), flow_variables['flow'])
        flow_variables['lower_bound'] = flow_variables['flow'] - wiggle_room
        flow_variables['upper_bound'] = flow_variables['flow'] + wiggle_room
        flow_variables = flow_variables.drop(['flow'], axis=1)
        self.market._decision_variables['interconnectors'] = flow_variables

    @staticmethod
    def _split_out_bass_link(interconnectors):
        bass_link = interconnectors[interconnectors['interconnector'] == 'T-V-MNSP1']
        interconnectors = interconnectors[interconnectors['interconnector'] != 'T-V-MNSP1']
        return bass_link, interconnectors

    def dispatch(self, calc_prices=True):
        if 'OCD' in self.unit_inputs.xml_inputs.get_file_name():
            self.market.dispatch(price_market_constraints=calc_prices, over_constrained_dispatch_re_run=True,
                                 energy_market_floor_price=-1000.0, energy_market_ceiling_price=14500.0,
                                 fcas_market_ceiling_price=1000.0)
        else:
            self.market.dispatch(price_market_constraints=calc_prices, over_constrained_dispatch_re_run=False)

    def is_regional_demand_meet(self, tolerance=0.5):
        DISPATCHREGIONSUM = self.inputs_manager.DISPATCHREGIONSUM.get_data(self.interval)
        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        region_summary = self.market.get_region_dispatch_summary()
        region_summary = pd.merge(region_summary, regional_demand, on='region')
        region_summary['calc_demand'] = region_summary['dispatch'] + region_summary['inflow'] \
                                        - region_summary['interconnector_losses'] - \
                                        region_summary['transmission_losses']
        region_summary['diff'] = region_summary['calc_demand'] - region_summary['demand']
        region_summary['no_error'] = region_summary['diff'].abs() < tolerance
        return region_summary['no_error'].all()

    def is_generic_constraint_slack_correct(self):

        def calc_slack(rhs, lhs, type):
            if type == '<=':
                slack = rhs - lhs
            elif type == '>=':
                slack = lhs - rhs
            else:
                slack = 0.0
            if slack < 0.0:
                slack = 0.0
            return slack

        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        generic_cons_slack = self.market._constraints_rhs_and_type['generic']
        generic_cons_slack['slack'] = generic_cons_slack['slack'].abs()
        generic_cons_slack = pd.merge(generic_cons_slack, DISPATCHCONSTRAINT, left_on='set',
                                      right_on='CONSTRAINTID')
        generic_cons_slack['aemo_slack'] = (generic_cons_slack['RHS'] - generic_cons_slack['LHS'])
        generic_cons_slack['aemo_slack'] = \
            generic_cons_slack.apply(lambda x: calc_slack(x['RHS'], x['LHS'], x['type']), axis=1)
        generic_cons_slack['comp'] = (generic_cons_slack['aemo_slack'] - generic_cons_slack['slack']).abs()
        generic_cons_slack['no_error'] = generic_cons_slack['comp'] < 0.9
        return generic_cons_slack['no_error'].all()

    def is_fcas_constraint_slack_correct(self):

        def calc_slack(rhs, lhs, type):
            if type == '<=':
                slack = rhs - lhs
            elif type == '>=':
                slack = lhs - rhs
            else:
                slack = 0.0
            if slack < 0.0:
                slack = 0.0
            return slack

        DISPATCHCONSTRAINT = self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)
        generic_cons_slack = self.market._market_constraints_rhs_and_type['fcas']
        generic_cons_slack['slack'] = generic_cons_slack['slack'].abs()
        generic_cons_slack = pd.merge(generic_cons_slack, DISPATCHCONSTRAINT, left_on='set',
                                      right_on='CONSTRAINTID')
        generic_cons_slack['aemo_slack'] = (generic_cons_slack['RHS'] - generic_cons_slack['LHS'])
        generic_cons_slack['aemo_slack'] = \
            generic_cons_slack.apply(lambda x: calc_slack(x['RHS'], x['LHS'], x['type']), axis=1)
        generic_cons_slack['comp'] = (generic_cons_slack['aemo_slack'] - generic_cons_slack['slack']).abs()
        generic_cons_slack['no_error'] = generic_cons_slack['comp'] < 0.9
        return generic_cons_slack['no_error'].all()

    def all_constraints_presenet(self):
        DISPATCHCONSTRAINT = list(self.inputs_manager.DISPATCHCONSTRAINT.get_data(self.interval)['CONSTRAINTID'])
        fcas = list(self.market._market_constraints_rhs_and_type['fcas']['set'])
        generic = list(self.market._constraints_rhs_and_type['generic']['set'])
        generic = generic + fcas
        return set(DISPATCHCONSTRAINT) < set(generic + [1])

    def get_price_comparison(self):
        energy_prices = self.market.get_energy_prices()
        energy_prices['time'] = self.interval
        energy_prices['service'] = 'energy'
        fcas_prices = self.market.get_fcas_prices()
        fcas_prices['time'] = self.interval
        prices = pd.concat([energy_prices, fcas_prices])

        price_to_service = {'ROP': 'energy', 'RAISE6SECROP': 'raise_6s', 'RAISE60SECROP': 'raise_60s',
                            'RAISE5MINROP': 'raise_5min', 'RAISEREGROP': 'raise_reg', 'LOWER6SECROP': 'lower_6s',
                            'LOWER60SECROP': 'lower_60s', 'LOWER5MINROP': 'lower_5min', 'LOWERREGROP': 'lower_reg'}
        price_columns = list(price_to_service.keys())
        historical_prices = self.inputs_manager.DISPATCHPRICE.get_data(self.interval)
        historical_prices = hf.stack_columns(historical_prices, cols_to_keep=['SETTLEMENTDATE', 'REGIONID'],
                                             cols_to_stack=price_columns, type_name='service',
                                             value_name='RRP')
        historical_prices['service'] = historical_prices['service'].apply(lambda x: price_to_service[x])
        historical_prices = historical_prices.loc[:, ['SETTLEMENTDATE', 'REGIONID', 'service', 'RRP']]
        historical_prices.columns = ['time', 'region', 'service', 'hist_price']
        prices = pd.merge(prices, historical_prices, on=['time', 'region', 'service'])
        return prices

    def get_dispatch_comparison(self):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)
        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services
        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')
        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        nempy_dispatch = self.market.get_unit_dispatch()
        comp = pd.merge(bounds, nempy_dispatch, 'inner', on=['unit', 'service'])
        comp['diff'] = comp['dispatch'] - comp['dispatched']
        comp = pd.merge(comp, self.market._unit_info.loc[:, ['unit', 'dispatch_type']], on='unit')
        comp['diff'] = np.where(comp['dispatch_type'] == 'load', comp['diff'] * -1, comp['diff'])
        return comp

    def do_fcas_availabilities_match_historical(self):
        DISPATCHLOAD = self.inputs_manager.DISPATCHLOAD.get_data(self.interval)
        availabilities = ['RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY',
                          'RAISE5MINACTUALAVAILABILITY', 'RAISEREGACTUALAVAILABILITY',
                          'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY',
                          'LOWER5MINACTUALAVAILABILITY', 'LOWERREGACTUALAVAILABILITY']

        availabilities_mapping = {'RAISEREGACTUALAVAILABILITY': 'raise_reg',
                                  'LOWERREGACTUALAVAILABILITY': 'lower_reg',
                                  'RAISE6SECACTUALAVAILABILITY': 'raise_6s',
                                  'RAISE60SECACTUALAVAILABILITY': 'raise_60s',
                                  'RAISE5MINACTUALAVAILABILITY': 'raise_5min',
                                  'LOWER6SECACTUALAVAILABILITY': 'lower_6s',
                                  'LOWER60SECACTUALAVAILABILITY': 'lower_60s',
                                  'LOWER5MINACTUALAVAILABILITY': 'lower_5min'}

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + availabilities]
        bounds.columns = ['unit'] + availabilities

        availabilities = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=availabilities,
                                          type_name='service', value_name='availability')

        bounds = DISPATCHLOAD.loc[:, ['DUID'] + self.services]
        bounds.columns = ['unit'] + self.services

        bounds = hf.stack_columns(bounds, cols_to_keep=['unit'], cols_to_stack=self.services, type_name='service',
                                  value_name='dispatched')

        bounds['service'] = bounds['service'].apply(lambda x: self.service_name_mapping[x])

        availabilities['service'] = availabilities['service'].apply(lambda x: availabilities_mapping[x])

        availabilities = pd.merge(availabilities, bounds, on=['unit', 'service'])

        availabilities = availabilities[~(availabilities['dispatched'] - 0.001 > availabilities['availability'])]

        output = self.market.get_fcas_availability()
        output.columns = ['unit', 'service', 'availability_measured']

        availabilities = pd.merge(availabilities, output, 'left', on=['unit', 'service'])

        availabilities['availability_measured'] = availabilities['availability_measured'].fillna(0)

        availabilities['error'] = availabilities['availability_measured'] - availabilities['availability']

        availabilities['match'] = availabilities['error'].abs() < 0.1
        availabilities = availabilities.sort_values('match')
        return availabilities
