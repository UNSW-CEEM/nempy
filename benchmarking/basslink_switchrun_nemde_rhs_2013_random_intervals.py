# Notice:
# - This script downloads large volumes of historical market data (~54 GB) from AEMO's nemweb
#   portal. The boolean on line 21 can be changed to prevent this happening repeatedly
#   once the data has been downloaded.

import sqlite3
from datetime import datetime, timedelta
import random
import pandas as pd
import numpy as np
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, constraints, rhs_calculator
from nempy.help_functions.helper_functions import update_rhs_values

con = sqlite3.connect('D:/nempy_2013/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('D:/nempy_2013/xml_cache')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = False

if download_inputs:
    # This requires approximately 4 GB of storage.
    mms_db_manager.populate(start_year=2013, start_month=1,
                            end_year=2013, end_month=12)

    # This requires approximately 50 GB of storage.
    xml_cache_manager.populate_by_day(start_year=2013, start_month=1, start_day=1,
                                      end_year=2014, end_month=1, end_day=1)

raw_inputs_loader = loaders.RawInputsLoader(
    nemde_xml_cache_manager=xml_cache_manager,
    market_management_system_database=mms_db_manager)


# A list of intervals we want to recreate historical dispatch for.
def get_test_intervals(number=100):
    start_time = datetime(year=2013, month=1, day=1, hour=0, minute=0)
    end_time = datetime(year=2013, month=12, day=31, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


# List for saving outputs to.
outputs = []
c = 0
# Create and dispatch the spot market for each dispatch interval.
for interval in get_test_intervals(number=1000):
    c += 1
    print(str(c) + ' ' + str(interval))
    raw_inputs_loader.set_interval(interval)
    unit_inputs = units.UnitData(raw_inputs_loader)
    interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
    constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
    demand_inputs = demand.DemandData(raw_inputs_loader)
    rhs_calculation_engine = rhs_calculator.RHSCalc(xml_cache_manager)

    unit_info = unit_inputs.get_unit_info()
    market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1',
                                                'SA1', 'TAS1'],
                                unit_info=unit_info)

    # Set bids
    volume_bids, price_bids = unit_inputs.get_processed_bids()
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    # Set bid in capacity limits
    unit_bid_limit = unit_inputs.get_unit_bid_availability()
    market.set_unit_bid_capacity_constraints(unit_bid_limit)
    cost = constraint_inputs.get_constraint_violation_prices()['unit_capacity']
    market.make_constraints_elastic('unit_bid_capacity', violation_cost=cost)

    # Set limits provided by the unconstrained intermittent generation
    # forecasts. Primarily for wind and solar.
    unit_uigf_limit = unit_inputs.get_unit_uigf_limits()
    market.set_unconstrained_intermittent_generation_forecast_constraint(
        unit_uigf_limit)
    cost = constraint_inputs.get_constraint_violation_prices()['uigf']
    market.make_constraints_elastic('uigf_capacity', violation_cost=cost)

    # Set unit ramp rates.
    def set_ramp_rates(run_type):
        ramp_rates = unit_inputs.get_ramp_rates_used_for_energy_dispatch(run_type=run_type)
        market.set_unit_ramp_up_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
        market.set_unit_ramp_down_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
        cost = constraint_inputs.get_constraint_violation_prices()['ramp_rate']
        market.make_constraints_elastic('ramp_up', violation_cost=cost)
        market.make_constraints_elastic('ramp_down', violation_cost=cost)


    set_ramp_rates(run_type='fast_start_first_run')

    # Set unit FCAS trapezium constraints.
    unit_inputs.add_fcas_trapezium_constraints()
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_max_avail']
    fcas_availability = unit_inputs.get_fcas_max_availability()
    market.set_fcas_max_availability(fcas_availability)
    market.make_constraints_elastic('fcas_max_availability', cost)
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_profile']
    regulation_trapeziums = unit_inputs.get_fcas_regulation_trapeziums()
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)
    market.make_constraints_elastic('energy_and_regulation_capacity', cost)
    contingency_trapeziums = unit_inputs.get_contingency_services()
    market.set_joint_capacity_constraints(contingency_trapeziums)
    market.make_constraints_elastic('joint_capacity', cost)

    def set_joint_ramping_constraints(run_type):
        cost = constraint_inputs.get_constraint_violation_prices()['fcas_profile']
        scada_ramp_down_rates = unit_inputs.get_scada_ramp_down_rates_of_lower_reg_units(
            run_type=run_type)
        market.set_joint_ramping_constraints_lower_reg(scada_ramp_down_rates)
        market.make_constraints_elastic('joint_ramping_lower_reg', cost)
        scada_ramp_up_rates = unit_inputs.get_scada_ramp_up_rates_of_raise_reg_units(
            run_type=run_type)
        market.set_joint_ramping_constraints_raise_reg(scada_ramp_up_rates)
        market.make_constraints_elastic('joint_ramping_raise_reg', cost)


    set_joint_ramping_constraints(run_type="fast_start_first_run")

    # Set interconnector definitions, limits and loss models.
    interconnectors_definitions = \
        interconnector_inputs.get_interconnector_definitions()
    loss_functions, interpolation_break_points = \
        interconnector_inputs.get_interconnector_loss_model()
    market.set_interconnectors(interconnectors_definitions)
    market.set_interconnector_losses(loss_functions, interpolation_break_points)

    fcas_requirements = constraint_inputs.get_fcas_requirements()
    generic_rhs = constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()

    initial_bl_freq_on_status = rhs_calculation_engine.scada_data['W']['BL_FREQ_ONSTATUS'][0]['@Value']
    freq_on_status_best_run = (
        xml_cache_manager.xml)['NEMSPDCaseFile']['NemSpdOutputs']['PeriodSolution']['@SwitchRunBestStatus']

    # Calculate constraint RHS values that depend on Basslink frequency controller status if the best run status
    # wasn't the initial status.
    if initial_bl_freq_on_status != freq_on_status_best_run:
        # Calculate rhs constraint values that depend on the basslink frequency controller
        # Find the constraints that need to be calculated because they depend on the frequency controller status.
        constraints_to_update = (
            rhs_calculation_engine.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
        # Calculate new rhs values for the constraints that need updating.
        new_rhs_values = rhs_calculation_engine.compute_constraint_rhs(constraints_to_update)
        fcas_requirements = update_rhs_values(fcas_requirements, new_rhs_values)
        generic_rhs = update_rhs_values(generic_rhs, new_rhs_values)

    # Add generic constraints and FCAS market constraints.
    market.set_fcas_requirements_constraints(fcas_requirements)
    violation_costs = constraint_inputs.get_violation_costs()
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    market.set_generic_constraints(generic_rhs)
    market.make_constraints_elastic('generic', violation_cost=violation_costs)

    unit_generic_lhs = constraint_inputs.get_unit_lhs()
    market.link_units_to_generic_constraints(unit_generic_lhs)
    interconnector_generic_lhs = constraint_inputs.get_interconnector_lhs()
    market.link_interconnectors_to_generic_constraints(
        interconnector_generic_lhs)

    # Set the operational demand to be met by dispatch.
    regional_demand = demand_inputs.get_operational_demand()
    market.set_demand_constraints(regional_demand)

    # Set tiebreak constraint to equalise dispatch of equally priced bids.
    cost = constraint_inputs.get_constraint_violation_prices()['tiebreak']
    market.set_tie_break_constraints(cost)

    # Get unit dispatch without fast start constraints and use it to
    # make fast start unit commitment decisions.
    market.dispatch()
    dispatch = market.get_unit_dispatch()

    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
    market.set_fast_start_constraints(fast_start_profiles)

    set_ramp_rates(run_type="fast_start_second_run")
    set_joint_ramping_constraints(run_type="fast_start_second_run")

    if 'fast_start' in market._constraints_rhs_and_type.keys():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    # First run of Basslink switch runs
    market.dispatch()  # First dispatch without allowing over constrained dispatch re-run to get objective function.
    objective_value_run_one = market.objective_value
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=14500.0,
                        fcas_market_ceiling_price=1000.0)
    prices_run_one = market.get_energy_prices()  # If this is the lowest cost run these will be the market prices.
    dispatch_run_one = market.get_unit_dispatch()

    # Re-run dispatch with Basslink Frequency controller off.
    fcas_requirements = constraint_inputs.get_fcas_requirements()
    generic_rhs = constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()

    # Calculate constraint RHS values that depend on Basslink frequency controller status if the best run status
    # was the initial status.
    if initial_bl_freq_on_status == freq_on_status_best_run:
        # Set frequency controller to off in rhs calculations
        rhs_calculation_engine.update_spd_id_value('BL_FREQ_ONSTATUS', 'W', '0')
        new_bl_freq_onstatus = rhs_calculation_engine.scada_data['W']['BL_FREQ_ONSTATUS'][0]['@Value']
        # Find the constraints that need to be updated because they depend on the frequency controller status.
        constraints_to_update = (
            rhs_calculation_engine.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
        # Calculate new rhs values for the constraints that need updating.
        new_rhs_values = rhs_calculation_engine.compute_constraint_rhs(constraints_to_update)

        fcas_requirements = update_rhs_values(fcas_requirements, new_rhs_values)
        generic_rhs = update_rhs_values(generic_rhs, new_rhs_values)

    # Update the constraints in the market.
    violation_costs = constraint_inputs.get_violation_costs()
    market.set_fcas_requirements_constraints(fcas_requirements)
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    market.set_generic_constraints(generic_rhs)
    market.make_constraints_elastic('generic', violation_cost=violation_costs)

    # Reset ramp rate constraints for first run of second Basslink switchrun
    set_ramp_rates(run_type='fast_start_first_run')
    set_joint_ramping_constraints(run_type='fast_start_first_run')

    # Get unit dispatch without fast start constraints and use it to
    # make fast start unit commitment decisions.
    market.remove_fast_start_constraints()
    market.dispatch()
    dispatch = market.get_unit_dispatch()
    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
    set_ramp_rates(run_type='fast_start_second_run')
    set_joint_ramping_constraints(run_type='fast_start_second_run')
    market.set_fast_start_constraints(fast_start_profiles)
    if 'fast_start' in market._constraints_rhs_and_type.keys():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    market.dispatch()  # First dispatch without allowing over constrained dispatch re-run to get objective function.
    objective_value_run_two = market.objective_value
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=14500.0,
                        fcas_market_ceiling_price=1000.0)
    prices_run_two = market.get_energy_prices()  # If this is the lowest cost run these will be the market prices.
    dispatch_run_two = market.get_unit_dispatch()

    prices_run_one['time'] = interval
    prices_run_one = prices_run_one.rename(columns={'price': 'run_one_price'})

    prices_run_two['time'] = interval
    prices_run_two['run_two_BL_FREQ_ONSTATUS'] = new_bl_freq_onstatus
    prices_run_two['run_two_obj_value'] = objective_value_run_two
    prices_run_two = prices_run_two.rename(columns={'price': 'run_two_price'})

    # Getting historical prices for comparison. Note, ROP price, which is
    # the regional reference node price before the application of any
    # price scaling by AEMO, is used for comparison.
    historical_prices = mms_db_manager.DISPATCHPRICE.get_data(interval)

    prices_run_one = pd.merge(prices_run_one, historical_prices,
                              left_on=['time', 'region'],
                              right_on=['SETTLEMENTDATE', 'REGIONID'])

    prices_run_two = pd.merge(prices_run_two, historical_prices,
                              left_on=['time', 'region'],
                              right_on=['SETTLEMENTDATE', 'REGIONID'])

    prices_run_one = prices_run_one.loc[:, ['time', 'region', 'ROP', 'run_one_price']]
    prices_run_one = pd.merge(prices_run_one, regional_demand.loc[:, ['region', 'demand']], on='region')
    prices_run_one['ROP'] = prices_run_one['ROP'] * prices_run_one['demand']
    prices_run_one['run_one_price'] = prices_run_one['run_one_price'] * prices_run_one['demand']
    prices_run_one = prices_run_one.groupby(['time'], as_index=False).agg(
        {'ROP': 'sum', 'run_one_price': 'sum', 'demand': 'sum', })
    prices_run_one['ROP'] = prices_run_one['ROP'] / prices_run_one['demand']
    prices_run_one['run_one_price'] = prices_run_one['run_one_price'] / prices_run_one['demand']
    prices_run_one['run_one_BL_FREQ_ONSTATUS'] = initial_bl_freq_on_status
    prices_run_one['run_one_obj_value'] = objective_value_run_one

    prices_run_two = prices_run_two.loc[:, ['time', 'region', 'run_two_price', 'run_two_BL_FREQ_ONSTATUS',
                                            'run_two_obj_value']]
    prices_run_two = pd.merge(prices_run_two, regional_demand.loc[:, ['region', 'demand']], on='region')
    prices_run_two['run_two_price'] = prices_run_two['run_two_price'] * prices_run_two['demand']
    prices_run_two = prices_run_two.groupby(['time'], as_index=False).agg(
        {'run_two_price': 'sum', 'demand': 'sum', })
    prices_run_two['run_two_price'] = prices_run_two['run_two_price'] / prices_run_two['demand']
    prices_run_two['run_two_BL_FREQ_ONSTATUS'] = new_bl_freq_onstatus
    prices_run_two['run_two_obj_value'] = objective_value_run_two

    prices = pd.merge(prices_run_one, prices_run_two, on=['time'])

    if objective_value_run_one <= objective_value_run_two:
        nempy_switch_run_best_status = initial_bl_freq_on_status
        prices['nempy_price'] = prices['run_one_price']
        dispatch_run_one.to_csv('dispatch.csv')
    else:
        nempy_switch_run_best_status = new_bl_freq_onstatus
        prices['nempy_price'] = prices['run_two_price']
        dispatch_run_two.to_csv('dispatch.csv')

    prices['nempy_switch_run_best_status'] = nempy_switch_run_best_status
    prices['nemde_switch_run_best_status'] = (
        xml_cache_manager.xml)['NEMSPDCaseFile']['NemSpdOutputs']['PeriodSolution']['@SwitchRunBestStatus']

    outputs.append(prices)

con.close()

outputs = pd.concat(outputs)

outputs.to_csv('nempy_check_blsr_pricing_nemde_rhs_2013_random_intervals.csv')
