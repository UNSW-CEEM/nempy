import sqlite3
from datetime import datetime, timedelta
import random
import pandas as pd
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, constraints, rhs_calculator
from nempy.help_functions.helper_functions import update_rhs_values
from nempy.historical_inputs.aemo_to_nempy_name_mapping import map_aemo_column_values_to_nempy_name

con = sqlite3.connect('D:/nempy_oct_2023/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('D:/nempy_oct_2023/xml_cache')

download_inputs = False

if download_inputs:
    # This requires approximately 4 GB of storage.
    mms_db_manager.populate(
        start_year=2023, start_month=10,
        end_year=2023, end_month=10,
        )

    # This requires approximately 50 GB of storage.
    xml_cache_manager.populate_by_day(
        start_year=2023, start_month=10, start_day=9,
        end_year=2023, end_month=10, end_day=31,
        )

raw_inputs_loader = loaders.RawInputsLoader(
    nemde_xml_cache_manager=xml_cache_manager,
    market_management_system_database=mms_db_manager)


# A list of intervals we want to recreate historical dispatch for.
def get_test_intervals(number=100):
    start_time = datetime(year=2023, month=10, day=9, hour=13, minute=5)  # R1/L1 go live datetime.
    end_time = datetime(year=2023, month=10, day=31, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


# List for saving outputs to.
outputs = []
fcas_outputs = []

c = 0
# Create and dispatch the spot market for each dispatch interval.
for interval in get_test_intervals(number=100):
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
    market.set_interconnector_losses(loss_functions,
                                     interpolation_break_points)

    # Calculate rhs constraint values that depend on the basslink frequency controller from scratch so there is
    # consistency between the basslink switch runs.
    # Find the constraints that need to be calculated because they depend on the frequency controller status.
    constraints_to_update = (
        rhs_calculation_engine.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
    initial_bl_freq_onstatus = rhs_calculation_engine.scada_data['W']['BL_FREQ_ONSTATUS'][0]['@Value']
    # Calculate new rhs values for the constraints that need updating.
    new_rhs_values = rhs_calculation_engine.compute_constraint_rhs(constraints_to_update)

    # Add generic constraints and FCAS market constraints.
    fcas_requirements = constraint_inputs.get_fcas_requirements()
    fcas_requirements = update_rhs_values(fcas_requirements, new_rhs_values)
    market.set_fcas_requirements_constraints(fcas_requirements)
    violation_costs = constraint_inputs.get_violation_costs()
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    generic_rhs = constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()
    generic_rhs = update_rhs_values(generic_rhs, new_rhs_values)
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
    set_ramp_rates(run_type='fast_start_second_run')
    set_joint_ramping_constraints(run_type='fast_start_second_run')
    market.set_fast_start_constraints(fast_start_profiles)
    if 'fast_start' in market._constraints_rhs_and_type.keys():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    # First run of Basslink switch runs
    market.dispatch()  # First dispatch without allowing over constrained dispatch re-run to get objective function.

    objective_value_run_one = market.objective_value
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=16600.0,
                        fcas_market_ceiling_price=1000.0)
    prices_run_one = market.get_energy_prices()  # If this is the lowest cost run these will be the market prices.
    fcas_prices_run_one = market.get_fcas_prices()

    # Re-run dispatch with Basslink Frequency controller off.
    # Set frequency controller to off in rhs calculations
    rhs_calculation_engine.update_spd_id_value('BL_FREQ_ONSTATUS', 'W', '0')
    new_bl_freq_onstatus = rhs_calculation_engine.scada_data['W']['BL_FREQ_ONSTATUS'][0]['@Value']
    # Find the constraints that need to be updated because they depend on the frequency controller status.
    constraints_to_update = (
        rhs_calculation_engine.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
    # Calculate new rhs values for the constraints that need updating.
    new_rhs_values = rhs_calculation_engine.compute_constraint_rhs(constraints_to_update)
    # Update the constraints in the market.
    fcas_requirements = update_rhs_values(fcas_requirements, new_rhs_values)
    violation_costs = constraint_inputs.get_violation_costs()
    market.set_fcas_requirements_constraints(fcas_requirements)
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    generic_rhs = update_rhs_values(generic_rhs, new_rhs_values)
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
    if 'fast_start' in market.get_constraint_set_names():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    market.dispatch()  # First dispatch without allowing over constrained dispatch re-run to get objective function.
    objective_value_run_two = market.objective_value
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=16600.0,
                        fcas_market_ceiling_price=1000.0)
    prices_run_two = market.get_energy_prices()  # If this is the lowest cost run these will be the market prices.
    fcas_prices_run_two = market.get_fcas_prices()

    prices_run_one['time'] = interval
    prices_run_two['time'] = interval
    fcas_prices_run_one['time'] = interval
    fcas_prices_run_two['time'] = interval


    # Getting historical prices for comparison. Note, ROP price, which is
    # the regional reference node price before the application of any
    # price scaling by AEMO, is used for comparison.
    historical_prices = mms_db_manager.DISPATCHPRICE.get_data(interval)

    # The prices from the run with the lowest objective function value are used.
    if objective_value_run_one < objective_value_run_two:
        prices = prices_run_one
        fcas_prices = fcas_prices_run_one
    else:
        prices = prices_run_two
        fcas_prices = fcas_prices_run_two

    prices['time'] = interval
    prices = pd.merge(prices, historical_prices,
                      left_on=['time', 'region'],
                      right_on=['SETTLEMENTDATE', 'REGIONID'])

    historical_prices = xml_cache_manager.get_service_prices()
    historical_prices = map_aemo_column_values_to_nempy_name(historical_prices, 'service')
    historical_prices = historical_prices.rename(columns={'price': 'ROP'})
    historical_prices['ROP'] = historical_prices['ROP'].astype(float)
    fcas_prices['time'] = interval
    fcas_prices = pd.merge(fcas_prices, historical_prices, on=['region', 'service'])

    outputs.append(prices)
    fcas_outputs.append(fcas_prices)

con.close()

outputs = pd.concat(outputs)
fcas_outputs = pd.concat(fcas_outputs)


outputs['error'] = outputs['price'] - outputs['ROP']
outputs.to_csv('benchmark_2023_ffr_random_intervals.csv')

fcas_outputs['error'] = fcas_outputs['price'] - fcas_outputs['ROP']
fcas_outputs.to_csv('benchmark_2023_ffr_fcas_random_intervals.csv')

print('\n Summary of error in energy price across all regions. \n'
      'Comparison is against ROP, the region price prior to \n'
      'any post dispatch adjustments, scaling, capping etc.')
print('Mean price error: {}'.format(outputs['error'].mean()))
print('Median price error: {}'.format(outputs['error'].quantile(0.5)))
print('5% percentile price error: {}'.format(outputs['error'].quantile(0.05)))
print('95% percentile price error: {}'.format(outputs['error'].quantile(0.95)))


print('\n Summary of error in FCAS price across all regions. \n'
      'Comparison is against ROP, the region price prior to \n'
      'any post dispatch adjustments, scaling, capping etc.')
print('Mean price error: {}'.format(fcas_outputs['error'].mean()))
print('Median price error: {}'.format(fcas_outputs['error'].quantile(0.5)))
print('5% percentile price error: {}'.format(fcas_outputs['error'].quantile(0.05)))
print('95% percentile price error: {}'.format(fcas_outputs['error'].quantile(0.95)))


# Summary of error in energy price across all regions.
# Comparison is against ROP, the region price prior to
# any post dispatch adjustments, scaling, capping etc.
# Mean price error: 0.17175635564022043
# Median price error: 0.0
# 5% percentile price error: -0.050982130191409604
# 95% percentile price error: 0.35580904152894177
