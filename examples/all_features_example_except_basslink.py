# Notice:
# - This script downloads large volumes of historical market data (~54 GB) from AEMO's nemweb
#   portal. You can also reduce the data usage by restricting the time window given to the
#   xml_cache_manager and in the get_test_intervals function. The boolean on line 22 can
#   also be changed to prevent this happening repeatedly once the data has been downloaded.

import sqlite3
from datetime import datetime, timedelta
import random
import pandas as pd
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, constraints

con = sqlite3.connect('D:/nempy_2024_07/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('D:/nempy_2024_07/xml_cache')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = True

if download_inputs:
    # This requires approximately 4 GB of storage.
    mms_db_manager.populate(start_year=2024, start_month=7,
                            end_year=2024, end_month=7)

    # This requires approximately 50 GB of storage.
    xml_cache_manager.populate_by_day(start_year=2024, start_month=7, start_day=1,
                                      end_year=2024, end_month=8, end_day=1)

raw_inputs_loader = loaders.RawInputsLoader(
    nemde_xml_cache_manager=xml_cache_manager,
    market_management_system_database=mms_db_manager)


# A list of intervals we want to recreate historical dispatch for.
def get_test_intervals(number=100):
    start_time = datetime(year=2024, month=7, day=1, hour=0, minute=0)
    end_time = datetime(year=2024, month=8, day=1, hour=0, minute=0)
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
for interval in get_test_intervals(number=100):

    c += 1
    print(str(c) + ' ' + str(interval))
    raw_inputs_loader.set_interval(interval)
    unit_inputs = units.UnitData(raw_inputs_loader)
    interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
    constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
    demand_inputs = demand.DemandData(raw_inputs_loader)

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
    cost = constraint_inputs.get_constraint_violation_prices()['unit_capacity']
    market.set_unit_bid_capacity_constraints(unit_bid_limit, violation_cost=cost)

    # Set limits provided by the unconstrained intermittent generation
    # forecasts. Primarily for wind and solar.
    unit_uigf_limit = unit_inputs.get_unit_uigf_limits()
    cost = constraint_inputs.get_constraint_violation_prices()['uigf']
    market.set_unconstrained_intermittent_generation_forecast_constraint(
        unit_uigf_limit, violation_cost=cost
    )

    # Set unit ramp rates.
    ramp_rates = unit_inputs.get_bid_ramp_rates()
    scada_ramp_rates = unit_inputs.get_scada_ramp_rates()
    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch()
    cost = constraint_inputs.get_constraint_violation_prices()['ramp_rate']
    market.set_unit_ramp_rate_constraints(
        ramp_rates, scada_ramp_rates, fast_start_profiles,
        run_type="fast_start_first_run", violation_cost=cost
    )

    # Set unit FCAS trapezium constraints.
    unit_inputs.add_fcas_trapezium_constraints()
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_max_avail']
    fcas_availability = unit_inputs.get_fcas_max_availability()
    market.set_fcas_max_availability(fcas_availability, violation_cost=cost)
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_profile']
    regulation_trapeziums = unit_inputs.get_fcas_regulation_trapeziums()
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums,
                                                          violation_cost=cost)
    scada_ramp_rates = unit_inputs.get_scada_ramp_rates(inlude_initial_output=True)
    market.set_joint_ramping_constraints_reg(
        scada_ramp_rates, fast_start_profiles, run_type="fast_start_first_run",
        violation_cost=cost
    )
    contingency_trapeziums = unit_inputs.get_contingency_services()
    market.set_joint_capacity_constraints(contingency_trapeziums, violation_cost=cost)

    # Set interconnector definitions, limits and loss models.
    interconnectors_definitions = \
        interconnector_inputs.get_interconnector_definitions()
    loss_functions, interpolation_break_points = \
        interconnector_inputs.get_interconnector_loss_model()
    market.set_interconnectors(interconnectors_definitions)
    market.set_interconnector_losses(loss_functions,
                                     interpolation_break_points)

    # Add generic constraints and FCAS market constraints.
    fcas_requirements = constraint_inputs.get_fcas_requirements()
    cost = constraint_inputs.get_violation_costs()
    market.set_fcas_requirements_constraints(fcas_requirements, violation_cost=cost)
    generic_rhs = constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()
    market.set_generic_constraints(generic_rhs, violation_cost=cost)
    unit_generic_lhs = constraint_inputs.get_unit_lhs()
    market.link_units_to_generic_constraints(unit_generic_lhs)
    interconnector_generic_lhs = constraint_inputs.get_interconnector_lhs()
    market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)

    # Set the operational demand to be met by dispatch.
    regional_demand = demand_inputs.get_operational_demand()
    cost = constraint_inputs.get_constraint_violation_prices()['regional_demand']
    market.set_demand_constraints(regional_demand, violation_cost=cost)

    # Set tiebreak constraint to equalise dispatch of equally priced bids.
    cost = constraint_inputs.get_constraint_violation_prices()['tiebreak']
    market.set_tie_break_constraints(cost)

    # Get unit dispatch without fast start constraints and use it to
    # make fast start unit commitment decisions.
    market.dispatch()
    dispatch = market.get_unit_dispatch()

    cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
    cols = ['unit', 'end_mode', 'time_in_end_mode', 'mode_two_length',
            'mode_four_length', 'min_loading']
    fsp = fast_start_profiles.loc[:, cols]
    market.set_fast_start_constraints(fsp, violation_cost=cost)

    ramp_rates = unit_inputs.get_bid_ramp_rates()
    scada_ramp_rates = unit_inputs.get_scada_ramp_rates()
    cols = ['unit', 'end_mode', 'time_since_end_of_mode_two', 'min_loading']
    fsp = fast_start_profiles.loc[:, cols]
    cost = constraint_inputs.get_constraint_violation_prices()['ramp_rate']
    market.set_unit_ramp_rate_constraints(
        ramp_rates, scada_ramp_rates, fsp,
        run_type="fast_start_second_run", violation_cost=cost
    )
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_profile']
    scada_ramp_rates = unit_inputs.get_scada_ramp_rates(inlude_initial_output=True)
    market.set_joint_ramping_constraints_reg(
        scada_ramp_rates, fsp, run_type="fast_start_second_run", violation_cost=cost
    )

    # If AEMO historically used the over constrained dispatch rerun
    # process then allow it to be used in dispatch. This is needed
    # because sometimes the conditions for over constrained dispatch
    # are present but the rerun process isn't used.
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=17500.0,
                        fcas_market_ceiling_price=1000.0)
    else:
        # The market price ceiling and floor are not needed here
        # because they are only used for the over constrained
        # dispatch rerun process.
        market.dispatch(allow_over_constrained_dispatch_re_run=False)

    # Save prices from this interval
    prices = market.get_energy_prices()
    prices['time'] = interval

    # Getting historical prices for comparison. Note, ROP price, which is
    # the regional reference node price before the application of any
    # price scaling by AEMO, is used for comparison.
    historical_prices = mms_db_manager.DISPATCHPRICE.get_data(interval)

    prices = pd.merge(prices, historical_prices,
                      left_on=['time', 'region'],
                      right_on=['SETTLEMENTDATE', 'REGIONID'])

    outputs.append(
        prices.loc[:, ['time', 'region', 'price', 'ROP']])

con.close()

outputs = pd.concat(outputs)

outputs['error'] = outputs['price'] - outputs['ROP']

outputs.to_csv("bdu_prices.csv")

print('\n Summary of error in energy price volume weighted average price. \n'
      'Comparison is against ROP, the price prior to \n'
      'any post dispatch adjustments, scaling, capping etc.')
print('Mean price error: {}'.format(outputs['error'].mean()))
print('Median price error: {}'.format(outputs['error'].quantile(0.5)))
print('5% percentile price error: {}'.format(outputs['error'].quantile(0.05)))
print('95% percentile price error: {}'.format(outputs['error'].quantile(0.95)))

# Summary of error in energy price volume weighted average price.
# Comparison is against ROP, the price prior to
# any post dispatch adjustments, scaling, capping etc.
# Mean price error: 0.13818277307210394
# Median price error: 0.0
# 5% percentile price error: -0.13335830516772942
# 95% percentile price error: 0.013533539900288811
