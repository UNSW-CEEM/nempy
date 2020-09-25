# Notice: this script downloads large volumes of historical market data from AEMO's nemweb portal.

import sqlite3
import pandas as pd
import random
from datetime import datetime, timedelta

from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, \
    constraints

# The size of historical data files for a full year of 5 min dispatch
# is very large, approximately 800 GB, for this reason the data is
# stored on an external SSD.
con = sqlite3.connect('/media/nickgorman/Samsung_T5/nempy_test_files/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)
xml_cache_manager = xml_cache.XMLCacheManager('/media/nickgorman/Samsung_T5/nempy_test_files/nemde_cache')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = False

if download_inputs:
    mms_db_manager.populate(start_year=2019, start_month=1,
                            end_year=2019, end_month=12)
    xml_cache_manager.populate(start_year=2019, start_month=1,
                               end_year=2019, end_month=12)

raw_inputs_loader = loaders.RawInputsLoader(
    nemde_xml_cache_manager=xml_cache_manager,
    market_management_system_database=mms_db_manager)


# Define a function for creating a list of randomly selected dispatch
# intervals
def get_test_intervals(number):
    start_time = datetime(year=2019, month=1, day=1, hour=0, minute=0)
    end_time = datetime(year=2019, month=12, day=31, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


# List for saving outputs to.
outputs = []

# Create and dispatch the spot market for each dispatch interval.
for interval in get_test_intervals(number=1000):
    raw_inputs_loader.set_interval(interval)
    unit_inputs = units.UnitData(raw_inputs_loader)
    interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
    constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
    demand_inputs = demand.DemandData(raw_inputs_loader)

    unit_info = unit_inputs.get_unit_info()
    market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1',
                                                'SA1', 'TAS1'],
                                unit_info=unit_info)

    # By default the CBC open source solver is used, but GUROBI is
    # also supported
    market.solver_name = 'CBC'  # or could be 'GUROBI'

    # Set bids
    volume_bids, price_bids = unit_inputs.get_processed_bids()
    volume_bids = volume_bids[volume_bids['service'] == 'energy']
    price_bids = price_bids[price_bids['service'] == 'energy']
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
    market.set_unconstrained_intermitent_generation_forecast_constraint(
        unit_uigf_limit)
    cost = constraint_inputs.get_constraint_violation_prices()['uigf']
    market.make_constraints_elastic('uigf_capacity', violation_cost=cost)

    # Set unit ramp rates.
    ramp_rates = unit_inputs.get_ramp_rates_used_for_energy_dispatch()
    market.set_unit_ramp_up_constraints(
        ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
    market.set_unit_ramp_down_constraints(
        ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
    cost = constraint_inputs.get_constraint_violation_prices()['ramp_rate']
    market.make_constraints_elastic('ramp_up', violation_cost=cost)
    market.make_constraints_elastic('ramp_down', violation_cost=cost)

    # Set interconnector definitions, limits and loss models.
    interconnectors_definitions = \
        interconnector_inputs.get_interconnector_definitions()
    loss_functions, interpolation_break_points = \
        interconnector_inputs.get_interconnector_loss_model()
    market.set_interconnectors(interconnectors_definitions)
    market.set_interconnector_losses(loss_functions,
                                     interpolation_break_points)

    # Set the operational demand to be met by dispatch.
    regional_demand = demand_inputs.get_operational_demand()
    market.set_demand_constraints(regional_demand)
    market.dispatch()

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
        prices.loc[:, ['time', 'region', 'price',
                       'SETTLEMENTDATE', 'REGIONID', 'ROP']])

con.close()
outputs = pd.concat(outputs)
outputs = outputs.sort_values('ROP')
outputs = outputs.reset_index(drop=True)
outputs.to_csv('energy_price_results_2019_1000_intervals.csv')

