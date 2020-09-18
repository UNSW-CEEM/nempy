# Notice: this script downloads large volumes of historical market data from AEMO's nemweb portal.

import sqlite3
import pandas as pd
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors

con = sqlite3.connect('market_management_system.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('cache_directory')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = True

if download_inputs:
    # This requires approximately 5 GB of storage.
    mms_db_manager.populate(start_year=2019, start_month=1,
                            end_year=2019, end_month=1)

    # This requires approximately 60 GB of storage.
    xml_cache_manager.populate(start_year=2019, start_month=1,
                               end_year=2019, end_month=1)

raw_inputs_loader = loaders.RawInputsLoader(
    nemde_xml_cache_manager=xml_cache_manager,
    market_management_system_database=mms_db_manager)

# A list of intervals we want to recreate historical dispatch for.
dispatch_intervals = ['2019/01/01 12:00:00',
                      '2019/01/01 12:05:00',
                      '2019/01/01 12:10:00',
                      '2019/01/01 12:15:00',
                      '2019/01/01 12:20:00',
                      '2019/01/01 12:25:00',
                      '2019/01/01 12:30:00']

# List for saving outputs to.
outputs = []

# Create and dispatch the spot market for each dispatch interval.
for interval in dispatch_intervals:
    raw_inputs_loader.set_interval(interval)
    unit_inputs = units.UnitData(raw_inputs_loader)
    demand_inputs = demand.DemandData(raw_inputs_loader)
    interconnector_inputs = \
        interconnectors.InterconnectorData(raw_inputs_loader)

    unit_info = unit_inputs.get_unit_info()
    market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1',
                                                'SA1', 'TAS1'],
                                unit_info=unit_info)

    volume_bids, price_bids = unit_inputs.get_processed_bids()
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    unit_bid_limit = unit_inputs.get_unit_bid_availability()
    market.set_unit_bid_capacity_constraints(unit_bid_limit)

    unit_uigf_limit = unit_inputs.get_unit_uigf_limits()
    market.set_unconstrained_intermitent_generation_forecast_constraint(
        unit_uigf_limit)

    regional_demand = demand_inputs.get_operational_demand()
    market.set_demand_constraints(regional_demand)

    interconnectors_definitions = \
        interconnector_inputs.get_interconnector_definitions()
    loss_functions, interpolation_break_points = \
        interconnector_inputs.get_interconnector_loss_model()
    market.set_interconnectors(interconnectors_definitions)
    market.set_interconnector_losses(loss_functions,
                                     interpolation_break_points)
    market.dispatch()

    # Save prices from this interval
    prices = market.get_energy_prices()
    prices['time'] = interval
    outputs.append(prices.loc[:, ['time', 'region', 'price']])

con.close()
print(pd.concat(outputs))
#                   time region      price
# 0  2019/01/01 12:00:00   NSW1  91.857666
# 1  2019/01/01 12:00:00   QLD1  76.180429
# 2  2019/01/01 12:00:00    SA1  85.126914
# 3  2019/01/01 12:00:00   TAS1  85.948523
# 4  2019/01/01 12:00:00   VIC1  83.250703
# 0  2019/01/01 12:05:00   NSW1  88.357224
# 1  2019/01/01 12:05:00   QLD1  72.255334
# 2  2019/01/01 12:05:00    SA1  82.417720
# 3  2019/01/01 12:05:00   TAS1  83.451561
# 4  2019/01/01 12:05:00   VIC1  80.621103
# 0  2019/01/01 12:10:00   NSW1  91.857666
# 1  2019/01/01 12:10:00   QLD1  75.665675
# 2  2019/01/01 12:10:00    SA1  85.680310
# 3  2019/01/01 12:10:00   TAS1  86.715499
# 4  2019/01/01 12:10:00   VIC1  83.774337
# 0  2019/01/01 12:15:00   NSW1  88.343034
# 1  2019/01/01 12:15:00   QLD1  71.746786
# 2  2019/01/01 12:15:00    SA1  82.379539
# 3  2019/01/01 12:15:00   TAS1  83.451561
# 4  2019/01/01 12:15:00   VIC1  80.621103
# 0  2019/01/01 12:20:00   NSW1  91.864122
# 1  2019/01/01 12:20:00   QLD1  75.052319
# 2  2019/01/01 12:20:00    SA1  85.722028
# 3  2019/01/01 12:20:00   TAS1  86.576848
# 4  2019/01/01 12:20:00   VIC1  83.859306
# 0  2019/01/01 12:25:00   NSW1  91.864122
# 1  2019/01/01 12:25:00   QLD1  75.696247
# 2  2019/01/01 12:25:00    SA1  85.746024
# 3  2019/01/01 12:25:00   TAS1  86.613642
# 4  2019/01/01 12:25:00   VIC1  83.894945
# 0  2019/01/01 12:30:00   NSW1  91.870167
# 1  2019/01/01 12:30:00   QLD1  75.188735
# 2  2019/01/01 12:30:00    SA1  85.694071
# 3  2019/01/01 12:30:00   TAS1  86.560602
# 4  2019/01/01 12:30:00   VIC1  83.843570
