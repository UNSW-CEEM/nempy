import os
import sqlite3
import pandas as pd
from nempy import markets
from nempy.historical import inputs, historical_inputs_from_mms_db, \
    historical_inputs_from_xml, units, demand, interconnectors

con = sqlite3.connect('market_management_system.db')
market_management_system_db_interface = \
    historical_inputs_from_mms_db.DBManager(connection=con)

nemde_xml_file_cache_interface = \
    historical_inputs_from_xml.XMLCacheManager('cache_directory')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
down_load_inputs = True

if down_load_inputs:
    # inputs.build_market_management_system_database(
    #     market_management_system_db_interface,
    #     start_year=2019, start_month=1,
    #     end_year=2019, end_month=1)

    inputs.build_xml_inputs_cache(
        nemde_xml_file_cache_interface,
        start_year=2019, start_month=1,
        end_year=2019, end_month=1)

raw_inputs_loader = inputs.RawInputsLoader(
    nemde_xml_cache_manager=nemde_xml_file_cache_interface,
    market_management_system_database=market_management_system_db_interface)

# A list of intervals we want to recreate historical dispatch for.
dispatch_intervals = ['2019/01/01 00:00:00',
                      '2019/01/01 00:05:00',
                      '2019/01/01 00:10:00',
                      '2019/01/01 00:15:00',
                      '2019/01/01 00:20:00',
                      '2019/01/01 00:25:00',
                      '2019/01/01 00:30:00']

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

    regional_demand = demand_inputs.get_operational_demand()
    market.set_demand_constraints(regional_demand)

    interconnector_inputs.add_loss_model()
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
    outputs.append(prices)

con.close()
print(pd.concat(outputs))
#    region      price                 time
# 0    NSW1  61.114147  2020/01/02 15:05:00
# 1    QLD1  58.130015  2020/01/02 15:05:00
# 2     SA1  72.675411  2020/01/02 15:05:00
# 3    TAS1  73.013327  2020/01/02 15:05:00
# 4    VIC1  68.778493  2020/01/02 15:05:00
# ..    ...        ...                  ...
# 0    NSW1  54.630861  2020/01/02 21:00:00
# 1    QLD1  55.885854  2020/01/02 21:00:00
# 2     SA1  53.038412  2020/01/02 21:00:00
# 3    TAS1  61.537939  2020/01/02 21:00:00
# 4    VIC1  57.040000  2020/01/02 21:00:00
#
# [360 rows x 3 columns]
