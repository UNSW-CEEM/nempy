# Notice:
# - This script downloads large volumes of historical market data from AEMO's nemweb
#   portal. The boolean on line 20 can be changed to prevent this happening repeatedly
#   once the data has been downloaded.
# - This example also requires plotly >= 5.3.1, < 6.0.0 and kaleido == 0.2.1
#   pip install plotly==5.3.1 and pip install kaleido==0.2.1

import sqlite3
import pandas as pd
import plotly.graph_objects as go
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors

con = sqlite3.connect('historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = False

if download_inputs:
    # This requires approximately 5 GB of storage.
    mms_db_manager.populate(start_year=2019, start_month=1,
                            end_year=2019, end_month=1)

    # This requires approximately 3.5 GB of storage.
    xml_cache_manager.populate_by_day(start_year=2019, start_month=1, start_day=1,
                                      end_year=2019, end_month=1, end_day=1)

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
    market.set_unconstrained_intermittent_generation_forecast_constraint(
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

# Plot results for QLD market region.
qld_prices = outputs[outputs['region'] == 'QLD1']

fig = go.Figure()
fig.add_trace(go.Scatter(x=qld_prices['time'], y=qld_prices['price'], name='Nempy price', mode='markers',
                         marker_size=12, marker_symbol='circle'))
fig.add_trace(go.Scatter(x=qld_prices['time'], y=qld_prices['ROP'], name='Historical price', mode='markers',
                         marker_size=8))
fig.update_xaxes(title="Time")
fig.update_yaxes(title="Price ($/MWh)")
fig.update_layout(yaxis_range=[0.0, 100.0], title="QLD Region Price")
fig.write_image('energy_market_only_qld_prices.png')
fig.show()

print(outputs)
#                   time region      price       ROP
# 0  2019/01/01 12:00:00   NSW1  91.857666  91.87000
# 1  2019/01/01 12:00:00   QLD1  76.180429  76.19066
# 2  2019/01/01 12:00:00    SA1  85.126914  86.89938
# 3  2019/01/01 12:00:00   TAS1  85.948523  89.70523
# 4  2019/01/01 12:00:00   VIC1  83.250703  84.98410
# 0  2019/01/01 12:05:00   NSW1  88.357224  91.87000
# 1  2019/01/01 12:05:00   QLD1  72.255334  64.99000
# 2  2019/01/01 12:05:00    SA1  82.417720  87.46213
# 3  2019/01/01 12:05:00   TAS1  83.451561  90.08096
# 4  2019/01/01 12:05:00   VIC1  80.621103  85.55555
# 0  2019/01/01 12:10:00   NSW1  91.857666  91.87000
# 1  2019/01/01 12:10:00   QLD1  75.665675  64.99000
# 2  2019/01/01 12:10:00    SA1  85.680310  86.86809
# 3  2019/01/01 12:10:00   TAS1  86.715499  89.87995
# 4  2019/01/01 12:10:00   VIC1  83.774337  84.93569
# 0  2019/01/01 12:15:00   NSW1  88.343034  91.87000
# 1  2019/01/01 12:15:00   QLD1  71.746786  64.78003
# 2  2019/01/01 12:15:00    SA1  82.379539  86.84407
# 3  2019/01/01 12:15:00   TAS1  83.451561  89.48585
# 4  2019/01/01 12:15:00   VIC1  80.621103  84.99034
# 0  2019/01/01 12:20:00   NSW1  91.864122  91.87000
# 1  2019/01/01 12:20:00   QLD1  75.052319  64.78003
# 2  2019/01/01 12:20:00    SA1  85.722028  87.49564
# 3  2019/01/01 12:20:00   TAS1  86.576848  90.28958
# 4  2019/01/01 12:20:00   VIC1  83.859306  85.59438
# 0  2019/01/01 12:25:00   NSW1  91.864122  91.87000
# 1  2019/01/01 12:25:00   QLD1  75.696247  64.99000
# 2  2019/01/01 12:25:00    SA1  85.746024  87.51983
# 3  2019/01/01 12:25:00   TAS1  86.613642  90.38750
# 4  2019/01/01 12:25:00   VIC1  83.894945  85.63046
# 0  2019/01/01 12:30:00   NSW1  91.870167  91.87000
# 1  2019/01/01 12:30:00   QLD1  75.188735  64.99000
# 2  2019/01/01 12:30:00    SA1  85.694071  87.46153
# 3  2019/01/01 12:30:00   TAS1  86.560602  90.09919
# 4  2019/01/01 12:30:00   VIC1  83.843570  85.57286
