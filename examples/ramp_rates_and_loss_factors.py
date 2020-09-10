import pandas as pd
from nempy import markets

# Volume of each bid, number of bands must equal number of bands in price_bids.
volume_bids = pd.DataFrame({
    'unit': ['A', 'B'],
    '1': [20.0, 50.0],  # MW
    '2': [20.0, 30.0],  # MW
    '3': [5.0, 10.0]  # More bid bands could be added.
})

# Price of each bid, bids must be monotonically increasing.
price_bids = pd.DataFrame({
    'unit': ['A', 'B'],
    '1': [50.0, 50.0],  # $/MW
    '2': [60.0, 55.0],  # $/MW
    '3': [100.0, 80.0]  # . . .
})

# Factors limiting unit output.
unit_limits = pd.DataFrame({
    'unit': ['A', 'B'],
    'initial_output': [55.0, 90.0],  # MW
    'capacity': [55.0, 90.0],  # MW
    'ramp_up_rate': [1000.0, 1500.0],  # MW/h
    'ramp_down_rate': [1000.0, 1500.0]  # MW/h
})

# Other unit properties including loss factors.
unit_info = pd.DataFrame({
    'unit': ['A', 'B'],
    'region': ['NSW', 'NSW'],  # MW
    'loss_factor': [0.9, 0.95]
})

# The demand in the region\s being dispatched.
demand = pd.DataFrame({
    'region': ['NSW'],
    'demand': [100.0]  # MW
})

# Create the market model
market = markets.SpotMarket(unit_info=unit_info,
                            market_regions=['NSW'])
market.set_unit_volume_bids(volume_bids)
market.set_unit_price_bids(price_bids)
market.set_unit_bid_capacity_constraints(
    unit_limits.loc[:, ['unit', 'capacity']])
market.set_unit_ramp_up_constraints(
    unit_limits.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
market.set_unit_ramp_down_constraints(
    unit_limits.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
market.set_demand_constraints(demand)

# Calculate dispatch and pricing
market.dispatch()

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit service  dispatch
# 0    A  energy      20.0
# 1    B  energy      80.0

# Return the price of energy in each region.
print(market.get_energy_prices())
#   region  price
# 0    NSW   57.89
