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

# Other unit properties
unit_info = pd.DataFrame({
    'unit': ['A', 'B'],
    'region': ['NSW', 'NSW'],  # MW
})

# The demand in the region\s being dispatched
demand = pd.DataFrame({
    'region': ['NSW'],
    'demand': [120.0]  # MW
})

# Create the market model
simple_market = markets.Spot(unit_info=unit_info)
simple_market.set_unit_energy_volume_bids(volume_bids)
simple_market.set_unit_energy_price_bids(price_bids)
simple_market.set_demand_constraints(demand)

# Calculate dispatch and pricing
simple_market.dispatch()

# Return the total dispatch of each unit in MW.
simple_market.get_energy_dispatch()

# returns pandas DataFrame
#   unit  dispatch
# 0    A      40.0
# 1    B      80.0

# Return the price of energy in each region.
simple_market.get_energy_prices()

# returns pandas DataFrame
#   region  price
# 0    NSW   60.0