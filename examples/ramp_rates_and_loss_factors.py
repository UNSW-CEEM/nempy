import pandas as pd
from nempy import markets

# Volume of each bid, number of bands must equal number of bands in price_bids.
volume_bids = pd.DataFrame({
    'unit': ['A', 'B'],
    '1': [20.0, 50.0],  # MW
    '2': [25.0, 30.0],  # MW
    '3': [5.0, 10.0]  # More bid bands could be added.
})

# Price of each bid, bids must be monotonically increasing.
price_bids = pd.DataFrame({
    'unit': ['A', 'B'],
    '1': [40.0, 50.0],  # $/MW
    '2': [60.0, 55.0],  # $/MW
    '3': [100.0, 80.0]  # . . .
})

# Factors limiting unit output.
unit_limits = pd.DataFrame({
    'unit': ['A', 'B'],
    'initial_output': [0.0, 0.0],  # MW
    'capacity': [55.0, 90.0],  # MW
    'ramp_up_rate': [600.0, 720.0],  # MW/h
    'ramp_down_rate': [600.0, 720.0]  # MW/h
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
market.set_unit_ramp_rate_constraints(
    unit_limits.loc[:, ['unit', 'initial_output', 'ramp_up_rate', 'ramp_down_rate']])
market.set_demand_constraints(demand)

# Calculate dispatch and pricing
market.dispatch()

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit service  dispatch
# 0    A  energy      40.0
# 1    B  energy      60.0

# Understanding the dispatch results: In this example unit loss factors are
# provided, that means the cost of a bid in the dispatch optimisation is
# the bid price divided by the unit loss factor. However, loss factors do
# not effect the amount of generation a unit can supply, this is because the
# regional demand already factors in intra regional losses. The cheapest bid is
# from unit A with 20 MW at 44.44 $/MW (after loss factor), this will be
# fully dispatched. The next cheapest bid is from unit B with 50 MW at
# 52.63 $/MW, again fully dispatch. The next cheapest is unit B with 30 MW at
# 57.89 $/MW, however, unit B starts the interval at a dispatch level of 0.0 MW
# and can ramp at speed of 720 MW/hr, the default dispatch interval of Nempy
# is 5 min, so unit B can at most produce 60 MW by the end of the
# dispatch interval, this means only 10 MW of the second bid from unit B can be
# dispatched. Finally, the last bid that needs to be dispatch for supply to
# equal demand is from unit A with 25 MW at 66.67 $/MW, only 20 MW of this
# bid is needed. Adding together the bids from each unit we can see that
# unit A is dispatch for a total of 40 MW and unit B for a total of 60 MW.

# Return the price of energy in each region.
print(market.get_energy_prices())
#   region  price
# 0    NSW  66.67

# Understanding the pricing result: In this case the marginal bid, the bid
# that would be dispatch if demand increased is the second bid from unit A,
# after adjusting for the loss factor this bid has a price of 66.67 $/MW bid,
# and this bid sets the price.
