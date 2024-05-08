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
    'demand': [115.0]  # MW
})

# Create the market model
market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW'])
market.set_unit_volume_bids(volume_bids)
market.set_unit_price_bids(price_bids)
market.set_demand_constraints(demand)

# Calculate dispatch and pricing
market.dispatch()

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit service  dispatch
# 0    A  energy      35.0
# 1    B  energy      80.0

# Understanding the dispatch results: Unit A's first bid is 20 MW at 50 $/MW,
# and unit B's first bid is 50 MW at 50 $/MW, as demand for electricity is
# 115 MW both these bids are need to meet demand and so both will be fully
# dispatched. The next cheapest bid is 30 MW at 55 $/MW from unit B, combining
# this with the first two bids we get 100 MW of generation, so all of this bid
# will be dispatched. The next cheapest bid is 20 MW at 60 $/MW from unit A, by
# dispatching 15 MW of this bid we get a total of 115 MW generation, and supply
# meets demand so no more bids need to be dispatched. Adding up the dispatched
# bids from each generator we can see that unit A will be dispatch for 35 MW
# and unit B will be dispatch for 80 MW, as given by our bid stack market model.

# Return the price of energy in each region.
print(market.get_energy_prices())
#   region  price
# 0    NSW   60.0

# Understanding the pricing result: In this case the marginal bid, the bid
# that would be dispatch if demand increased is the 60 $/MW bid from unit
# B, thus this bid sets the price.

# Additional Detail: The above is a simplified interpretation
# of the pricing result, note that the price is actually taken from the
# underlying linear problem's shadow price for the supply equals demand constraint.
# The way the problem is formulated if supply sits exactly between two bids,
# for example at 120.0 MW, then the price is set by the lower rather
# than the higher bid. Note, in practical use cases if the demand is a floating point
# number this situation is unlikely to occur.
