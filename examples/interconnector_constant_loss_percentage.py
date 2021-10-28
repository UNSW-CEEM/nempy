import pandas as pd
from nempy import markets

# The only generator is located in NSW.
unit_info = pd.DataFrame({
    'unit': ['A'],
    'region': ['NSW']  # MW
})

# Create a market instance.
market = markets.SpotMarket(unit_info=unit_info, market_regions=['NSW', 'VIC'])

# Volume of each bids.
volume_bids = pd.DataFrame({
    'unit': ['A'],
    '1': [100.0]  # MW
})

market.set_unit_volume_bids(volume_bids)

# Price of each bid.
price_bids = pd.DataFrame({
    'unit': ['A'],
    '1': [50.0]  # $/MW
})

market.set_unit_price_bids(price_bids)

# NSW has no demand but VIC has 90 MW.
demand = pd.DataFrame({
    'region': ['NSW', 'VIC'],
    'demand': [0.0, 90.0]  # MW
})

market.set_demand_constraints(demand)

# There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
interconnectors = pd.DataFrame({
    'interconnector': ['little_link'],
    'to_region': ['VIC'],
    'from_region': ['NSW'],
    'max': [100.0],
    'min': [-120.0]
})

market.set_interconnectors(interconnectors)


# The interconnector loss function. In this case losses are always 5 % of line flow.
def constant_losses(flow):
    return abs(flow) * 0.05


# The loss function on a per interconnector basis. Also details how the losses should be proportioned to the
# connected regions.
loss_functions = pd.DataFrame({
    'interconnector': ['little_link'],
    'from_region_loss_share': [0.5],  # losses are shared equally.
    'loss_function': [constant_losses]
})

# The points to linearly interpolate the loss function between. In this example the loss function is linear so only
# three points are needed, but if a non linear loss function was used then more points would be better.
interpolation_break_points = pd.DataFrame({
    'interconnector': ['little_link', 'little_link', 'little_link'],
    'loss_segment': [1, 2, 3],
    'break_point': [-120.0, 0.0, 100]
})

market.set_interconnector_losses(loss_functions, interpolation_break_points)

# Calculate dispatch.
market.dispatch()

# Return interconnector flow and losses.
print(market.get_interconnector_flows())
#   interconnector       flow    losses
# 0    little_link  92.307692  4.615385

# Understanding the interconnector flows: Losses are modelled as extra demand
# in the regions on either side of the interconnector, in this case the losses
# are split evenly between the regions. All demand in VIC must be supplied
# across the interconnector, and losses in VIC will add to the interconnector
# flow required, so we can write the equation:
#
# flow = vic_demand + flow * loss_pct_vic
# flow - flow * loss_pct_vic = vic_demand
# flow * (1 - loss_pct_vic) = vic_demand
# flow = vic_demand / (1 - loss_pct_vic)
#
# Since interconnector losses are 5% and half occur in VIC the
# loss_pct_vic = 2.5%. Thus:
#
# flow = 90 / (1 - 0.025) = 92.31
#
# Knowing the interconnector flow we can work out total losses
#
# losses = flow * loss_pct
# losses = 92.31 * 0.05 = 4.62

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit service   dispatch
# 0    A  energy  94.615385

# Understanding dispatch results: Unit A must be dispatch to
# meet supply in VIC plus all interconnector losses, therefore
# dispatch is 90.0 + 4.62 = 94.62.

# Return the price of energy in each region.
print(market.get_energy_prices())
#   region      price
# 0    NSW  50.000000
# 1    VIC  52.564103

# Understanding pricing results: The marginal cost of supply in NSW is simply
# the cost of unit A's bid. However, the marginal cost of supply in VIC also
# includes the cost of paying for interconnector losses.
