import pandas as pd
from nempy import markets
from nempy.historical_inputs import interconnectors as interconnector_inputs


# The only generator is located in NSW.
unit_info = pd.DataFrame({
    'unit': ['A'],
    'region': ['NSW']  # MW
})

# Create a market instance.
market = markets.SpotMarket(unit_info=unit_info,
                            market_regions=['NSW', 'VIC'])

# Volume of each bids.
volume_bids = pd.DataFrame({
    'unit': ['A'],
    '1': [1000.0]  # MW
})

market.set_unit_volume_bids(volume_bids)

# Price of each bid.
price_bids = pd.DataFrame({
    'unit': ['A'],
    '1': [50.0]  # $/MW
})

market.set_unit_price_bids(price_bids)

# NSW has no demand but VIC has 800 MW.
demand = pd.DataFrame({
    'region': ['NSW', 'VIC'],
    'demand': [0.0, 800.0],  # MW
    'loss_function_demand': [0.0, 800.0]  # MW
})

market.set_demand_constraints(demand.loc[:, ['region', 'demand']])

# There is one interconnector between NSW and VIC.
# Its nominal direction is towards VIC.
interconnectors = pd.DataFrame({
    'interconnector': ['VIC1-NSW1'],
    'to_region': ['VIC'],
    'from_region': ['NSW'],
    'max': [1000.0],
    'min': [-1200.0]
})

market.set_interconnectors(interconnectors)

# Create a demand dependent loss function.
# Specify the demand dependency
demand_coefficients = pd.DataFrame({
    'interconnector': ['VIC1-NSW1', 'VIC1-NSW1'],
    'region': ['NSW1', 'VIC1'],
    'demand_coefficient': [0.000021734, -0.000031523]})

# Specify the loss function constant and flow coefficient.
interconnector_coefficients = pd.DataFrame({
    'interconnector': ['VIC1-NSW1'],
    'loss_constant': [1.0657],
    'flow_coefficient': [0.00017027],
    'from_region_loss_share': [0.5]})

# Create loss functions on per interconnector basis.
loss_functions = interconnector_inputs.create_loss_functions(
    interconnector_coefficients, demand_coefficients,
    demand.loc[:, ['region', 'loss_function_demand']])

# The points to linearly interpolate the loss function between.
interpolation_break_points = pd.DataFrame({
    'interconnector': 'VIC1-NSW1',
    'loss_segment': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    'break_point': [-1200.0, -1000.0, -800.0, -600.0, -400.0, -200.0,
                    0.0, 200.0, 400.0, 600.0, 800.0, 1000]
})

market.set_interconnector_losses(loss_functions,
                                 interpolation_break_points)

# Calculate dispatch.
market.dispatch()

# Return interconnector flow and losses.
print(market.get_interconnector_flows())
#   interconnector        flow      losses
# 0      VIC1-NSW1  860.102737  120.205473

# Understanding the interconnector flows: In this case it is not simple to
# analytically derive and explain the interconnector flow result. The loss
# model is constructed within the underlying mixed integer linear problem
# as set of constraints and the interconnector flow and losses are
# determined as part of the problem solution. However, the loss model can
# be explained at a high level, and the results shown to be consistent. The
# first step in the interconnector model is to drive the loss function as a
# function  of regional demand, which is a pre-market model creation step, the
# mathematics is explained in
# docs/pdfs/Marginal Loss Factors for the 2020-21 Financial year.pdf. The loss
# function is then evaluated at the given break points and linearly interpolated
# between those points in the market model. So for our model the losses are
# interpolated between 800 MW and 1000 MW. We can show the losses are consistent
# with this approach:
#
# Losses at a flow of 800 MW
print(loss_functions['loss_function'].iloc[0](800))
# 107.0464
# Losses at a flow of 1000 MW
print(loss_functions['loss_function'].iloc[0](1000))
# 150.835
# Then interpolating by taking the weighted sum of the two losses based on the
# relative difference between the actual flow and the interpolation break points:
# Weighting of 800 MW break point = 1 - ((860.102737 - 800.0)/(1000 - 800))
# Weighting of 800 MW break point = 0.7
# Weighting of 1000 MW break point = 1 - ((1000 - 860.102737)/(1000 - 800))
# Weighting of 1000 MW break point = 0.3
# Weighed sum of losses = 107.0464 * 0.7 + 150.835 * 0.3 = 120.18298
#
# We can also see that the flow and loss results are consistent with the supply
# equals demand constraint, all demand in the VIC region is supplied by the
# interconnector, so the interconnector flow minus the VIC region interconnector
# losses should equal the VIC region demand. Note that the VIC region loss
# share is 50%:
# VIC region demand = interconnector flow - losses * VIC region loss share
# 800 = 860.102737 - 120.205473 * 0.5
# 800 = 800

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit service    dispatch
# 0    A  energy  920.205473

# Understanding the dispatch results: Unit A is the only generator and it must
# be dispatched to meet demand plus losses:
# dispatch = VIC region demand + NSW region demand + losses
# dispatch = 920.205473

# Return the price of energy in each region.
print(market.get_energy_prices())
#   region      price
# 0    NSW  50.000000
# 1    VIC  62.292869

# Understanding the pricing results: Pricing in the NSW region is simply the
# marginal cost of supply from unit A. The marginal cost of supply in the
# VIC region is the cost of unit A to meet both marginal demand and the
# marginal losses on the interconnector.
