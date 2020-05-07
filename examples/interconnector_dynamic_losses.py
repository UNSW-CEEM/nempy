import pandas as pd
from nempy import markets, input_preprocessing


# Create a market instance.
simple_market = markets.Spot()

# The only generator is located in NSW.
unit_info = pd.DataFrame({
    'unit': ['A'],
    'region': ['NSW']  # MW
})

simple_market.set_unit_info(unit_info)

# Volume of each bids.
volume_bids = pd.DataFrame({
    'unit': ['A'],
    '1': [1000.0]  # MW
})

simple_market.set_unit_energy_volume_bids(volume_bids)

# Price of each bid.
price_bids = pd.DataFrame({
    'unit': ['A'],
    '1': [50.0]  # $/MW
})

simple_market.set_unit_energy_bids(price_bids)

# NSW has no demand but VIC has 800 MW.
demand = pd.DataFrame({
    'region': ['NSW', 'VIC'],
    'demand': [0.0, 800.0]  # MW
})

simple_market.set_demand_constraints(demand)

# There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
interconnectors = pd.DataFrame({
    'interconnector': ['VIC1-NSW1'],
    'to_region': ['VIC'],
    'from_region': ['NSW'],
    'max': [1000.0],
    'min': [-1200.0]
})

simple_market.set_interconnectors(interconnectors)

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
    'flow_coefficient': [0.00017027]})

# Create loss functions on per interconnector basis.
loss_functions = input_preprocessing.create_loss_functions(interconnector_coefficients, demand_coefficients, demand)

# Specify that losses are shared equally between connected regions.
loss_functions['from_region_loss_share'] = 0.5

# The points to linearly interpolate the loss function between.
interpolation_break_points = pd.DataFrame({
    'interconnector': 'VIC1-NSW1',
    'break_point': [-1200.0, -1000.0, -800.0, -600.0, -400.0, -200.0, 0.0, 200.0, 400.0, 600.0, 800.0, 1000]
})

simple_market.set_interconnector_losses(loss_functions, interpolation_break_points)

# Calculate dispatch.
simple_market.dispatch()

# Return the total dispatch of each unit in MW.
print(simple_market.get_energy_dispatch())
#   unit    dispatch
# 0    A  920.205473

# Return interconnector flow and losses.
print(simple_market.get_interconnector_flows())
#   interconnector        flow      losses
# 0      VIC1-NSW1  860.102737  120.205473

# Return the price of energy in each region.
print(simple_market.get_energy_prices())
#   region      price
# 0    NSW  50.000000
# 1    VIC  62.292869