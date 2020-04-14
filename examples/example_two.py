import pandas as pd
from nempy import RealTimeMarket

# Volume of each bid, number of bands must equal number of bands in price_bids.
volume_bids = pd.DataFrame({
    'unit': ['A', 'B'],
    '1': [20, 50],  # MW
    '2': [20, 30],  # MW
    '3': [5, 10]  # More bid bands could be added.
})

# Price of each bid, bids must be monotonically increasing.
price_bids = pd.DataFrame({
    'unit': ['A', 'B'],
    '1': [50, 50],  # $/MW
    '2': [60, 55],  # $/MW
    '3': [100, 80]  # . . .
})

# Factors limiting unit output
unit_limits = pd.DataFrame({
    'unit': ['A', 'B'],
    'initial_output': [55, 90],  # MW
    'capacity': [55, 90],  # MW
    'ramp_up_rate': [1000, 1500],  # MW/h
    'ramp_down_rate': [1000, 1500]  # MW/h
})

# Other unit properties
unit_info = pd.DataFrame({
    'unit': ['A', 'B'],
    'region': ['NSW', 'NSW'],  # MW
    'loss_factor': [0.9, 0.95]  # MW/h
})

demand = pd.DataFrame({
    'region': ['NSW'],
    'demand': [100]  # MW
})

simple_market = RealTimeMarket(unit_info=unit_info, dispatch_interval=5)
simple_market.set_unit_energy_volume_bids(volume_bids)
simple_market.set_unit_capacity_constraints(unit_limits)
simple_market.set_unit_energy_price_bids(price_bids)
simple_market.set_demand_constraints(demand)
simple_market.dispatch()

