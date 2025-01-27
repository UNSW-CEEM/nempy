import pandas as pd
from nempy import markets


# Set options so you see all DataFrame columns in print outs.
pd.options.display.width = 0

# Volume of each bid.
volume_bids = pd.DataFrame({
    'unit': ['A', 'A', 'B', 'B', 'B'],
    'service': ['energy', 'raise_6s', 'energy',
                'raise_6s', 'raise_reg'],
    '1': [100.0, 10.0, 110.0, 15.0, 15.0],  # MW
})

print(volume_bids)
#   unit    service      1
# 0    A     energy  100.0
# 1    A   raise_6s   10.0
# 2    B     energy  110.0
# 3    B   raise_6s   15.0
# 4    B  raise_reg   15.0

# Price of each bid.
price_bids = pd.DataFrame({
    'unit': ['A', 'A', 'B', 'B', 'B'],
    'service': ['energy', 'raise_6s', 'energy',
                'raise_6s', 'raise_reg'],
    '1': [50.0, 35.0, 60.0, 20.0, 30.0],  # $/MW
})

print(price_bids)
#   unit    service     1
# 0    A     energy  50.0
# 1    A   raise_6s  35.0
# 2    B     energy  60.0
# 3    B   raise_6s  20.0
# 4    B  raise_reg  30.0

# Participant defined operational constraints on FCAS enablement.
fcas_trapeziums = pd.DataFrame({
    'unit': ['B', 'B', 'A'],
    'service': ['raise_reg', 'raise_6s', 'raise_6s'],
    'max_availability': [15.0, 15.0, 10.0],
    'enablement_min': [50.0, 50.0, 70.0],
    'low_break_point': [65.0, 65.0, 80.0],
    'high_break_point': [95.0, 95.0, 100.0],
    'enablement_max': [110.0, 110.0, 110.0]
})

print(fcas_trapeziums)
#   unit    service  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
# 0    B  raise_reg              15.0            50.0             65.0              95.0           110.0
# 1    B   raise_6s              15.0            50.0             65.0              95.0           110.0
# 2    A   raise_6s              10.0            70.0             80.0             100.0           110.0

# Unit locations.
unit_info = pd.DataFrame({
    'unit': ['A', 'B'],
    'region': ['NSW', 'NSW']
})

print(unit_info)
#   unit region
# 0    A    NSW
# 1    B    NSW

# The demand in the region\s being dispatched.
demand = pd.DataFrame({
    'region': ['NSW'],
    'demand': [195.0]  # MW
})

print(demand)
#   region  demand
# 0    NSW   195.0

# FCAS requirement in the region\s being dispatched.
fcas_requirements = pd.DataFrame({
    'set': ['nsw_regulation_requirement', 'nsw_raise_6s_requirement'],
    'region': ['NSW', 'NSW'],
    'service': ['raise_reg', 'raise_6s'],
    'volume': [10.0, 10.0]  # MW
})

print(fcas_requirements)
#                           set region    service  volume
# 0  nsw_regulation_requirement    NSW  raise_reg    10.0
# 1    nsw_raise_6s_requirement    NSW   raise_6s    10.0

# Create the market model with unit service bids.
market = markets.SpotMarket(unit_info=unit_info,
                            market_regions=['NSW'])
market.set_unit_volume_bids(volume_bids)
market.set_unit_price_bids(price_bids)

# Create constraints that enforce the top of the FCAS trapezium.
fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
market.set_fcas_max_availability(fcas_availability)

# Create constraints that enforce the lower and upper slope of the FCAS regulation
# service trapeziums.
regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_reg'].copy()
market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)

# Create constraints that enforce the lower and upper slope of the FCAS contingency
# trapezium. These constraints also scale slopes of the trapezium to ensure the
# co-dispatch of contingency and regulation services is technically feasible.
contingency_trapeziums = fcas_trapeziums[fcas_trapeziums['service'] == 'raise_6s'].copy()
market.set_joint_capacity_constraints(contingency_trapeziums)

# Set the demand for energy.
market.set_demand_constraints(demand)

# Set the required volume of FCAS services.
market.set_fcas_requirements_constraints(fcas_requirements)

# Calculate dispatch and pricing
market.dispatch()

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit    service  dispatch
# 0    A     energy     100.0
# 1    A   raise_6s       5.0
# 2    B     energy      95.0
# 3    B   raise_6s       5.0
# 4    B  raise_reg      10.0

# Understanding the dispatch results: Starting with the raise regulation
# service we can see that only unit B has bid to provide this service so
# 10 MW of its raise regulation bid must be dispatch. For the raise 6 s
# service while unit B is cheaper it's provision of 10 MW of raise
# regulation means it can only provide 5 MW of raise 6 s, so 5 MW must be
# provided by unit A. For the energy service unit A is cheaper so all
# 100 MW of its energy bid are dispatched, leaving the remaining 95 MW to
# provided by unit B. Also, note that these energy and FCAS dispatch levels are
# permitted by the FCAS trapezium constraints. Further explanation of these
# constraints are provided here: docs/pdfs/FCAS Model in NEMDE.pdf.

# Return the price of energy.
print(market.get_energy_prices())
#   region  price
# 0    NSW   75.0

#  Understanding energy price results:
#  A marginal unit of energy would have to come from unit B, as unit A is fully
#  dispatch, this would cost 60 $/MW/h. However, to turn unit B up, you would
#  need it to dispatch less raise_6s, this would cost - 20 $/MW/h, and the
#  extra FCAS would have to come from unit A, this would cost 35 $/MW/h.
#  Therefore, the marginal cost of energy is 60 - 20 + 35 = 75 $/MW/h

# Return the price of regulation FCAS.
print(market.get_fcas_prices())
#   region    service  price
# 0    NSW   raise_6s   35.0
# 1    NSW  raise_reg   45.0

# Understanding FCAS price results:
# A marginal unit of raise_reg would have to come from unit B as it is the only
# provider, this would cost 30 $/MW/h. It would also require unit B to provide
# less raise_6s, this would cost -20 $/MW/h, extra raise_6s would then be
# required from unit A costing 35 $/MW/h. This gives a total marginal cost of
# 30 - 20 + 35 = 45 $/MW/h.
#
# A marginal unit of raise_6s would be provided by unit A at a cost of 35$/MW/h/.
