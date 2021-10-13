## Table of Contents
- [Introduction](https://github.com/UNSW-CEEM/nempy#introduction)
- [Motivation](https://github.com/UNSW-CEEM/nempy#motivation)
- [Documentation](https://github.com/UNSW-CEEM/nempy#documentation)
- [Community](https://github.com/UNSW-CEEM/nempy#community)
- [Installation](https://github.com/UNSW-CEEM/nempy#installation)
- [A simple example](https://github.com/UNSW-CEEM/nempy#a-simple-example)
- [A detailed example](https://github.com/UNSW-CEEM/nempy#a-detailed-example)

## Introduction
Nempy is a python package for modelling the dispatch procedure of the Australian National Electricity Market (NEM). The idea is 
that you can start simple, like in the example below, and grow the complexity of your model by adding features such as 
ramping constraints, interconnectors, FCAS markets and more. 

## Motivation and Use cases
Operational dispatch is a important part of the NEM's decision making process. It allows information from generation operators, dispatchable or flexible loads, network operators, and the market operator to be simulatenous considered on a near real time basis in order to minimize costs while ensuring secure and reliable supply, and it creates market signals for longer term decisions. The motivation for developing Nempy was to create a re-usable model of this process. Nempy has several potential use case:

1. As a reference implementation of the NEM's dispatch procedure. While the Australian Energy Market Operator (AEMO) has published several documents that describe aspects of the dispatch process, such as [FCAS Model in NEMDE](https://github.com/UNSW-CEEM/nempy/blob/master/docs/pdfs/FCAS%20Model%20in%20NEMDE.pdf), [Fast Start Unit Inflexibility Profile Model](https://github.com/UNSW-CEEM/nempy/blob/master/docs/pdfs/Fast_Start_Unit_Inflexibility_Profile_Model_October_2014.pdf), [Marginal Loss Factors](https://github.com/UNSW-CEEM/nempy/blob/master/docs/pdfs/Marginal%20Loss%20Factors%20for%20the%202020-21%20Financial%20year.pdf), [Schedule of Constraint Violation Penalty Factors](https://github.com/UNSW-CEEM/nempy/blob/master/docs/pdfs/Schedule%20of%20Constraint%20Violation%20Penalty%20factors.pdf), [Treatment of Loss Factors in the NEM](https://github.com/UNSW-CEEM/nempy/blob/master/docs/pdfs/Treatment_of_Loss_Factors_in_the_NEM.pdf), our experience developing Nempy has indicated that key implementation detail is often missing from the public available documentation. Through a process of testing various implementation options, where the documentation was not explicit, Nempy has been refined in an attempt to better reflect the actual dispatch procedure implementation. As result Nempy is a useful additional reference for analysts and modelers looking to understand the NEM's dispatch procedure.
2. As a tool for studying the dispatch process itself. Models of any energy system or electricity market are necessarily simplification, however to improve model preformance it is often desirable to add additional detail. Nempy can be used to study the relative impact of different simplifications on modelling outcomes, and thus provide guidance on how model performance could be improved by adding additional detail. For example, someone working on an agent based model of the NEM might be interested in whether improving the agents bidding algorithm or improving the representation of network constraints will have a large impact on model peformance. The two figures in [section on Nempy's accuracy](https://nempy.readthedocs.io/en/latest/intro.html#accuracy) on the readthedocs page show how Nempy's accuracy is effected by removing key market features, and such analysis could be extended to look at the realtive impact of simiplify different market features or model inputs.
3. As a building block in agent based market models. Agent based models can be used to study electricity market operation, and are particulary useful in capturing the competitive nature of these markets. In such models, agents must interact with a modelled environment, and a key part of that environment is the market dispatch process. Thus, Nempy could be useful as building block to create agent based models of the NEM, and play role in answering the various questions about market operational outcomes to which these models are commonly applied.


## Documentation
Find it on [readthedocs](https://nempy.readthedocs.io/en/latest/)

## Community
Nempy is open-source and we welcome all forms of community engagment, some more info is provided below.

### Support
You can seek suport for using Nempy using the discussion tab, checking the issues register, or by contacting Nick directly.

### Reporting issues
Issues with Nempy can be reported via the issues register, issues submissions do not need to adhere to any particular format.

### Contributing
Contributions via pull requests are welcome. Contributions should; follow the PEP8 style guide (with exception of line length up to 120 rather than 80), ensure that all existing automated tests continue to pass (unless you are explicitly changing intended behavour, please highlight this in your pull request description), implement automated tests for new features, and provided doc strings for public interfaces. 

### Get in touch
This project is being lead by Nick Gorman, a PhD candidate at the University for New South Wales and the Collaboration 
on Energy and Environmental Markets (CEEM). As part the project we hope to engage with and support prospective users of
the software. Feel welcome to get in touch if you have any questions, want to provide feed back, have a feature request,
are interested in collaborating or just want to discuss this project. You can contact Nick via n.gorman<@>unsw.edu.au.      

## Installation
Installing nempy to use in your project is easy.

`pip install nempy`

To install for development purposes, such as adding new features. Download the source code, unzip, cd into the directory, then install.

`pip install e .[dev]`

Then the test suite can be run using.

`python -m pytest`

## A simple example
```python
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
market = markets.SpotMarket(unit_info=unit_info, 
                            market_regions=['NSW'])
market.set_unit_volume_bids(volume_bids)
market.set_unit_price_bids(price_bids)
market.set_demand_constraints(demand)

# Calculate dispatch and pricing
market.dispatch()

# Return the total dispatch of each unit in MW.
print(market.get_unit_dispatch())
#   unit service  dispatch
# 0    A  energy      40.0
# 1    B  energy      80.0

# Return the price of energy in each region.
print(market.get_energy_prices())
#   region  price
# 0    NSW   60.0
```

## A detailed example
The example demonstrates the broad range of market features that can be implemented with nempy and the use of auxiliary 
modelling tools for accessing historical market data published by AEMO and preprocessing it for compatibility with nempy.
```python
# Notice: this script downloads large volumes of historical market data from AEMO's nemweb portal.

import sqlite3
import pandas as pd
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, \
    constraints

con = sqlite3.connect('market_management_system.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('cache_directory')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = True

if download_inputs:
    # This requires approximately 5 GB of storage.
    mms_db_manager.populate(start_year=2019, start_month=1,
                            end_year=2019, end_month=1)

    # This requires approximately 60 GB of storage.
    xml_cache_manager.populate(start_year=2019, start_month=1,
                               end_year=2019, end_month=1)

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
    interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
    constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
    demand_inputs = demand.DemandData(raw_inputs_loader)

    unit_info = unit_inputs.get_unit_info()
    market = markets.SpotMarket(market_regions=['QLD1', 'NSW1', 'VIC1',
                                                'SA1', 'TAS1'],
                                unit_info=unit_info)

    # Set bids
    volume_bids, price_bids = unit_inputs.get_processed_bids()
    market.set_unit_volume_bids(volume_bids)
    market.set_unit_price_bids(price_bids)

    # Set bid in capacity limits
    unit_bid_limit = unit_inputs.get_unit_bid_availability()
    market.set_unit_bid_capacity_constraints(unit_bid_limit)
    cost = constraint_inputs.get_constraint_violation_prices()['unit_capacity']
    market.make_constraints_elastic('unit_bid_capacity', violation_cost=cost)

    # Set limits provided by the unconstrained intermittent generation
    # forecasts. Primarily for wind and solar.
    unit_uigf_limit = unit_inputs.get_unit_uigf_limits()
    market.set_unconstrained_intermitent_generation_forecast_constraint(
        unit_uigf_limit)
    cost = constraint_inputs.get_constraint_violation_prices()['uigf']
    market.make_constraints_elastic('uigf_capacity', violation_cost=cost)

    # Set unit ramp rates.
    ramp_rates = unit_inputs.get_ramp_rates_used_for_energy_dispatch()
    market.set_unit_ramp_up_constraints(
        ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
    market.set_unit_ramp_down_constraints(
        ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
    cost = constraint_inputs.get_constraint_violation_prices()['ramp_rate']
    market.make_constraints_elastic('ramp_up', violation_cost=cost)
    market.make_constraints_elastic('ramp_down', violation_cost=cost)

    # Set unit FCAS trapezium constraints.
    unit_inputs.add_fcas_trapezium_constraints()
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_max_avail']
    fcas_availability = unit_inputs.get_fcas_max_availability()
    market.set_fcas_max_availability(fcas_availability)
    market.make_constraints_elastic('fcas_max_availability', cost)
    cost = constraint_inputs.get_constraint_violation_prices()['fcas_profile']
    regulation_trapeziums = unit_inputs.get_fcas_regulation_trapeziums()
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)
    market.make_constraints_elastic('energy_and_regulation_capacity', cost)
    scada_ramp_down_rates = unit_inputs.get_scada_ramp_down_rates_of_lower_reg_units()
    market.set_joint_ramping_constraints_lower_reg(scada_ramp_down_rates)
    market.make_constraints_elastic('joint_ramping_lower_reg', cost)
    scada_ramp_up_rates = unit_inputs.get_scada_ramp_up_rates_of_raise_reg_units()
    market.set_joint_ramping_constraints_raise_reg(scada_ramp_up_rates)
    market.make_constraints_elastic('joint_ramping_raise_reg', cost)
    contingency_trapeziums = unit_inputs.get_contingency_services()
    market.set_joint_capacity_constraints(contingency_trapeziums)
    market.make_constraints_elastic('joint_capacity', cost)

    # Set interconnector definitions, limits and loss models.
    interconnectors_definitions = \
        interconnector_inputs.get_interconnector_definitions()
    loss_functions, interpolation_break_points = \
        interconnector_inputs.get_interconnector_loss_model()
    market.set_interconnectors(interconnectors_definitions)
    market.set_interconnector_losses(loss_functions,
                                     interpolation_break_points)

    # Add generic constraints and FCAS market constraints.
    fcas_requirements = constraint_inputs.get_fcas_requirements()
    market.set_fcas_requirements_constraints(fcas_requirements)
    violation_costs = constraint_inputs.get_violation_costs()
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    generic_rhs = constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()
    market.set_generic_constraints(generic_rhs)
    market.make_constraints_elastic('generic', violation_cost=violation_costs)
    unit_generic_lhs = constraint_inputs.get_unit_lhs()
    market.link_units_to_generic_constraints(unit_generic_lhs)
    interconnector_generic_lhs = constraint_inputs.get_interconnector_lhs()
    market.link_interconnectors_to_generic_constraints(
        interconnector_generic_lhs)

    # Set the operational demand to be met by dispatch.
    regional_demand = demand_inputs.get_operational_demand()
    market.set_demand_constraints(regional_demand)
    
    # Get unit dispatch without fast start constraints and use it to
    # make fast start unit commitment decisions.
    market.dispatch()
    dispatch = market.get_unit_dispatch()
    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
    market.set_fast_start_constraints(fast_start_profiles)
    if 'fast_start' in market.get_constraint_set_names():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    # If AEMO historical used the over constrained dispatch rerun
    # process then allow it to be used in dispatch. This is needed
    # because sometimes the conditions for over constrained dispatch
    # are present but the rerun process isn't used.
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=14500.0,
                        fcas_market_ceiling_price=1000.0)
    else:
        # The market price ceiling and floor are not needed here
        # because they are only used for the over constrained
        # dispatch rerun process.
        market.dispatch(allow_over_constrained_dispatch_re_run=False)

    # Save prices from this interval
    prices = market.get_energy_prices()
    prices['time'] = interval
    outputs.append(prices.loc[:, ['time', 'region', 'price']])

con.close()
print(pd.concat(outputs))
#                   time region      price
# 0  2019/01/01 12:00:00   NSW1  91.870167
# 1  2019/01/01 12:00:00   QLD1  76.190796
# 2  2019/01/01 12:00:00    SA1  86.899534
# 3  2019/01/01 12:00:00   TAS1  89.805037
# 4  2019/01/01 12:00:00   VIC1  84.984255
# 0  2019/01/01 12:05:00   NSW1  91.870496
# 1  2019/01/01 12:05:00   QLD1  64.991736
# 2  2019/01/01 12:05:00    SA1  87.462599
# 3  2019/01/01 12:05:00   TAS1  90.178036
# 4  2019/01/01 12:05:00   VIC1  85.556009
# 0  2019/01/01 12:10:00   NSW1  91.870496
# 1  2019/01/01 12:10:00   QLD1  64.991736
# 2  2019/01/01 12:10:00    SA1  86.868556
# 3  2019/01/01 12:10:00   TAS1  89.983716
# 4  2019/01/01 12:10:00   VIC1  84.936150
# 0  2019/01/01 12:15:00   NSW1  91.870496
# 1  2019/01/01 12:15:00   QLD1  64.776456
# 2  2019/01/01 12:15:00    SA1  86.844540
# 3  2019/01/01 12:15:00   TAS1  89.582288
# 4  2019/01/01 12:15:00   VIC1  84.990796
# 0  2019/01/01 12:20:00   NSW1  91.870496
# 1  2019/01/01 12:20:00   QLD1  64.776456
# 2  2019/01/01 12:20:00    SA1  87.496112
# 3  2019/01/01 12:20:00   TAS1  90.291144
# 4  2019/01/01 12:20:00   VIC1  85.594840
# 0  2019/01/01 12:25:00   NSW1  91.870167
# 1  2019/01/01 12:25:00   QLD1  64.991736
# 2  2019/01/01 12:25:00    SA1  87.519993
# 3  2019/01/01 12:25:00   TAS1  90.488064
# 4  2019/01/01 12:25:00   VIC1  85.630617
# 0  2019/01/01 12:30:00   NSW1  91.870496
# 1  2019/01/01 12:30:00   QLD1  64.991736
# 2  2019/01/01 12:30:00    SA1  87.462000
# 3  2019/01/01 12:30:00   TAS1  90.196284
# 4  2019/01/01 12:30:00   VIC1  85.573321
```
