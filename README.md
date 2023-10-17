# Nempy

[![Current build](https://github.com/UNSW-CEEM/nempy/actions/workflows/test.yml/badge.svg)](https://github.com/UNSW-CEEM/nempy/actions/workflows/test.yml)
[![Documentation](https://readthedocs.org/projects/nempy/badge/?version=latest)](https://nempy.readthedocs.io/en/latest/?badge=latest)
[![DOI](https://joss.theoj.org/papers/10.21105/joss.03596/status.svg)](https://doi.org/10.21105/joss.03596)

## Table of Contents
- [Introduction](https://github.com/UNSW-CEEM/nempy#introduction)
- [Installation](https://github.com/UNSW-CEEM/nempy#installation)
- [Documentation](https://github.com/UNSW-CEEM/nempy#documentation)
- [Community](https://github.com/UNSW-CEEM/nempy#community)
- [Author](https://github.com/UNSW-CEEM/nempy#author)
- [Citation](https://github.com/UNSW-CEEM/nempy#citation)
- [License](https://github.com/UNSW-CEEM/nempy#license)
- [Examples](https://github.com/UNSW-CEEM/nempy#examples)

## Introduction

Nempy is a Python package for modelling the dispatch procedure of the Australian National Electricity Market (NEM). The idea is 
that you can start simple and grow the complexity of your model by adding features such as 
ramping constraints, interconnectors, FCAS markets and more. See the [examples](https://github.com/UNSW-CEEM/nempy#examples) below.

| ![nempy-accuracy](https://github.com/prakaa/nempy/assets/40549624/6a994cee-3255-4e3d-b04b-6d4d7e155065) | 
|:--:| 
| *Dispatch price results from the New South Wales region for 1000 randomly selected intervals in the 2019 calendar year. The actual prices, prior to scaling or capping, are also shown for comparison. Results from two Nempy models are shown, one with a full set of dispatch features, and one without FCAS markets or generic constraints (network and security constraints). Actual prices, results from the full featured model, and the simpler model are shown in descending order for actual prices, results from the simpler model are also shown resorted.* |

For further details, refer to the [documentation](https://nempy.readthedocs.io/en/latest/intro.html#).

For a brief introduction to the NEM, refer to this [ document](https://aemo.com.au/-/media/Files/Electricity/NEM/National-Electricity-Market-Fact-Sheet.pdf).

## Installation
Installing Nempy to use in your project is easy.

```bash
pip install nempy
```

## Documentation

A more detailed introduction to Nempy, examples, and reference documentation can be found on the 
[readthedocs](https://nempy.readthedocs.io/en/latest/) page.

## Community

Nempy is open-source and we welcome all forms of community engagement.

### Support

You can seek support for using Nempy using the [discussion tab on GitHub](https://github.com/UNSW-CEEM/nempy/discussions), checking the [issues register](https://github.com/UNSW-CEEM/nempy/issues), or by contacting Nick directly (n.gorman at unsw.edu.au).

If you cannot find a pre-existing issue related to your enquiry, you can submit a new one via the [issues register](https://github.com/UNSW-CEEM/nempy/issues). Issue submissions do not need to adhere to any particular format.

### Future support and maintenance

Planning to continue support and maintenance for Nempy after the PhD project is complete is currently underway. If Nempy
is useful to your work, research, or business, please reach out and inform us so we can consider your use case and
needs.

### Contributing

Contributions via pull requests are welcome. Contributions should:

1. Follow the PEP8 style guide (with exception of line length up to 120 rather than 80)
2. Ensure that all existing automated tests continue to pass (unless you are explicitly changing intended behavour; if you are, please highlight this in your pull request description)
3. Implement automated tests for new features
4. Provide doc strings for public interfaces

#### Installation for development

Nempy uses [`poetry`](https://python-poetry.org/docs/) as a dependency and project management tool. To install Nempy for development, clone or fork the repo and then run the following command in the main directory to install required dependencies and the source code as an editable project:

```bash
poetry install --with=dev
```
You can then work within the virtual environment using `poetry shell` or run commands within it using `poetry run`.

## Author

Nempy's development is being led by Nick Gorman as part of his PhD candidature at the Collaboration on Energy and Environmental
Markets at the University of New South Wales' School of Photovoltaics and Renewable Energy Engineering. (https://www.ceem.unsw.edu.au/). 

## Citation

If you use Nempy, please cite the package via the [JOSS paper](https://doi.org/10.5281/zenodo.7397514) (suggested citation below):
> Gorman et al., (2022). Nempy: A Python package for modelling the Australian National Electricity Market dispatch procedure. Journal of Open Source Software, 7(70), 3596, https://doi.org/10.21105/joss.03596

## License

Nempy was created by Nicholas Gorman. It is licensed under the terms of [the BSD 3-Clause Licence](./LICENSE).

## Examples
<details>

<summary>A simple example</summary>

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

</details>

<details>

<summary>A detailed example</summary>

The example demonstrates the broad range of market features that can be implemented with Nempy and the use of auxiliary 
modelling tools for accessing historical market data published by AEMO and preprocessing it for compatibility with Nempy.

> [!WARNING]  
> This example downloads approximately 54 GB of data from AEMO.

```python
# Notice:
# - This script downloads large volumes of historical market data (~54 GB) from AEMO's nemweb
#   portal. You can also reduce the data usage by restricting the time window given to the 
#   xml_cache_manager and in the get_test_intervals function. The boolean on line 23 can 
#   also be changed to prevent this happening repeatedly once the data has been downloaded. 

import sqlite3
from datetime import datetime, timedelta
import random
import pandas as pd
from nempy import markets
from nempy.historical_inputs import loaders, mms_db, \
    xml_cache, units, demand, interconnectors, constraints, rhs_calculator
from nempy.help_functions.helper_functions import update_rhs_values

con = sqlite3.connect('D:/nempy_2021/historical_mms.db')
mms_db_manager = mms_db.DBManager(connection=con)

xml_cache_manager = xml_cache.XMLCacheManager('D:/nempy_2021/xml_cache')

# The second time this example is run on a machine this flag can
# be set to false to save downloading the data again.
download_inputs = False

if download_inputs:
    # This requires approximately 4 GB of storage.
    mms_db_manager.populate(start_year=2021, start_month=1,
                            end_year=2021, end_month=1)

    # This requires approximately 50 GB of storage.
    xml_cache_manager.populate_by_day(start_year=2021, start_month=1, start_day=1,
                                      end_year=2021, end_month=2, end_day=1)

raw_inputs_loader = loaders.RawInputsLoader(
    nemde_xml_cache_manager=xml_cache_manager,
    market_management_system_database=mms_db_manager)


# A list of intervals we want to recreate historical dispatch for.
def get_test_intervals(number=100):
    start_time = datetime(year=2021, month=12, day=1, hour=0, minute=0)
    end_time = datetime(year=2021, month=12, day=31, hour=0, minute=0)
    difference = end_time - start_time
    difference_in_5_min_intervals = difference.days * 12 * 24
    random.seed(1)
    intervals = random.sample(range(1, difference_in_5_min_intervals), number)
    times = [start_time + timedelta(minutes=5 * i) for i in intervals]
    times_formatted = [t.isoformat().replace('T', ' ').replace('-', '/') for t in times]
    return times_formatted


# List for saving outputs to.
outputs = []
c = 0
# Create and dispatch the spot market for each dispatch interval.
for interval in get_test_intervals(number=100):
    c += 1
    print(str(c) + ' ' + str(interval))
    raw_inputs_loader.set_interval(interval)
    unit_inputs = units.UnitData(raw_inputs_loader)
    interconnector_inputs = interconnectors.InterconnectorData(raw_inputs_loader)
    constraint_inputs = constraints.ConstraintData(raw_inputs_loader)
    demand_inputs = demand.DemandData(raw_inputs_loader)
    rhs_calculation_engine = rhs_calculator.RHSCalc(xml_cache_manager)

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
    def set_ramp_rates(run_type):
        ramp_rates = unit_inputs.get_ramp_rates_used_for_energy_dispatch(run_type='fast_start_first_run')
        market.set_unit_ramp_up_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
        market.set_unit_ramp_down_constraints(
            ramp_rates.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])
        cost = constraint_inputs.get_constraint_violation_prices()['ramp_rate']
        market.make_constraints_elastic('ramp_up', violation_cost=cost)
        market.make_constraints_elastic('ramp_down', violation_cost=cost)


    set_ramp_rates(run_type='fast_start_first_run')

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
    contingency_trapeziums = unit_inputs.get_contingency_services()
    market.set_joint_capacity_constraints(contingency_trapeziums)
    market.make_constraints_elastic('joint_capacity', cost)


    def set_joint_ramping_constraints(run_type):
        scada_ramp_down_rates = unit_inputs.get_scada_ramp_down_rates_of_lower_reg_units(
            run_type=run_type)
        market.set_joint_ramping_constraints_lower_reg(scada_ramp_down_rates)
        market.make_constraints_elastic('joint_ramping_lower_reg', cost)
        scada_ramp_up_rates = unit_inputs.get_scada_ramp_up_rates_of_raise_reg_units(
            run_type=run_type)
        market.set_joint_ramping_constraints_raise_reg(scada_ramp_up_rates)
        market.make_constraints_elastic('joint_ramping_raise_reg', cost)


    set_joint_ramping_constraints(run_type="fast_start_first_run")

    # Set interconnector definitions, limits and loss models.
    interconnectors_definitions = \
        interconnector_inputs.get_interconnector_definitions()
    loss_functions, interpolation_break_points = \
        interconnector_inputs.get_interconnector_loss_model()
    market.set_interconnectors(interconnectors_definitions)
    market.set_interconnector_losses(loss_functions,
                                     interpolation_break_points)

    # Calculate rhs constraint values that depend on the basslink frequency controller from scratch so there is
    # consistency between the basslink switch runs.
    # Find the constraints that need to be calculated because they depend on the frequency controller status.
    constraints_to_update = (
        rhs_calculation_engine.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
    initial_bl_freq_onstatus = rhs_calculation_engine.scada_data['W']['BL_FREQ_ONSTATUS'][0]['@Value']
    # Calculate new rhs values for the constraints that need updating.
    new_rhs_values = rhs_calculation_engine.compute_constraint_rhs(constraints_to_update)

    # Add generic constraints and FCAS market constraints.
    fcas_requirements = constraint_inputs.get_fcas_requirements()
    fcas_requirements = update_rhs_values(fcas_requirements, new_rhs_values)
    market.set_fcas_requirements_constraints(fcas_requirements)
    violation_costs = constraint_inputs.get_violation_costs()
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    generic_rhs = constraint_inputs.get_rhs_and_type_excluding_regional_fcas_constraints()
    generic_rhs = update_rhs_values(generic_rhs, new_rhs_values)
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

    # Set tiebreak constraint to equalise dispatch of equally priced bids.
    cost = constraint_inputs.get_constraint_violation_prices()['tiebreak']
    market.set_tie_break_constraints(cost)

    # Get unit dispatch without fast start constraints and use it to
    # make fast start unit commitment decisions.
    market.dispatch()
    dispatch = market.get_unit_dispatch()
    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
    set_ramp_rates(run_type='fast_start_second_run')
    set_joint_ramping_constraints(run_type='fast_start_second_run')
    market.set_fast_start_constraints(fast_start_profiles)
    if 'fast_start' in market._constraints_rhs_and_type.keys():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    # First run of Basslink switch runs
    market.dispatch()  # First dispatch without allowing over constrained dispatch re-run to get objective function.
    objective_value_run_one = market.objective_value
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=14500.0,
                        fcas_market_ceiling_price=1000.0)
    prices_run_one = market.get_energy_prices()  # If this is the lowest cost run these will be the market prices.

    # Re-run dispatch with Basslink Frequency controller off.
    # Set frequency controller to off in rhs calculations
    rhs_calculation_engine.update_spd_id_value('BL_FREQ_ONSTATUS', 'W', '0')
    new_bl_freq_onstatus = rhs_calculation_engine.scada_data['W']['BL_FREQ_ONSTATUS'][0]['@Value']
    # Find the constraints that need to be updated because they depend on the frequency controller status.
    constraints_to_update = (
        rhs_calculation_engine.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W'))
    # Calculate new rhs values for the constraints that need updating.
    new_rhs_values = rhs_calculation_engine.compute_constraint_rhs(constraints_to_update)
    # Update the constraints in the market.
    fcas_requirements = update_rhs_values(fcas_requirements, new_rhs_values)
    violation_costs = constraint_inputs.get_violation_costs()
    market.set_fcas_requirements_constraints(fcas_requirements)
    market.make_constraints_elastic('fcas', violation_cost=violation_costs)
    generic_rhs = update_rhs_values(generic_rhs, new_rhs_values)
    market.set_generic_constraints(generic_rhs)
    market.make_constraints_elastic('generic', violation_cost=violation_costs)

    # Reset ramp rate constraints for first run of second Basslink switchrun
    set_ramp_rates(run_type='fast_start_first_run')
    set_joint_ramping_constraints(run_type='fast_start_first_run')

    # Get unit dispatch without fast start constraints and use it to
    # make fast start unit commitment decisions.
    market.remove_fast_start_constraints()
    market.dispatch()
    dispatch = market.get_unit_dispatch()
    fast_start_profiles = unit_inputs.get_fast_start_profiles_for_dispatch(dispatch)
    set_ramp_rates(run_type='fast_start_second_run')
    set_joint_ramping_constraints(run_type='fast_start_second_run')
    market.set_fast_start_constraints(fast_start_profiles)
    if 'fast_start' in market._constraints_rhs_and_type.keys():
        cost = constraint_inputs.get_constraint_violation_prices()['fast_start']
        market.make_constraints_elastic('fast_start', violation_cost=cost)

    market.dispatch()  # First dispatch without allowing over constrained dispatch re-run to get objective function.
    objective_value_run_two = market.objective_value
    if constraint_inputs.is_over_constrained_dispatch_rerun():
        market.dispatch(allow_over_constrained_dispatch_re_run=True,
                        energy_market_floor_price=-1000.0,
                        energy_market_ceiling_price=14500.0,
                        fcas_market_ceiling_price=1000.0)
    prices_run_two = market.get_energy_prices()  # If this is the lowest cost run these will be the market prices.

    prices_run_one['time'] = interval
    prices_run_two['time'] = interval

    # Getting historical prices for comparison. Note, ROP price, which is
    # the regional reference node price before the application of any
    # price scaling by AEMO, is used for comparison.
    historical_prices = mms_db_manager.DISPATCHPRICE.get_data(interval)

    # The prices from the run with the lowest objective function value are used.
    if objective_value_run_one < objective_value_run_two:
        prices = prices_run_one
    else:
        prices = prices_run_two

    prices['time'] = interval
    prices = pd.merge(prices, historical_prices,
                      left_on=['time', 'region'],
                      right_on=['SETTLEMENTDATE', 'REGIONID'])

    outputs.append(prices)

con.close()

outputs = pd.concat(outputs)

outputs['error'] = outputs['price'] - outputs['ROP']

print('\n Summary of error in energy price volume weighted average price. \n'
      'Comparison is against ROP, the price prior to \n'
      'any post dispatch adjustments, scaling, capping etc.')
print('Mean price error: {}'.format(outputs['error'].mean()))
print('Median price error: {}'.format(outputs['error'].quantile(0.5)))
print('5% percentile price error: {}'.format(outputs['error'].quantile(0.05)))
print('95% percentile price error: {}'.format(outputs['error'].quantile(0.95)))

#  Summary of error in energy price volume weighted average price. 
# Comparison is against ROP, the price prior to 
# any post dispatch adjustments, scaling, capping etc.
# Mean price error: -0.187389732115115
# Median price error: 0.0
# 5% percentile price error: -5.407795875785957
# 95% percentile price error: 0.8614836155591679
```
</details>
    
