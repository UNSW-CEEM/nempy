import os
import sqlite3
import pandas as pd
from nempy import markets
from nempy.help_functions import helper_functions as hf
from nempy.historical import historical_inputs_from_mms_db as hi

# Create a list of the historical dispatch intervals to be used.
dispatch_intervals = hi.datetime_dispatch_sequence(start_time='2020/01/02 00:00:00',
                                                   end_time='2020/01/02 00:05:00')

# Build a database of historical inputs if it doesn't already exist.
if not os.path.isfile('historical.db'):
    con = sqlite3.connect('historical.db')

    # Create a data base manager.
    inputs_manager = hi.DBManager(connection=con)

    # This is the first time the database has been used so we need to add the tables.
    # inputs_manager.create_tables()

    # Download the relevant historical data from
    # http://nemweb.com.au/#mms-data-model and into the database.

    # # Unit regions, loss factors, dispatch types.
    # inputs_manager.DUDETAILSUMMARY.set_data(year=2020, month=1)
    # # Volume bids
    # inputs_manager.BIDPEROFFER_D.add_data(year=2020, month=1)
    # # Price bids
    # inputs_manager.BIDDAYOFFER_D.add_data(year=2020, month=1)
    # # Unit availability, ramp rates etc.
    # inputs_manager.DISPATCHLOAD.add_data(year=2020, month=1)
    # # Regional demand.
    # inputs_manager.DISPATCHREGIONSUM.add_data(year=2020, month=1)
    # # Definitions of interconnector by connected regions.
    # inputs_manager.INTERCONNECTOR.set_data(year=2020, month=1)
    # # Record of which interconnectors were used in each dispatch interval
    # inputs_manager.DISPATCHINTERCONNECTORRES.add_data(year=2020, month=1)
    # # Interconnector parameters
    # inputs_manager.INTERCONNECTORCONSTRAINT.set_data(year=2020, month=1)
    # # Regional demand coefficients in interconnector loss functions.
    # inputs_manager.LOSSFACTORMODEL.set_data(year=2020, month=1)
    # # Break points for linear interpolation of interconnectors loss functions.
    # inputs_manager.LOSSMODEL.set_data(year=2020, month=1)
    # # FCAS requirements across.
    # inputs_manager.SPDREGIONCONSTRAINT.set_data(year=2020, month=1)
    # # Definition of generic constraints by grid connection point.
    # inputs_manager.SPDCONNECTIONPOINTCONSTRAINT.set_data(year=2020, month=1)
    # # Definition of generic constraints by interconnector.
    # inputs_manager.SPDINTERCONNECTORCONSTRAINT.set_data(year=2020, month=1)
    # # Generic constraint direction i.e. >=, <= or =.
    # inputs_manager.GENCONDATA.set_data(year=2020, month=1)
    # # Generic constraint right hand side (rhs).
    # inputs_manager.DISPATCHCONSTRAINT.add_data(year=2020, month=1)
    # # Historical prices for comparison
    # inputs_manager.DISPATCHPRICE.add_data(year=2020, month=1)

    con.close()

# Connect to the database of historical inputs
con = sqlite3.connect('historical.db')
inputs_manager = hi.DBManager(connection=con)

# List for saving outputs to.
outputs = []

# Create and dispatch the spot market for each dispatch interval.
for interval in dispatch_intervals:
    historical_prices = inputs_manager.DISPATCHPRICE.get_data(interval)
    # Transform the historical input data into the format accepted
    # by the Spot market class.

    # Unit info.
    DUDETAILSUMMARY = inputs_manager.DUDETAILSUMMARY.get_data(interval)
    unit_info = hi.format_unit_info(DUDETAILSUMMARY)

    # Unit bids.
    BIDPEROFFER_D = inputs_manager.BIDPEROFFER_D.get_data(interval)
    BIDDAYOFFER_D = inputs_manager.BIDDAYOFFER_D.get_data(interval)

    # The unit operating conditions at the start of the historical interval.
    DISPATCHLOAD = inputs_manager.DISPATCHLOAD.get_data(interval)
    unit_limits = hi.determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

    # FCAS bid prepocessing
    BIDPEROFFER_D = \
        hi.scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)
    BIDPEROFFER_D = \
        hi.scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD)
    BIDPEROFFER_D = \
        hi.scaling_for_uigf(BIDPEROFFER_D, DISPATCHLOAD, DUDETAILSUMMARY)
    BIDPEROFFER_D, BIDDAYOFFER_D = \
        hi.enforce_preconditions_for_enabling_fcas(
            BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, unit_limits.loc[:, ['unit', 'capacity']])
    BIDPEROFFER_D, BIDDAYOFFER_D = hi.use_historical_actual_availability_to_filter_fcas_bids(
        BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD)

    # Change bidding data to conform to nempy input format.
    volume_bids = hi.format_volume_bids(BIDPEROFFER_D)
    price_bids = hi.format_price_bids(BIDDAYOFFER_D)
    fcas_trapeziums = hi.format_fcas_trapezium_constraints(BIDPEROFFER_D)

    # Demand on regional basis.
    DISPATCHREGIONSUM = inputs_manager.DISPATCHREGIONSUM.get_data(interval)
    regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)

    # FCAS volumes required.
    SPDREGIONCONSTRAINT = inputs_manager.SPDREGIONCONSTRAINT.get_data(interval)
    DISPATCHCONSTRAINT = inputs_manager.DISPATCHCONSTRAINT.get_data(interval)
    GENCONDATA = inputs_manager.GENCONDATA.get_data(interval)
    fcas_requirements = hi.format_fcas_market_requirements(
        SPDREGIONCONSTRAINT, DISPATCHCONSTRAINT, GENCONDATA)

    # Generic constraint definitions.
    SPDINTERCONNECTORCONSTRAINT = inputs_manager.SPDINTERCONNECTORCONSTRAINT.get_data(interval)
    SPDCONNECTIONPOINTCONSTRAINT = inputs_manager.SPDCONNECTIONPOINTCONSTRAINT.get_data(interval)
    generic_rhs = hi.format_generic_constraints_rhs_and_type(DISPATCHCONSTRAINT, GENCONDATA)
    unit_generic_lhs = hi.format_generic_unit_lhs(SPDCONNECTIONPOINTCONSTRAINT, DUDETAILSUMMARY)
    interconnector_generic_lhs = hi.format_generic_interconnector_lhs(SPDINTERCONNECTORCONSTRAINT)

    # Interconnector details.
    INTERCONNECTOR = inputs_manager.INTERCONNECTOR.get_data()
    INTERCONNECTORCONSTRAINT = inputs_manager.INTERCONNECTORCONSTRAINT.get_data(interval)
    interconnectors = hi.format_interconnector_definitions(
        INTERCONNECTOR, INTERCONNECTORCONSTRAINT)
    interconnector_loss_coefficients = hi.format_interconnector_loss_coefficients(INTERCONNECTORCONSTRAINT)
    LOSSFACTORMODEL = inputs_manager.LOSSFACTORMODEL.get_data(interval)
    interconnector_demand_coefficients = hi.format_interconnector_loss_demand_coefficient(LOSSFACTORMODEL)
    LOSSMODEL = inputs_manager.LOSSMODEL.get_data(interval)
    interpolation_break_points = hi.format_interpolation_break_points(LOSSMODEL)
    loss_functions = hi.create_loss_functions(interconnector_loss_coefficients, interconnector_demand_coefficients,
                                              regional_demand.loc[:, ['region', 'loss_function_demand']])

    # Create a market instance.
    market = markets.SpotMarket()

    # Add generators to the market.
    market.set_unit_info(unit_info.loc[:, ['unit', 'region', 'dispatch_type']])

    # Set volume of each bids.
    volume_bids = volume_bids[volume_bids['unit'].isin(list(unit_info['unit']))]
    market.set_unit_volume_bids(volume_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                                    '6', '7', '8', '9', '10']])

    # Set prices of each bid.
    price_bids = price_bids[price_bids['unit'].isin(list(unit_info['unit']))]
    market.set_unit_price_bids(price_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                                  '6', '7', '8', '9', '10']])

    # Set unit operating limits.
    market.set_unit_capacity_constraints(unit_limits.loc[:, ['unit', 'capacity']])
    market.set_unit_ramp_up_constraints(unit_limits.loc[:, ['unit', 'initial_output', 'ramp_up_rate']])
    market.set_unit_ramp_down_constraints(unit_limits.loc[:, ['unit', 'initial_output', 'ramp_down_rate']])

    # Create constraints that enforce the top of the FCAS trapezium.
    fcas_availability = fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]
    market.set_fcas_max_availability(fcas_availability)

    # Create constraints the enforce the lower and upper slope of the FCAS regulation
    # service trapeziums.
    regulation_trapeziums = fcas_trapeziums[fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]
    market.set_energy_and_regulation_capacity_constraints(regulation_trapeziums)
    market.make_constraints_elastic('energy_and_regulation_capacity', 14000.0)
    market.set_joint_ramping_constraints(regulation_trapeziums.loc[:, ['unit', 'service']],
                                         unit_limits.loc[:, ['unit', 'initial_output',
                                                             'ramp_down_rate', 'ramp_up_rate']])
    market.make_constraints_elastic('joint_ramping', 14000.0)

    # Create constraints that enforce the lower and upper slope of the FCAS contingency
    # trapezium. These constrains also scale slopes of the trapezium to ensure the
    # co-dispatch of contingency and regulation services is technically feasible.
    contingency_trapeziums = fcas_trapeziums[~fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]
    market.set_joint_capacity_constraints(contingency_trapeziums)
    market.make_constraints_elastic('joint_capacity', 14000.0)

    # Set regional demand.
    market.set_demand_constraints(regional_demand.loc[:, ['region', 'demand']])

    # Set FCAS requirements.
    market.set_fcas_requirements_constraints(fcas_requirements)

    # Set generic constraints
    market.set_generic_constraints(generic_rhs)
    GENCONDATA['cost'] = GENCONDATA['GENERICCONSTRAINTWEIGHT'] * 14000.0
    generic_constraint_violation_costs = GENCONDATA.loc[:, ['GENCONID', 'cost']]
    generic_constraint_violation_costs.columns = ['set', 'cost']
    market.make_constraints_elastic('generic', generic_constraint_violation_costs)
    market.link_units_to_generic_constraints(unit_generic_lhs)
    market.link_interconnectors_to_generic_constraints(interconnector_generic_lhs)

    # Create the interconnectors.
    market.set_interconnectors(interconnectors)

    # Create loss functions on per interconnector basis.
    market.set_interconnector_losses(loss_functions, interpolation_break_points)

    # Calculate dispatch.
    market.dispatch()

    print('Dispatch for interval {} complete.'.format(interval))

    # Save prices from this interval
    energy_prices = market.get_energy_prices()
    energy_prices['time'] = interval
    energy_prices['service'] = 'energy'
    fcas_prices = market.get_fcas_prices()
    fcas_prices['time'] = interval
    prices = pd.concat([energy_prices, fcas_prices])

    price_to_service = {'RRP': 'energy', 'RAISE6SECRRP': 'raise_6s', 'RAISE60SECRRP': 'raise_60s',
                        'RAISE5MINRRP': 'raise_5min', 'RAISEREGRRP': 'raise_reg', 'LOWER6SECRRP': 'lower_6s',
                       'LOWER60SECRRP': 'lower_60s', 'LOWER5MINRRP': 'lower_5min', 'LOWERREGRRP': 'lower_reg'}
    price_columns = list(price_to_service.keys())
    historical_prices = hf.stack_columns(historical_prices, cols_to_keep=['SETTLEMENTDATE', 'REGIONID'],
                                         cols_to_stack=price_columns, type_name='service',
                                         value_name='RRP')
    historical_prices['service'] = historical_prices['service'].apply(lambda x: price_to_service[x])
    historical_prices = historical_prices.loc[:, ['SETTLEMENTDATE', 'REGIONID', 'service', 'RRP']]
    historical_prices.columns = ['time', 'region', 'service', 'hist_price']
    prices = pd.merge(prices, historical_prices, on=['time', 'region', 'service'])
    outputs.append(prices)


con.close()
outputs = pd.concat(outputs)
outputs.to_csv('full_feature_results')
print(outputs)
#    region      price                 time
# 0    NSW1  61.114147  2020/01/02 15:05:00
# 1    QLD1  58.130015  2020/01/02 15:05:00
# 2     SA1  72.675411  2020/01/02 15:05:00
# 3    TAS1  73.013327  2020/01/02 15:05:00
# 4    VIC1  68.778493  2020/01/02 15:05:00
# ..    ...        ...                  ...
# 0    NSW1  54.630861  2020/01/02 21:00:00
# 1    QLD1  55.885854  2020/01/02 21:00:00
# 2     SA1  53.038412  2020/01/02 21:00:00
# 3    TAS1  61.537939  2020/01/02 21:00:00
# 4    VIC1  57.040000  2020/01/02 21:00:00
#
# [360 rows x 3 columns]
