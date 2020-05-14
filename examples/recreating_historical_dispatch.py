import pandas as pd
import sqlite3
from nempy import markets, historical_spot_market_inputs as hi

# Create a list of the historical dispatch intervals to be used.
dispatch_intervals = hi.datetime_dispatch_sequence(start_time='2020/01/02 00:00:00', end_time='2020/01/02 00:05:00')

# Create a database for the require inputs.
con = sqlite3.connect('historical_inputs.db')

# Create a data base manager.
inputs_manager = hi.DBManager(connection=con)

# This is the first time the database has been used so we need to add the tables.
# inputs_manager.create_tables()

# Download the relevant historical data from http://nemweb.com.au/#mms-data-model and into the database.
# inputs_manager.DUDETAILSUMMARY.set_data(year=2020, month=1)  # Unit information
# inputs_manager.BIDPEROFFER_D.add_data(year=2020, month=1)  # historical volume bids
# inputs_manager.BIDDAYOFFER_D.add_data(year=2020, month=1)  # historical price bids
# inputs_manager.DISPATCHREGIONSUM.add_data(year=2020, month=1)  # historical demand
# inputs_manager.INTERCONNECTOR.set_data(year=2020, month=1)  # Regions connected by interconnector
# inputs_manager.DISPATCHINTERCONNECTORRES.add_data(year=2020, month=1)  # Interconnectors used in each dispatch interval
# inputs_manager.INTERCONNECTORCONSTRAINT.set_data(year=2020, month=1)  # Interconnector data
# inputs_manager.LOSSFACTORMODEL.set_data(year=2020, month=1)  # Regional demand coefficients in loss functions
# inputs_manager.LOSSMODEL.set_data(year=2020, month=1)  # Break points for linear interpolation of loss functions

# Create and dispatch the spot market for each dispatch interval.
for interval in dispatch_intervals:
    # Transform the historical input data into the format accepted by the Spot market class.
    unit_info = inputs_manager.DUDETAILSUMMARY.get_data(interval)
    unit_info = hi.format_unit_info(unit_info)
    volume_bids = inputs_manager.BIDPEROFFER_D.get_data(interval)
    volume_bids = hi.format_volume_bids(volume_bids)
    price_bids = inputs_manager.BIDDAYOFFER_D.get_data(interval)
    price_bids = hi.format_price_bids(price_bids)
    interconnector_directions = inputs_manager.INTERCONNECTOR.get_data()
    interconnector_paramaters = inputs_manager.INTERCONNECTORCONSTRAINT.get_data(interval)
    interconnectors = hi.format_interconnector_definitions(interconnector_directions, interconnector_paramaters)
    interconnector_loss_coefficients = hi.format_interconnector_loss_coefficients(interconnector_paramaters)
    interconnector_demand_coefficients = inputs_manager.LOSSFACTORMODEL.get_data(interval)
    interconnector_demand_coefficients = hi.format_interconnector_loss_demand_coefficient(
        interconnector_demand_coefficients)
    interpolation_break_points = inputs_manager.LOSSMODEL.get_data(interval)
    interpolation_break_points = hi.format_interpolation_break_points(interpolation_break_points)
    regional_demand = inputs_manager.DISPATCHREGIONSUM.get_data(interval)
    regional_demand = hi.format_regional_demand(regional_demand)

    # Create a market instance.
    simple_market = markets.Spot()

    # The only generator is located in NSW.
    unit_info = unit_info[unit_info['dispatch_type'] == 'GENERATOR']
    simple_market.set_unit_info(unit_info.loc[:, ['unit', 'region']])

    # Volume of each bids.
    volume_bids = volume_bids[volume_bids['service'] == 'ENERGY']
    simple_market.set_unit_volume_bids(volume_bids.loc[:, ['unit', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']])

    # Price of each bid.
    price_bids = price_bids[price_bids['service'] == 'ENERGY']
    simple_market.set_unit_price_bids(price_bids.loc[:, ['unit', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']])

    # regional_demand
    simple_market.set_demand_constraints(regional_demand.loc[:, ['region', 'demand']])

    # There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
    simple_market.set_interconnectors(interconnectors)

    # Create loss functions on per interconnector basis.
    loss_functions = hi.create_loss_functions(interconnector_loss_coefficients,
                                              interconnector_demand_coefficients,
                                              regional_demand.loc[:, ['region', 'demand']])

    simple_market.set_interconnector_losses(loss_functions, interpolation_break_points)

    # Calculate dispatch.
    simple_market.dispatch()

    # Return the total dispatch of each unit in MW.
    print(simple_market.get_unit_dispatch())
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