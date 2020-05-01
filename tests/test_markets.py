import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import markets


def test_one_region_energy_market():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [20.0, 20.0],  # MW
        '2': [50.0, 30.0],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 52.0],  # $/MW
        '2': [53.0, 60.0],  # $/MW
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [55.0, 90.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW'],  # MW
        'loss_factor': [0.9, 0.95]  # MW/h
    })

    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [60.0]  # MW
    })

    simple_market = markets.Spot(dispatch_interval=5)
    simple_market.set_unit_info(unit_info)
    simple_market.set_unit_energy_volume_bids(volume_bids)
    simple_market.set_unit_capacity_constraints(unit_limits)
    simple_market.set_unit_energy_price_bids(price_bids)
    simple_market.set_demand_constraints(demand)
    simple_market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [53/0.9]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch': [40.0, 20.0]
    })

    assert_frame_equal(simple_market.get_energy_prices(), expected_prices)
    assert_frame_equal(simple_market.get_energy_dispatch(), expected_dispatch)


def test_two_region_energy_market():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [20.0, 20.0],  # MW
        '2': [50.0, 100.0],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 52.0],  # $/MW
        '2': [53.0, 60.0],  # $/MW
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [70.0, 120.0],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'VIC'],  # MW
        'loss_factor': [0.9, 0.95]  # MW/h
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [60.0, 80.0]  # MW
    })

    simple_market = markets.Spot(dispatch_interval=5)
    simple_market.set_unit_info(unit_info)
    simple_market.set_unit_energy_volume_bids(volume_bids)
    simple_market.set_unit_capacity_constraints(unit_limits)
    simple_market.set_unit_energy_price_bids(price_bids)
    simple_market.set_demand_constraints(demand)
    simple_market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [53/0.9, 60/0.95]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch': [60.0, 80.0]
    })

    assert_frame_equal(simple_market.get_energy_prices(), expected_prices)
    assert_frame_equal(simple_market.get_energy_dispatch(), expected_dispatch)


def test_one_interconnector():
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
        '1': [100.0]  # MW
    })

    simple_market.set_unit_energy_volume_bids(volume_bids)

    # Price of each bid.
    price_bids = pd.DataFrame({
        'unit': ['A'],
        '1': [50.0]  # $/MW
    })

    simple_market.set_unit_energy_price_bids(price_bids)

    # NSW has no demand but VIC has 90 MW.
    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [0.0, 90.0]  # MW
    })

    simple_market.set_demand_constraints(demand)

    # There is one interconnector between NSW and VIC. Its nominal direction is towards VIC.
    interconnectors = pd.DataFrame({
        'interconnector': ['little_link'],
        'to_region': ['VIC'],
        'from_region': ['NSW'],
        'max': [100.0],
        'min': [-120.0]
    })

    simple_market.set_interconnectors(interconnectors)

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

    # The points to linearly interpolate the loss function bewteen. In this example the loss function is linear so only
    # three points are needed, but if a non linear loss function was used then more points would be better.
    interpolation_break_points = pd.DataFrame({
        'interconnector': ['little_link', 'little_link', 'little_link'],
        'break_point': [-120.0, 0.0, 100]
    })

    simple_market.set_interconnector_losses(loss_functions, interpolation_break_points)

    # Calculate dispatch.
    simple_market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [50.0, 50.0 * (((90.0/0.975) + (90.0/0.975) * 0.025)/90)]
    })

    expected_dispatch = pd.DataFrame({
        'unit': ['A'],
        'dispatch': [(90.0/0.975) + (90.0/0.975) * 0.025]
    })

    expected_interconnector_flow = pd.DataFrame({
        'interconnector': ['little_link'],
        'flow': [90.0/0.975],
        'losses': [(90.0/0.975) * 0.05]
    })

    assert_frame_equal(simple_market.get_energy_prices(), expected_prices)
    assert_frame_equal(simple_market.get_energy_dispatch(), expected_dispatch)
    assert_frame_equal(simple_market.get_interconnector_flows(), expected_interconnector_flow)

