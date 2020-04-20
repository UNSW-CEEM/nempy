import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import markets


def test_one_region_energy_market():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [20, 20],  # MW
        '2': [50, 30],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50, 52],  # $/MW
        '2': [53, 60],  # $/MW
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [55, 90],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'NSW'],  # MW
        'loss_factor': [0.9, 0.95]  # MW/h
    })

    demand = pd.DataFrame({
        'region': ['NSW'],
        'demand': [60]  # MW
    })

    simple_market = markets.Spot(unit_info=unit_info, dispatch_interval=5)
    simple_market.set_unit_energy_volume_bids(volume_bids)
    simple_market.set_unit_capacity_constraints(unit_limits)
    simple_market.set_unit_energy_price_bids(price_bids)
    simple_market.set_demand_constraints(demand)
    simple_market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW'],
        'price': [53/0.9]
    })

    expected_variable_values = pd.DataFrame({
        'variable_id': [0, 1, 2, 3],
        'value': [20.0, 20.0, 20.0, 0.0]
    })

    assert_frame_equal(simple_market.market_constraints_rhs_and_type['energy_market'].loc[:, ['region', 'price']],
                       expected_prices)
    assert_frame_equal(simple_market.decision_variables['energy_units'].loc[:, ['variable_id', 'value']],
                       expected_variable_values)


def test_two_region_energy_market():
    # Volume of each bid, number of bid bands must equal number of bands in price_bids.
    volume_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [20, 20],  # MW
        '2': [50, 100],  # MW
    })

    # Price of each bid, bids must be monotonically increasing.
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50, 52],  # $/MW
        '2': [53, 60],  # $/MW
    })

    # Factors limiting unit output
    unit_limits = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [70, 120],  # MW
    })

    # Other unit properties
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['NSW', 'VIC'],  # MW
        'loss_factor': [0.9, 0.95]  # MW/h
    })

    demand = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'demand': [60, 80]  # MW
    })

    simple_market = markets.RealTime(unit_info=unit_info, dispatch_interval=5)
    simple_market.set_unit_energy_volume_bids(volume_bids)
    simple_market.set_unit_capacity_constraints(unit_limits)
    simple_market.set_unit_energy_price_bids(price_bids)
    simple_market.set_demand_constraints(demand)
    simple_market.dispatch()

    expected_prices = pd.DataFrame({
        'region': ['NSW', 'VIC'],
        'price': [53/0.9, 60/0.95]
    })

    expected_variable_values = pd.DataFrame({
        'variable_id': [0, 1, 2, 3],
        'value': [20.0, 40.0, 20.0, 60.0]
    })

    assert_frame_equal(simple_market.market_constraints_rhs_and_type['energy_market'].loc[:, ['region', 'price']],
                       expected_prices)
    assert_frame_equal(simple_market.decision_variables['energy_units'].loc[:, ['variable_id', 'value']],
                       expected_variable_values)