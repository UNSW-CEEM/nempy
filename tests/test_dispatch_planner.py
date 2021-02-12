import pandas as pd
import numpy as np
from pandas._testing import assert_frame_equal
from nempy.bidding_model import planner


def test_energy_storage_over_two_intervals_with_inelastic_prices():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101)})

    forward_data = pd.DataFrame({
        'interval': [0, 1],
        'nsw-demand': [100, 200]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data)

    p.add_unit('storage_one', 'nsw')
    p.add_unit_to_market_flow('storage_one', 1.0)
    p.add_market_to_unit_flow('storage_one', 1.0)
    p.add_storage('storage_one', mwh=1.0, initial_mwh=0.0, output_capacity=1.0, input_capacity=1.0,
                  output_efficiency=1.0, input_efficiency=1.0)

    p.add_regional_market('nsw', 'energy')

    p.optimise()

    dispatch = p.get_unit_dispatch('storage_one')

    expect_dispatch = pd.DataFrame({
        'interval': [0, 1],
        'net_dispatch': [-1.0, 1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_energy_storage_over_two_intervals_with_inelastic_prices_with_inefficiencies():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101)})

    forward_data = pd.DataFrame({
        'interval': [0, 1],
        'nsw-demand': [100, 200]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data)

    p.add_unit('storage_one', 'nsw')
    p.add_unit_to_market_flow('storage_one', 1.0)
    p.add_market_to_unit_flow('storage_one', 1.0)
    p.add_storage('storage_one', mwh=1.0, initial_mwh=0.0, output_capacity=1.0, input_capacity=1.0,
                  output_efficiency=0.9, input_efficiency=0.8)

    p.add_regional_market('nsw', 'energy')

    p.optimise()

    dispatch = p.get_unit_dispatch('storage_one')

    expect_dispatch = pd.DataFrame({
        'interval': [0, 1],
        'net_dispatch': [-1.0, 0.9 * 0.8]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_energy_storage_over_three_intervals_with_elastic_prices():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101)})

    forward_data = pd.DataFrame({
        'interval': [0, 1, 2],
        'nsw-demand': [100, 400, 400]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('storage_one', 'nsw')
    p.add_unit_to_market_flow('storage_one', 50.0)
    p.add_market_to_unit_flow('storage_one', 50.0)
    p.add_storage('storage_one', mwh=50.0, initial_mwh=0.0, output_capacity=50.0, input_capacity=50.0,
                  output_efficiency=1.0, input_efficiency=1.0)

    p.add_regional_market('nsw', 'energy')

    p.optimise()

    dispatch = p.get_unit_dispatch('storage_one')

    expect_dispatch = pd.DataFrame({
        'interval': [0, 1, 2],
        'net_dispatch': [-50.0, 25.0, 25.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_convergence_across_strongly_linked_energy_markets():
    """One energy storage unit in each market, 3 dispatch intervals"""

    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'vic-demand': np.linspace(0, 500, num=101),
        'vic-energy-fleet-dispatch': np.zeros(101)
    })

    historical_data['nsw-energy'] = historical_data['nsw-demand'] + historical_data['vic-demand']
    historical_data['vic-energy'] = historical_data['nsw-demand'] + historical_data['vic-demand']

    forward_data = pd.DataFrame({
        'interval': [0, 1, 2, 3, 4],
        'nsw-demand': [100, 400, 400, 400, 400],
        'vic-demand': [100, 400, 400, 400, 400]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('storage_one', 'nsw')
    p.add_unit_to_market_flow('storage_one', 50.0)
    p.add_market_to_unit_flow('storage_one', 50.0)
    p.add_storage('storage_one', mwh=50.0, initial_mwh=0.0, output_capacity=50.0, input_capacity=50.0,
                  output_efficiency=1.0, input_efficiency=1.0)

    p.add_regional_market('nsw', 'energy')

    p.add_unit('storage_two', 'vic')
    p.add_unit_to_market_flow('storage_two', 50.0)
    p.add_market_to_unit_flow('storage_two', 50.0)
    p.add_storage('storage_two', mwh=50.0, initial_mwh=0.0, output_capacity=50.0, input_capacity=50.0,
                  output_efficiency=1.0, input_efficiency=1.0)

    p.add_regional_market('vic', 'energy')

    p.optimise()

    dispatch_nsw = p.get_unit_dispatch('storage_one')
    dispatch_vic = p.get_unit_dispatch('storage_two')

    expect_dispatch = pd.DataFrame({
        'interval': [0, 1, 2, 3, 4],
        'net_dispatch': [-50.0, 25.0, 25.0, 0.0, 0.0]
    })

    assert_frame_equal(expect_dispatch, dispatch_nsw)
    assert_frame_equal(expect_dispatch, dispatch_vic)

