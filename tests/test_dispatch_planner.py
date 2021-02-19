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


def test_storage_providing_raise_6_second_service_1():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_6_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_6_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0, 1, 2],
        'nsw-demand': [400, 400, 400]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('storage_one', 'nsw')
    p.add_unit_to_market_flow('storage_one', 50.0)
    p.add_market_to_unit_flow('storage_one', 50.0)
    p.add_storage('storage_one', mwh=50.0, initial_mwh=0.0, output_capacity=50.0, input_capacity=50.0,
                  output_efficiency=1.0, input_efficiency=1.0)
    p.set_unit_fcas_region('storage_one', 'raise_6_second', 'nsw')
    p.add_contingency_service_to_output('storage_one', 'raise_6_second', 50.0)
    p.add_contingency_service_to_input('storage_one', 'raise_6_second', 50.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_6_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0, 1, 2],
        'nsw-energy-dispatch': [0.0, 0.0, 0.0],
        'nsw-raise_6_second-dispatch': [50.0, 50.0, 50.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_storage_providing_raise_6_second_service_2():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_6_second': np.linspace(0, 500, num=101) * 0.5,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_6_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0, 1],
        'nsw-demand': [150, 500]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('storage_one', 'nsw')
    p.add_unit_to_market_flow('storage_one', 25.0)
    p.add_market_to_unit_flow('storage_one', 25.0)
    p.add_storage('storage_one', mwh=50.0, initial_mwh=0.0, output_capacity=25.0, input_capacity=25.0,
                  output_efficiency=1.0, input_efficiency=1.0)
    p.set_unit_fcas_region('storage_one', 'raise_6_second', 'nsw')
    p.add_contingency_service_to_output('storage_one', 'raise_6_second', 25.0)
    p.add_contingency_service_to_input('storage_one', 'raise_6_second', 25.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_6_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0, 1],
        'nsw-energy-dispatch': [-25.0, 25.0],
        'nsw-raise_6_second-dispatch': [50.0, 0.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)
    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-raise_60_second-dispatch': [25.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 45.0)
    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-45.0],
        'nsw-raise_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 50.0)
    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-50.0],
        'nsw-raise_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 15.0)
    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-15.0],
        'nsw-raise_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)
    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-raise_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 35.0)
    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-35.0],
        'nsw-raise_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 0.0)
    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-raise_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 5.0)
    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [5.0],
        'nsw-raise_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 30.0)
    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [30.0],
        'nsw-raise_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 15.0)
    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [15.0],
        'nsw-raise_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-raise_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 35.0)
    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [35.0],
        'nsw-raise_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 0.0)
    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-lower_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 10.0)
    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-10.0],
        'nsw-lower_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 30.0)
    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-30.0],
        'nsw-lower_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 15.0)
    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-15.0],
        'nsw-lower_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)
    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-lower_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 35.0)
    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-35.0],
        'nsw-lower_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 45.0)
    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [45.0],
        'nsw-lower_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 50.0)
    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [50.0],
        'nsw-lower_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 15.0)
    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [15.0],
        'nsw-lower_60_second-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0, fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_60_second-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-raise_regulation-dispatch': [25.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 45.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-45.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 50.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-50.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 15.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=60.0,
                                      fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-15.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=60.0,
                                      fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 35.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=80.0,
                                      fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-35.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 0.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 5.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [5.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 30.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [30.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 15.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=60.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [15.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=60.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 35.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=60.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [35.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 0.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 10.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-10.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 30.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-30.0],
        'nsw-lower_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 15.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=60.0,
                                      fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-15.0],
        'nsw-lower_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=80.0,
                                      fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 35.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=80.0,
                                      fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-35.0],
        'nsw-lower_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 45.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [45.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 50.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=60.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [50.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_explicit_trapezium_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 15.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=60.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [15.0],
        'nsw-lower_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_explicit_trapezium_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=60.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_explicit_trapezium_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 35.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')

    fcas_trapezium = {'enablement_min': 10,
                      'low_breakpoint': 20,
                      'high_breakpoint': 30,
                      'enablement_max': 40}

    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=60.0,
                                       fcas_trapezium=fcas_trapezium)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [35.0],
        'nsw-lower_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_and_raise_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)

    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-raise_60_second-dispatch': [24.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_raise_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 45.0)

    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-45.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_and_raise_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 50.0)

    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-50.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_and_raise_regulation_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 0.0)

    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_and_raise_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 5.0)

    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [5.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_and_raise_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 30.0)

    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [30.0],
        'nsw-raise_60_second-dispatch': [19.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_and_lower_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 25.0)

    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-25.0],
        'nsw-raise_60_second-dispatch': [25.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_lower_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 45.0)

    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-45.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_contingency_and_lower_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 10,
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 50.0)

    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-49.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_and_lower_regulation_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 0.0)

    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [0.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_and_lower_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 5.0)

    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [5.0],
        'nsw-raise_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_contingency_and_lower_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-raise_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 30.0)

    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'raise_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'raise_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [30.0],
        'nsw-raise_60_second-dispatch': [20.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_and_raise_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 0.0)

    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [0.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_raise_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 5.0)

    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-5.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_and_raise_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 30.0)

    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-30.0],
        'nsw-lower_60_second-dispatch': [20.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_and_raise_regulation_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)

    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_60_second-dispatch': [20.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_and_raise_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 45.0)

    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [45.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_and_raise_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 2,
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 50.0)

    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [49.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-raise_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_and_lower_reg_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 0.0)

    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-0.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_lower_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 5.0)

    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-5.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_contingency_and_lower_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 10,
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw')
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 30.0)

    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('load_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_input('load_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-30.0],
        'nsw-lower_60_second-dispatch': [19.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_and_lower_regulation_joint_capacity_con_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)

    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_60_second-dispatch': [19.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_and_lower_regulation_joint_capacity_con_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 45.0)

    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [45.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_contingency_and_lower_regulation_joint_capacity_con_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-lower_60_second': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_60_second-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw')
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 50.0)

    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=1.0, ramp_rate=60.0)

    p.set_unit_fcas_region('gen_one', 'lower_60_second', 'nsw')
    p.add_contingency_service_to_output('gen_one', 'lower_60_second', availability=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_60_second')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [50.0],
        'nsw-lower_60_second-dispatch': [40.0],
        'nsw-lower_regulation-dispatch': [1.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_ramping_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw', initial_mw=50.0)
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 30.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-30.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_ramping_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw', initial_mw=50.0)
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 45.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-45.0],
        'nsw-raise_regulation-dispatch': [35.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_raise_reg_joint_capacity_con_ramping_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw', initial_mw=50.0)
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 50.0)
    p.set_unit_fcas_region('load_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'raise_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-50.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_ramping_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw', initial_mw=0.0)
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 0.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-raise_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_ramping_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw', initial_mw=0.0)
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 5.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [5.0],
        'nsw-raise_regulation-dispatch': [35.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_raise_reg_joint_capacity_con_ramping_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-raise_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-raise_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw', initial_mw=0.0)
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'raise_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'raise_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'raise_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-raise_regulation-dispatch': [20.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_ramping_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw', initial_mw=0.0)
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 0.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [0.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_ramping_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw', initial_mw=0.0)
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 10.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-10.0],
        'nsw-lower_regulation-dispatch': [30.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_load_energy_and_lower_reg_joint_capacity_con_ramping_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': -1 * np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('load_one', 'nsw', initial_mw=0.0)
    p.add_market_to_unit_flow('load_one', 50.0)
    p.add_load('load_one', 30.0)
    p.set_unit_fcas_region('load_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_input('load_one', 'lower_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [-30.0],
        'nsw-lower_regulation-dispatch': [10.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_ramping_lower_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw', initial_mw=50.0)
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 20.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [20.0],
        'nsw-lower_regulation-dispatch': [10.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_ramping_plateau():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101),
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw', initial_mw=50.0)
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 45.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [45.0],
        'nsw-lower_regulation-dispatch': [35.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)


def test_generator_energy_and_lower_reg_joint_capacity_con_ramping_upper_slope():
    historical_data = pd.DataFrame({
        'interval': np.linspace(0, 100, num=101).astype(int),
        'nsw-energy': np.linspace(0, 500, num=101),
        'nsw-lower_regulation': np.linspace(0, 500, num=101) * 0.1,
        'nsw-demand': np.linspace(0, 500, num=101),
        'nsw-energy-fleet-dispatch': np.zeros(101),
        'nsw-lower_regulation-fleet-dispatch': np.zeros(101),
    })

    forward_data = pd.DataFrame({
        'interval': [0],
        'nsw-demand': [250]})

    p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
                                train_pct=1.0, demand_delta_steps=100)

    p.add_unit('gen_one', 'nsw', initial_mw=50.0)
    p.add_unit_to_market_flow('gen_one', 50.0)
    p.add_generator('gen_one', 50.0)
    p.set_unit_fcas_region('gen_one', 'lower_regulation', 'nsw')
    p.add_regulation_service_to_output('gen_one', 'lower_regulation', availability=40.0, ramp_rate=40.0)

    p.add_regional_market('nsw', 'energy')
    p.add_regional_market('nsw', 'lower_regulation')

    p.optimise()

    dispatch = p.get_dispatch()

    expect_dispatch = pd.DataFrame({
        'interval': [0],
        'nsw-energy-dispatch': [50.0],
        'nsw-lower_regulation-dispatch': [40.0]
    })

    assert_frame_equal(expect_dispatch, dispatch)
# def test_convergence_across_strongly_linked_energy_markets():
#     """One energy storage unit in each market, 3 dispatch intervals"""
#
#     historical_data = pd.DataFrame({
#         'interval': np.linspace(0, 100, num=101).astype(int),
#         'nsw-demand': np.linspace(0, 500, num=101),
#         'nsw-energy-fleet-dispatch': np.zeros(101),
#         'vic-demand': np.linspace(0, 500, num=101),
#         'vic-energy-fleet-dispatch': np.zeros(101)
#     })
#
#     historical_data['nsw-energy'] = historical_data['nsw-demand'] + historical_data['vic-demand']
#     historical_data['vic-energy'] = historical_data['nsw-demand'] + historical_data['vic-demand']
#
#     forward_data = pd.DataFrame({
#         'interval': [0, 1, 2, 3, 4],
#         'nsw-demand': [100, 400, 400, 400, 400],
#         'vic-demand': [100, 400, 400, 400, 400]})
#
#     p = planner.DispatchPlanner(dispatch_interval=60, historical_data=historical_data, forward_data=forward_data,
#                                 train_pct=1.0, demand_delta_steps=100)
#
#     p.add_unit('storage_one', 'nsw')
#     p.add_unit_to_market_flow('storage_one', 50.0)
#     p.add_market_to_unit_flow('storage_one', 50.0)
#     p.add_storage('storage_one', mwh=50.0, initial_mwh=0.0, output_capacity=50.0, input_capacity=50.0,
#                   output_efficiency=1.0, input_efficiency=1.0)
#
#     p.add_regional_market('nsw', 'energy')
#
#     p.add_unit('storage_two', 'vic')
#     p.add_unit_to_market_flow('storage_two', 50.0)
#     p.add_market_to_unit_flow('storage_two', 50.0)
#     p.add_storage('storage_two', mwh=50.0, initial_mwh=0.0, output_capacity=50.0, input_capacity=50.0,
#                   output_efficiency=1.0, input_efficiency=1.0)
#
#     p.add_regional_market('vic', 'energy')
#
#     p.cross_market_optimise()
#
#     dispatch_nsw = p.get_unit_dispatch('storage_one')
#     dispatch_vic = p.get_unit_dispatch('storage_two')
#
#     expect_dispatch = pd.DataFrame({
#         'interval': [0, 1, 2, 3, 4],
#         'net_dispatch': [-50.0, 12.0, 12.0, 13.0, 13.0]
#     })
#
#     assert_frame_equal(expect_dispatch, dispatch_nsw)
#     assert_frame_equal(expect_dispatch, dispatch_vic)
