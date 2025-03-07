import pandas as pd
from pandas._testing import assert_frame_equal
from nempy.spot_market_backend import unit_constraints, ramp_rate_processing


def test_create_constraints():
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'upper': [16.0, 23.0]
    })
    next_constraint_id = 4
    rhs_col = 'upper'
    direction = '<='
    output_rhs, output_variable_map = unit_constraints.create_constraints(unit_limit, next_constraint_id, rhs_col,
                                                                          direction)
    expected_rhs = pd.DataFrame({
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'constraint_id': [4, 5],
        'type': ['<=', '<='],
        'rhs': [16.0, 23.0]
    })
    expected_variable_map = pd.DataFrame({
        'constraint_id': [4, 5],
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'coefficient': [1.0, 1.0]
    })
    assert_frame_equal(output_rhs, expected_rhs)
    assert_frame_equal(output_variable_map, expected_variable_map)


def test_one_unit_create_constraints():
    unit_limit = pd.DataFrame({
        'unit': ['A'],
        'upper': [16.0]
    })
    next_constraint_id = 4
    rhs_col = 'upper'
    direction = '<='
    output_rhs, output_variable_map = unit_constraints.create_constraints(unit_limit, next_constraint_id, rhs_col,
                                                                          direction)
    expected_rhs = pd.DataFrame({
        'unit': ['A'],
        'service': ['energy'],
        'dispatch_type': ['generator'],
        'constraint_id': [4],
        'type': ['<='],
        'rhs': [16.0],

    })
    expected_variable_map = pd.DataFrame({
        'constraint_id': [4],
        'unit': ['A'],
        'service': ['energy'],
        'dispatch_type': ['generator'],
        'coefficient': [1.0],
    })
    assert_frame_equal(output_rhs, expected_rhs)
    assert_frame_equal(output_variable_map, expected_variable_map)


def test_ramp_down():
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'initial_output': [16.0, 23.0],
        'ramp_down_rate': [12.0, 36.0]
    })
    next_constraint_id = 4
    dispatch_interval = 5
    output_rhs, output_variable_map = unit_constraints.ramp_down(unit_limit, next_constraint_id, dispatch_interval)
    expected_rhs = pd.DataFrame({
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'constraint_id': [4, 5],
        'type': ['>=', '>='],
        'rhs': [15.0, 20.0]
    })
    expected_variable_map = pd.DataFrame({
        'constraint_id': [4, 5],
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'coefficient': [1.0, 1.0],
    })
    assert_frame_equal(output_rhs.reset_index(drop=True), expected_rhs)
    assert_frame_equal(output_variable_map.reset_index(drop=True), expected_variable_map)


def test_ramp_up():
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'initial_output': [16, 23],
        'ramp_up_rate': [12, 36]
    })
    next_constraint_id = 4
    dispatch_interval = 5
    output_rhs, output_variable_map = unit_constraints.ramp_up(unit_limit, next_constraint_id, dispatch_interval)
    expected_rhs = pd.DataFrame({
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'constraint_id': [4, 5],
        'type': ['<=', '<='],
        'rhs': [17.0, 26.0],
    })
    expected_variable_map = pd.DataFrame({
        'constraint_id': [4, 5],
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'coefficient': [1.0, 1.0]
    })
    assert_frame_equal(output_rhs.reset_index(drop=True), expected_rhs)
    assert_frame_equal(output_variable_map.reset_index(drop=True), expected_variable_map)


def test_capacity():
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [16.0, 23.0]
    })
    next_constraint_id = 4
    bidirectional_units = []
    output_rhs, output_variable_map = unit_constraints.capacity(unit_limit, next_constraint_id, bidirectional_units)
    expected_rhs = pd.DataFrame({
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'constraint_id': [4, 5],
        'type': ['<=', '<='],
        'rhs': [16.0, 23.0],
    })
    expected_variable_map = pd.DataFrame({
        'constraint_id': [4, 5],
        'unit': ['A', 'B'],
        'service': ['energy', 'energy'],
        'dispatch_type': ['generator', 'generator'],
        'coefficient': [1.0, 1.0],
    })
    assert_frame_equal(output_rhs.reset_index(drop=True), expected_rhs)
    assert_frame_equal(output_variable_map.reset_index(drop=True), expected_variable_map)


def test_composite_ramp_ramp_rates_unchanged():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [0.0, 0.0],
        'ramp_up_rate': [100.0, 100.0],
        'ramp_down_rate': [100.0, 100.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [100.0],
        'ramp_up_rate': [100.0],
        'initial_output': [0.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)


def test_composite_ramp_ramp_rates_trapped_above_zero():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [10.0, 10.0],
        'ramp_up_rate': [1.0, 100.0],
        'ramp_down_rate': [1.0, 100.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [1.0],
        'ramp_up_rate': [1.0],
        'initial_output': [10.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)


def test_composite_ramp_ramp_rates_trapped_below_zero():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [-10.0, -10.0],
        'ramp_up_rate': [100.0, 1.0],
        'ramp_down_rate': [100.0, 1.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [1.0],
        'ramp_up_rate': [1.0],
        'initial_output': [-10.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)


def test_composite_ramp_ramp_rates_starts_below_zero():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [-10.0, -10.0],
        'ramp_up_rate': [100.0, 20.0],
        'ramp_down_rate': [100.0, 20.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [20.0],
        'ramp_up_rate': [60.0],
        'initial_output': [-10.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)


def test_composite_ramp_ramp_rates_starts_above_zero():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [10.0, 10.0],
        'ramp_up_rate': [20.0, 100.0],
        'ramp_down_rate': [20.0, 100.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [60.0],
        'ramp_up_rate': [20.0],
        'initial_output': [10.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)


def test_composite_ramp_ramp_rates_trapped_below_zero_no_ramp_up():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [-10.0, -10.0],
        'ramp_up_rate': [100.0, 10.0],
        'ramp_down_rate': [100.0, 0.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [10.0],
        'ramp_up_rate': [0.0],
        'initial_output': [-10.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)


def test_composite_ramp_ramp_rates_trapped_above_zero_no_ramp_down():
    ramp_rates = pd.DataFrame({
        'unit': ['A', 'A'],
        'dispatch_type': ["generator", "load"],
        'initial_output': [10.0, 10.0],
        'ramp_up_rate': [20.0, 100.0],
        'ramp_down_rate': [0.0, 100.0]
    })
    dispatch_interval = 60
    _, ramp_rates_out = ramp_rate_processing._calculate_composite_ramp_rates(
        ramp_rates, dispatch_interval, ["A"]
    )
    expected_ramp_rates_out = pd.DataFrame({
        'unit': ['A'],
        'ramp_down_rate': [0.0],
        'ramp_up_rate': [20.0],
        'initial_output': [10.0],
    })
    assert_frame_equal(ramp_rates_out.reset_index(drop=True), expected_ramp_rates_out)
