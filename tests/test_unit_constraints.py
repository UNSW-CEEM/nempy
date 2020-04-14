import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import unit_constraints


def test_create_constraints():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'variable_id': [1, 2, 3, 4]
    })
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'upper': [16, 23]
    })
    next_constraint_id = 4
    rhs_col = 'upper'
    direction = '<='
    output = unit_constraints.create_constraints(bidding_ids, unit_limit, next_constraint_id, rhs_col, direction)
    expected = pd.DataFrame({
        'variable_id': [1, 2, 3, 4],
        'constraint_id': [4, 4, 5, 5],
        'unit': ['A', 'A', 'B', 'B'],
        'rhs': [16, 16, 23, 23],
        'coefficient': [1, 1, 1, 1],
        'type': ['<=', '<=', '<=', '<=']
    })
    assert_frame_equal(output, expected)


def test_one_unit_create_constraints():
    bidding_ids = pd.DataFrame({
        'unit': ['A'],
        'variable_id': [1]
    })
    unit_limit = pd.DataFrame({
        'unit': ['A'],
        'upper': [16]
    })
    next_constraint_id = 4
    rhs_col = 'upper'
    direction = '<='
    output = unit_constraints.create_constraints(bidding_ids, unit_limit, next_constraint_id, rhs_col, direction)
    expected = pd.DataFrame({
        'variable_id': [1],
        'constraint_id': [4],
        'unit': ['A'],
        'rhs': [16],
        'coefficient': [1],
        'type': ['<=']
    })
    assert_frame_equal(output, expected)


def test_ramp_down():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'variable_id': [1, 2, 3, 4]
    })
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'initial_output': [16, 23],
        'ramp_down_rate': [12, 36]
    })
    next_constraint_id = 4
    dispatch_interval = 5
    output_lhs, output_rhs = unit_constraints.ramp_down(bidding_ids, unit_limit, next_constraint_id, dispatch_interval)
    expected_lhs = pd.DataFrame({
        'variable_id': [1, 2, 3, 4],
        'constraint_id': [4, 4, 5, 5],
        'coefficient': [1, 1, 1, 1],
    })
    expected_rhs = pd.DataFrame({
        'constraint_id': [4, 5],
        'type': ['>=', '>='],
        'rhs': [15.0, 20.0]
    })
    assert_frame_equal(output_lhs, expected_lhs)
    assert_frame_equal(output_rhs.reset_index(drop=True), expected_rhs)


def test_ramp_up():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'variable_id': [1, 2, 3, 4]
    })
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'initial_output': [16, 23],
        'ramp_up_rate': [12, 36]
    })
    next_constraint_id = 4
    dispatch_interval = 5
    output_lhs, output_rhs = unit_constraints.ramp_up(bidding_ids, unit_limit, next_constraint_id, dispatch_interval)
    expected_lhs = pd.DataFrame({
        'variable_id': [1, 2, 3, 4],
        'constraint_id': [4, 4, 5, 5],
        'coefficient': [1, 1, 1, 1],
    })
    expected_rhs = pd.DataFrame({
        'constraint_id': [4, 5],
        'type': ['<=', '<='],
        'rhs': [17.0, 26.0]
    })
    assert_frame_equal(output_lhs, expected_lhs)
    assert_frame_equal(output_rhs.reset_index(drop=True), expected_rhs)


def test_capacity():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'variable_id': [1, 2, 3, 4]
    })
    unit_limit = pd.DataFrame({
        'unit': ['A', 'B'],
        'capacity': [16.0, 23.0]
    })
    next_constraint_id = 4
    dispatch_interval = 5
    output_lhs, output_rhs = unit_constraints.capacity(bidding_ids, unit_limit, next_constraint_id)
    expected_lhs = pd.DataFrame({
        'variable_id': [1, 2, 3, 4],
        'constraint_id': [4, 4, 5, 5],
        'coefficient': [1, 1, 1, 1],
    })
    expected_rhs = pd.DataFrame({
        'constraint_id': [4, 5],
        'type': ['<=', '<='],
        'rhs': [16.0, 23.0]
    })
    assert_frame_equal(output_lhs, expected_lhs)
    assert_frame_equal(output_rhs.reset_index(drop=True), expected_rhs)
