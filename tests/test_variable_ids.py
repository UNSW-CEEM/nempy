import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import variable_ids


def test_energy_one_unit():
    bids = pd.DataFrame({
        'unit': ['A'],
        '1': [1.0]
    })
    unit_info = pd.DataFrame({
        'unit': ['A'],
        'region': ['X']
    })
    next_constraint_id = 4
    output_vars, output_constraint_map = variable_ids.energy(bids, unit_info, next_constraint_id)
    expected_vars = pd.DataFrame({
        'unit': ['A'],
        'capacity_band': ['1'],
        'variable_id': [4],
        'lower_bound': [0.0],
        'upper_bound': [1.0],
        'type': ['continuous']
    })
    expected_constraint_map = pd.DataFrame({
        'variable_id': [4],
        'unit': ['A'],
        'region': ['X'],
        'service': ['energy'],
        'coefficient': [1.0]
    })
    assert_frame_equal(output_vars, expected_vars)
    assert_frame_equal(output_constraint_map, expected_constraint_map)


def test_energy_one_unit_drop_zero_bids():
    bids = pd.DataFrame({
        'unit': ['A'],
        '1': [1.0],
        '2': [0.0]
    })
    unit_info = pd.DataFrame({
        'unit': ['A'],
        'region': ['X']
    })
    next_constraint_id = 4
    output_vars, output_constraint_map = variable_ids.energy(bids, unit_info, next_constraint_id)
    expected_vars = pd.DataFrame({
        'unit': ['A'],
        'capacity_band': ['1'],
        'variable_id': [4],
        'lower_bound': [0.0],
        'upper_bound': [1.0],
        'type': ['continuous']
    })
    expected_constraint_map = pd.DataFrame({
        'variable_id': [4],
        'unit': ['A'],
        'region': ['X'],
        'service': ['energy'],
        'coefficient': [1.0]
    })
    assert_frame_equal(output_vars, expected_vars)
    assert_frame_equal(output_constraint_map, expected_constraint_map)


def test_energy_two_units():
    bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [1.0, 5.0],
        '2': [6.0, 7.0]
    })
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['X', 'Y']
    })
    next_constraint_id = 4
    output_vars, output_constraint_map = variable_ids.energy(bids, unit_info, next_constraint_id)
    expected_vars = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'variable_id': [4, 5, 6, 7],
        'lower_bound': [0.0, 0.0, 0.0, 0.0],
        'upper_bound': [1.0, 6.0, 5.0, 7.0],
        'type': ['continuous', 'continuous', 'continuous', 'continuous']
    })
    expected_constraint_map = pd.DataFrame({
        'variable_id': [4, 5, 6, 7],
        'unit': ['A', 'A', 'B', 'B'],
        'region': ['X', 'X', 'Y', 'Y'],
        'service': ['energy', 'energy', 'energy', 'energy'],
        'coefficient': [1.0, 1.0, 1.0, 1.0]
    })
    assert_frame_equal(output_vars, expected_vars)
    assert_frame_equal(output_constraint_map, expected_constraint_map)