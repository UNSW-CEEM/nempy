import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import variable_ids


def test_energy_one_unit():
    bids = pd.DataFrame({
        'unit': ['A'],
        '1': [1]
    })
    next_constraint_id = 4
    output = variable_ids.energy(bids, next_constraint_id)
    expected = pd.DataFrame({
        'unit': ['A'],
        'capacity_band': ['1'],
        'upper_bound': [1],
        'variable_id': [4],
        'lower_bound': [0.0],
        'type': ['continuous'],
    })
    assert_frame_equal(output, expected)


def test_energy_one_unit_drop_zero_bids():
    bids = pd.DataFrame({
        'unit': ['A'],
        '1': [1],
        '2': [0]
    })
    next_constraint_id = 4
    output = variable_ids.energy(bids, next_constraint_id)
    expected = pd.DataFrame({
        'unit': ['A'],
        'capacity_band': ['1'],
        'upper_bound': [1],
        'variable_id': [4],
        'lower_bound': [0.0],
        'type': ['continuous'],
    })
    assert_frame_equal(output, expected)


def test_energy_two_units():
    bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [1, 5],
        '2': [6, 7]
    })
    next_constraint_id = 4
    output = variable_ids.energy(bids, next_constraint_id)
    expected = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'upper_bound': [1, 6, 5, 7],
        'variable_id': [4, 5, 6, 7],
        'lower_bound': [0.0, 0.0, 0.0, 0.0],
        'type': ['continuous', 'continuous', 'continuous','continuous'],
    })
    expected.index = list(expected.index)
    assert_frame_equal(output, expected)