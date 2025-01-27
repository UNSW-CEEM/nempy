import pandas as pd
from pandas._testing import assert_frame_equal
from nempy.spot_market_backend import variable_ids


def test_energy_one_unit():
    bids = pd.DataFrame({
        'unit': ['A'],
        '1': [1.0]
    })
    unit_info = pd.DataFrame({
        'unit': ['A'],
        'region': ['X'],
        'dispatch_type': ['generator']
    })
    bidirectional_units = []
    next_constraint_id = 4
    output_vars, unit_level_constraint_map, region_level_constraint_map = \
        variable_ids.bids(bids, unit_info, next_constraint_id, bidirectional_units)
    expected_vars = pd.DataFrame({
        'unit': ['A'],
        'capacity_band': ['1'],
        'service': ['energy'],
        'dispatch_type': ['generator'],
        'variable_id': [4],
        'lower_bound': [0.0],
        'upper_bound': [1.0],
        'type': ['continuous']
    })
    expected_unit_constraint_map = pd.DataFrame({
        'variable_id': [4],
        'unit': ['A'],
        'service': ['energy'],
        'dispatch_type': ['generator'],
        'coefficient': [1.0]
    })
    expected_regional_constraint_map = pd.DataFrame({
        'variable_id': [4],
        'region': ['X'],
        'service': ['energy'],
        'dispatch_type': ['generator'],
        'coefficient': [1.0]
    })
    assert_frame_equal(output_vars, expected_vars)
    assert_frame_equal(unit_level_constraint_map, expected_unit_constraint_map)
    assert_frame_equal(region_level_constraint_map, expected_regional_constraint_map)


def test_energy_two_units():
    bids = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'load'],
        '1': [1.0, 5.0],
        '2': [6.0, 7.0]
    })
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['X', 'Y'],
        'dispatch_type': ['generator', 'load']
    })
    bidirectional_units = []
    next_constraint_id = 4
    output_vars, unit_level_constraint_map, region_level_constraint_map = \
        variable_ids.bids(bids, unit_info, next_constraint_id, bidirectional_units)
    expected_vars = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'service': ['energy', 'energy', 'energy', 'energy'],
        'dispatch_type': ['generator', 'generator', 'load', 'load'],
        'variable_id': [4, 5, 6, 7],
        'lower_bound': [0.0, 0.0, 0.0, 0.0],
        'upper_bound': [1.0, 6.0, 5.0, 7.0],
        'type': ['continuous', 'continuous', 'continuous', 'continuous']
    })
    expected_unit_constraint_map = pd.DataFrame({
        'variable_id': [4, 5, 6, 7],
        'unit': ['A', 'A', 'B', 'B'],
        'service': ['energy', 'energy', 'energy', 'energy'],
        'dispatch_type': ['generator', 'generator', 'load', 'load'],
        'coefficient': [1.0, 1.0, 1.0, 1.0]
    })
    expected_region_constraint_map = pd.DataFrame({
        'variable_id': [4, 5, 6, 7],
        'region': ['X', 'X', 'Y', 'Y'],
        'service': ['energy', 'energy', 'energy', 'energy'],
        'dispatch_type': ['generator', 'generator', 'load', 'load'],
        'coefficient': [1.0, 1.0, -1.0, -1.0]
    })
    assert_frame_equal(output_vars, expected_vars)
    assert_frame_equal(unit_level_constraint_map, expected_unit_constraint_map)
    assert_frame_equal(region_level_constraint_map, expected_region_constraint_map)
