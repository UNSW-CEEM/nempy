import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import market_constraints


def test_energy():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'variable_id': [1, 2, 3, 4]
    })
    demand = pd.DataFrame({
        'region': ['X', 'Y'],
        'demand': [16.0, 23.0],
    })
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'region': ['X', 'Y']
    })
    output_lhs, output_rhs = market_constraints.energy(bidding_ids, demand, unit_info, next_constraint_id=0)
    expected_lhs = pd.DataFrame({
        'constraint_id': [0, 0, 1, 1],
        'variable_id': [1, 2, 3, 4],
        'coefficient': [1.0, 1.0, 1.0, 1.0]
    })
    expected_rhs = pd.DataFrame({
        'region': ['X', 'Y'],
        'constraint_id': [0, 1],
        'type': ['=', '='],
        'rhs': [16.0, 23.0]
    })
    expected_lhs.index = list(expected_lhs.index)
    expected_rhs.index = list(expected_rhs.index)
    assert_frame_equal(output_lhs, expected_lhs)
    assert_frame_equal(output_rhs, expected_rhs)