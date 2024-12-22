import pandas as pd
from pandas._testing import assert_frame_equal
from nempy.spot_market_backend import market_constraints


def test_energy():
    demand = pd.DataFrame({
        'region': ['X', 'Y'],
        'demand': [16.0, 23.0],
    })
    expected_rhs = pd.DataFrame({
        'region': ['X', 'Y'],
        'constraint_id': [0, 1],
        'type': ['=', '='],
        'rhs': [16.0, 23.0],
    })
    expected_variable_map = pd.DataFrame({
        'constraint_id': [0, 1],
        'region': ['X', 'Y'],
        'service': ['energy', 'energy'],
        'coefficient': [1.0, 1.0]
    })
    output_rhs, output_variable_map = market_constraints.energy(demand, next_constraint_id=0)
    expected_rhs.index = list(expected_rhs.index)
    assert_frame_equal(output_rhs, expected_rhs)
    assert_frame_equal(output_variable_map, expected_variable_map)
