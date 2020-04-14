import pandas as pd
from pandas._testing import assert_frame_equal
from nempy import objective_function


def test_energy():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'variable_id': [1, 2, 3, 4]
    })
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [16, 23],
        '2': [17, 18]
    })
    unit_info = pd.DataFrame({
        'unit': ['A', 'B'],
        'loss_factor': [0.85, 1.1]
    })
    output = objective_function.energy(bidding_ids, price_bids, unit_info)
    expected = pd.DataFrame({
        'variable_id': [1, 2, 3, 4],
        'cost': [16/0.85, 17/0.85, 23/1.1, 18/1.1]
    })
    expected.index = list(expected.index)
    assert_frame_equal(output, expected)