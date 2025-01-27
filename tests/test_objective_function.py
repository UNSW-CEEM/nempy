import pandas as pd
from pandas._testing import assert_frame_equal
from nempy.spot_market_backend import objective_function


def test_energy():
    bidding_ids = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'dispatch_type': ['generator', 'generator', 'load', 'load'],
        'service': ['energy', 'energy', 'energy', 'energy'],
        'variable_id': [1, 2, 3, 4]
    })
    price_bids = pd.DataFrame({
        'unit': ['A', 'B'],
        'dispatch_type': ['generator', 'load'],
        '1': [16.0, 23.0],
        '2': [17.0, 18.0]
    })
    output = objective_function.bids(bidding_ids, price_bids)
    expected = pd.DataFrame({
        'unit': ['A', 'A', 'B', 'B'],
        'capacity_band': ['1', '2', '1', '2'],
        'dispatch_type': ['generator', 'generator', 'load', 'load'],
        'service': ['energy', 'energy', 'energy', 'energy'],
        'variable_id': [1, 2, 3, 4],
        'cost': [16.0, 17.0, -23.0, -18.0],
    })
    expected.index = list(expected.index)
    assert_frame_equal(output, expected)
