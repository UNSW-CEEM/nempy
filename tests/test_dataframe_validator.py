import numpy as np
import pandas as pd
import pytest

from nempy.spot_market_backend import dataframe_validator as dv


def _bid_schema():
    schema = dv.DataFrameSchema(name='price_bids', primary_keys=['unit'],
                                row_monatonic_increasing=['1', '2', '3'])
    schema.add_column(dv.SeriesSchema(name='unit', data_type=str))
    for band in ['1', '2', '3']:
        schema.add_column(dv.SeriesSchema(name=band, data_type=np.float64))
    return schema


def test_row_monatonic_increasing_accepts_increasing_bids():
    schema = _bid_schema()
    bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 100.0],
        '2': [100.0, 130.0],
        '3': [100.0, 150.0]})

    schema._check_row_monatonic_increasing(bids)


def test_row_monatonic_increasing_rejects_decreasing_bids():
    schema = _bid_schema()
    bids = pd.DataFrame({
        'unit': ['A', 'B'],
        '1': [50.0, 100.0],
        '2': [100.0, 130.0],
        '3': [100.0, 120.0]})  # unit B decreases across bands

    with pytest.raises(dv.BidsNotMonotonicIncreasing):
        schema._check_row_monatonic_increasing(bids)


def test_validate_does_not_run_the_monotonicity_check():
    """Guards the documented gap: validate() never calls the monotonicity check.

    This asserts current behaviour, not desired behaviour. Non-monotonic price
    bids are accepted even though a schema declares row_monatonic_increasing. If
    the check is ever wired into validate(), this test should fail and be
    replaced deliberately, rather than the behaviour changing unnoticed.
    """
    schema = _bid_schema()
    non_monotonic = pd.DataFrame({
        'unit': ['A'],
        '1': [100.0],
        '2': [50.0],
        '3': [10.0]})

    schema.validate(non_monotonic)


def test_validate_still_catches_repeated_rows():
    schema = _bid_schema()
    repeated = pd.DataFrame({
        'unit': ['A', 'A'],
        '1': [50.0, 50.0],
        '2': [100.0, 100.0],
        '3': [150.0, 150.0]})

    with pytest.raises(dv.RepeatedRowError):
        schema.validate(repeated)
