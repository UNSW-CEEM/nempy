import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from nempy import markets, time_sequential


def prior_dispatch():
    return pd.DataFrame({
        'unit': ['B', 'B', 'B'],
        'dispatch_type': ['generator', 'load', 'generator'],
        'service': ['energy', 'energy', 'raise_reg'],
        'dispatch': [4.0, 1.0, 9.0],
    })


def ramp_rates():
    return pd.DataFrame({
        'unit': ['B', 'B', 'C'],
        'dispatch_type': ['generator', 'load', 'generator'],
        'initial_output': [100.0, 100.0, 100.0],
        'ramp_up_rate': [12.0, 24.0, np.nan],
        'ramp_down_rate': [24.0, 12.0, 60.0],
    })


def test_full_key_and_authoritative_dispatch():
    prior, rates = prior_dispatch(), ramp_rates()
    before = prior.copy(deep=True), rates.copy(deep=True)
    result = time_sequential.construct_ramp_rate_parameters(prior, rates)
    assert len(result) == 3
    assert not result.duplicated(['unit', 'dispatch_type']).any()
    assert set(result.columns) == set(rates.columns)
    assert result['initial_output'].tolist() == [3.0, 3.0, 0.0]
    assert result['ramp_up_rate'].iloc[:2].tolist() == [12.0, 24.0]
    assert pd.isna(result['ramp_up_rate'].iloc[2])
    assert_frame_equal(prior, before[0])
    assert_frame_equal(rates, before[1])


def test_seed_full_key_and_inner_join():
    prior = prior_dispatch().query("service == 'energy'").drop(columns='service')
    prior = prior.rename(columns={'dispatch': 'initial_output'})
    prior['initial_output'] = 3.0
    result = time_sequential.create_seed_ramp_rate_parameters(prior, ramp_rates())
    assert len(result) == 2
    assert result['dispatch_type'].tolist() == ['generator', 'load']
    assert result['initial_output'].tolist() == [3.0, 3.0]


@pytest.mark.parametrize('seed', [False, True])
@pytest.mark.parametrize('duplicate_side', ['prior', 'rates'])
def test_duplicate_keys_rejected(seed, duplicate_side):
    prior = prior_dispatch().query("service == 'energy'").copy()
    rates = ramp_rates()
    if duplicate_side == 'prior':
        prior = pd.concat([prior, prior.iloc[:1]], ignore_index=True)
    else:
        rates = pd.concat([rates, rates.iloc[:1]], ignore_index=True)
    if seed:
        prior = prior.drop(columns='service').rename(columns={'dispatch': 'initial_output'})
    helper = (time_sequential.create_seed_ramp_rate_parameters if seed
              else time_sequential.construct_ramp_rate_parameters)
    with pytest.raises(ValueError, match='duplicate'):
        helper(prior, rates)


@pytest.mark.parametrize('seed', [False, True])
@pytest.mark.parametrize('missing_direction', ['prior', 'rates'])
def test_ambiguous_direction_rejected(seed, missing_direction):
    prior = prior_dispatch().query("service == 'energy'").copy()
    rates = ramp_rates()
    if missing_direction == 'prior':
        prior = prior.iloc[:1].drop(columns='dispatch_type')
    else:
        rates = rates.iloc[[0, 2]].drop(columns='dispatch_type')
    if seed:
        prior = prior.drop(columns='service').rename(columns={'dispatch': 'initial_output'})
    helper = (time_sequential.create_seed_ramp_rate_parameters if seed
              else time_sequential.construct_ramp_rate_parameters)
    if seed and missing_direction == 'prior':
        # Historical initial output has unit scope, unlike directional dispatch.
        result = helper(prior, rates)
        assert result['initial_output'].tolist() == [4.0, 4.0]
        assert result['dispatch_type'].tolist() == ['generator', 'load']
        return
    with pytest.raises(ValueError, match='dispatch_type'):
        helper(prior, rates)


@pytest.mark.parametrize('directions', ['neither', 'prior', 'rates', 'both'])
@pytest.mark.parametrize('service', [False, True])
def test_legacy_and_unambiguous_direction(directions, service):
    prior = pd.DataFrame({'unit': ['A'], 'dispatch': [45.0]})
    rates = pd.DataFrame({'unit': ['A'], 'ramp_up_rate': [600.0], 'ramp_down_rate': [600.0]})
    if directions in ['prior', 'both']:
        prior['dispatch_type'] = 'load'
    if directions in ['rates', 'both']:
        rates['dispatch_type'] = 'load'
    if service:
        prior['service'] = 'energy'
    result = time_sequential.construct_ramp_rate_parameters(prior, rates)
    assert result['initial_output'].tolist() == [45.0]
    if directions != 'neither':
        assert result['dispatch_type'].tolist() == ['load']


def test_missing_prior_does_not_fill_missing_ramp_or_existing_nan():
    prior = pd.DataFrame({'unit': ['A'], 'service': ['energy'], 'dispatch': [np.nan]})
    rates = pd.DataFrame({'unit': ['A', 'B'], 'ramp_up_rate': [12.0, np.nan],
                          'ramp_down_rate': [24.0, 60.0]})
    result = time_sequential.construct_ramp_rate_parameters(prior, rates)
    assert pd.isna(result.loc[0, 'initial_output'])
    assert result.loc[1, 'initial_output'] == 0.0
    assert pd.isna(result.loc[1, 'ramp_up_rate'])


@pytest.mark.parametrize('first_target,second_demand,expected', [
    (4.0, 6.0, 5.0), (-4.0, -6.0, -5.0),
    (0.5, -2.0, -0.5), (-0.5, 2.0, 0.5),
])
def test_two_interval_bidirectional_market(first_target, second_demand, expected):
    identity = {'unit': ['B', 'B', 'G', 'L'],
                'dispatch_type': ['generator', 'load', 'generator', 'load']}
    rates = pd.DataFrame({**identity, 'ramp_up_rate': [1200.0] * 4,
                          'ramp_down_rate': [1200.0] * 4})
    seed = pd.DataFrame({'unit': ['B', 'G', 'L'], 'initial_output': [0.0] * 3})
    initial = time_sequential.create_seed_ramp_rate_parameters(seed, rates)

    def dispatch(demand, ramp_details):
        market = markets.SpotMarket(['R'], pd.DataFrame({**identity, 'region': ['R'] * 4}))
        market.set_unit_volume_bids(pd.DataFrame({**identity, '1': [10.0, 10.0, 100.0, 100.0]}))
        market.set_unit_price_bids(pd.DataFrame({**identity, '1': [1.0, -1.0, 10.0, -10.0]}))
        market.set_unit_ramp_rate_constraints(ramp_details)
        market.set_demand_constraints(pd.DataFrame({'region': ['R'], 'demand': [demand]}))
        market.dispatch()
        return market

    first = dispatch(first_target, initial)
    rates.loc[rates['unit'] == 'B', ['ramp_up_rate', 'ramp_down_rate']] = 12.0
    rates['initial_output'] = 100.0  # stale historical output in the next bid table
    next_rates = time_sequential.construct_ramp_rate_parameters(first.get_unit_dispatch(), rates)
    assert next_rates.query("unit == 'B'")['initial_output'].tolist() == pytest.approx([first_target] * 2)
    assert not next_rates.duplicated(['unit', 'dispatch_type']).any()
    second = dispatch(second_demand, next_rates)
    bdu = second.get_unit_dispatch().query("unit == 'B'").set_index('dispatch_type')['dispatch']
    assert bdu['generator'] - bdu['load'] == pytest.approx(expected)
    assert second._constraints_rhs_and_type['bidirectional_ramp_up']['rhs'].iloc[0] == pytest.approx(first_target + 1.0)
    assert second._constraints_rhs_and_type['bidirectional_ramp_down']['rhs'].iloc[0] == pytest.approx(first_target - 1.0)


@pytest.mark.parametrize('seed', [False, True])
def test_empty_prior_preserves_join_contract(seed):
    prior = pd.DataFrame({'unit': pd.Series(dtype=str), 'dispatch': pd.Series(dtype=float)})
    rates = pd.DataFrame({'unit': ['A'], 'ramp_up_rate': [12.0], 'ramp_down_rate': [24.0]})
    if seed:
        result = time_sequential.create_seed_ramp_rate_parameters(
            prior.rename(columns={'dispatch': 'initial_output'}), rates)
        assert result.empty
    else:
        result = time_sequential.construct_ramp_rate_parameters(prior, rates)
        assert result['initial_output'].tolist() == [0.0]


@pytest.mark.parametrize('seed', [False, True])
def test_missing_direction_value_is_not_inferred(seed):
    prior = prior_dispatch().query("service == 'energy'").copy()
    prior.loc[0, 'dispatch_type'] = None
    if seed:
        prior = prior.drop(columns='service').rename(columns={'dispatch': 'initial_output'})
    helper = (time_sequential.create_seed_ramp_rate_parameters if seed
              else time_sequential.construct_ramp_rate_parameters)
    with pytest.raises(ValueError, match='missing dispatch identity'):
        helper(prior, ramp_rates())
