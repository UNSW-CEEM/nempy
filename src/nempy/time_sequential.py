import pandas as pd


def _merge_ramp_outputs(prior_output, ramp_rates, how, unit_level_seed=False):
    """Join unique dispatch identities without overwriting prior output."""
    keys = ['unit']
    if 'dispatch_type' in prior_output and 'dispatch_type' in ramp_rates:
        keys.append('dispatch_type')
    for name, frame in [('prior output', prior_output), ('ramp rates', ramp_rates)]:
        identity = ['unit', 'dispatch_type'] if 'dispatch_type' in frame else ['unit']
        if frame[identity].isna().any().any():
            raise ValueError(f'{name} contains missing dispatch identity values.')
        if frame.duplicated(identity).any():
            raise ValueError(f'{name} contains duplicate dispatch identities: {identity}.')
        if keys == ['unit'] and frame.duplicated('unit').any() and not (unit_level_seed and name == 'ramp rates'):
            raise ValueError('dispatch_type is required on both inputs for bidirectional units.')

    result = pd.merge(prior_output, ramp_rates.drop(columns='initial_output', errors='ignore'),
                      on=keys, how=how, validate='one_to_many' if unit_level_seed else 'one_to_one', indicator=True)
    if 'dispatch_type' in result and result['dispatch_type'].isna().any():
        raise ValueError('Cannot infer dispatch_type for a unit with no prior dispatch.')
    if how == 'right':
        result.loc[result['_merge'] == 'right_only', 'initial_output'] = 0.0
    return result.drop(columns='_merge')


def _set_bidirectional_initial_output(ramp_rates):
    """Composite ramps use signed net output on both bidirectional rows."""
    if 'dispatch_type' not in ramp_rates:
        return ramp_rates
    generator = ramp_rates[ramp_rates['dispatch_type'] == 'generator'].set_index('unit')['initial_output']
    load = ramp_rates[ramp_rates['dispatch_type'] == 'load'].set_index('unit')['initial_output']
    units = generator.index.intersection(load.index)
    net_output = generator.loc[units] - load.loc[units]
    mask = ramp_rates['unit'].isin(units)
    ramp_rates.loc[mask, 'initial_output'] = ramp_rates.loc[mask, 'unit'].map(net_output)
    return ramp_rates


def construct_ramp_rate_parameters(last_interval_dispatch, ramp_rates):
    """Combine dispatch and ramp rates into the ramp rate inputs compatible with the SpotMarket class.

    When present on both inputs, ``dispatch_type`` is part of the unit identity.
    If only one input has it, each unit must have a single unambiguous direction.
    Duplicate or missing identities raise ``ValueError``. Previous dispatch takes
    precedence over any ``initial_output`` in ``ramp_rates``; newly appearing
    units start at zero. Missing ramp values are preserved for input validation.
    For a bidirectional unit, both ramp rows receive generation minus consumption,
    as required by ``SpotMarket``'s composite ramp constraints. Single-direction
    loads retain positive consumption.

    Examples
    -------

    >>> last_interval_dispatch = pd.DataFrame({
    ... 'unit': ['A', 'A', 'B'],
    ... 'service': ['energy', 'raise_reg', 'energy'],
    ... 'dispatch': [45.0, 50.0, 88.0]})

    >>> ramp_rates = pd.DataFrame({
    ... 'unit': ['A', 'B', 'C'],
    ... 'ramp_up_rate': [600.0, 1200.0, 700.0],
    ... 'ramp_down_rate': [600.0, 1200.0, 700.0]})

    >>> construct_ramp_rate_parameters(last_interval_dispatch,
    ...                                ramp_rates)
      unit  initial_output  ramp_up_rate  ramp_down_rate
    0    A            45.0         600.0           600.0
    1    B            88.0        1200.0          1200.0
    2    C             0.0         700.0           700.0

    Parameters
    ----------
    last_interval_dispatch : pd.DataFrame

        ========  ================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        service   the service being provided, optional, \n
                  default 'energy', (as `str`)
        dispatch  the dispatch target from the previous dispatch \n
                  interval, in MW, (as `np.float64`)
        ========  ================================================

    ramp_rates : pd.DataFrame

        ================  ========================================
        Columns:          Description:
        unit              unique identifier for units, (as `str`) \n
        ramp_up_rate      the ramp up rate, in MW/h, \n
                          (as `np.float64`)
        ramp_down_rate    the ramp down rate, in MW/h, \n
                          (as `np.float64`)
        ================  ========================================

    Returns
    -------
    pd.DataFrame

        ================  ========================================
        Columns:          Description:
        unit              unique identifier for units, (as `str`) \n
        initial_output    the output/consumption of the unit at \n
                          the start of the dispatch interval, \n
                          in MW, (as `np.float64`)
        ramp_up_rate      the ramp up rate, in MW/h, \n
                          (as `np.float64`)
        ramp_down_rate    the ramp down rate, in MW/h, \n
                          (as `np.float64`)
        ================  ========================================


    """
    last_interval_energy_dispatch = last_interval_dispatch
    if 'service' in last_interval_dispatch:
        last_interval_energy_dispatch = last_interval_dispatch[last_interval_dispatch['service'] == 'energy']
    columns = ['unit', 'dispatch']
    if 'dispatch_type' in last_interval_energy_dispatch:
        columns.insert(1, 'dispatch_type')
    prior_output = last_interval_energy_dispatch.loc[:, columns].rename(columns={'dispatch': 'initial_output'})
    result = _merge_ramp_outputs(prior_output, ramp_rates, how='right')
    return _set_bidirectional_initial_output(result)


def create_seed_ramp_rate_parameters(historical_dispatch, as_bid_ramp_rates):
    """Combine historical dispatch and as bid ramp rates to get seed ramp rate parameters for a time sequential model.

    ``historical_dispatch`` supplies the authoritative ``initial_output``.
    The result contains only identities present in both inputs. ``dispatch_type``
    is part of the key when present on both inputs. Historical input without
    ``dispatch_type`` describes unit-level initial output (signed net output for
    a bidirectional unit), which is copied to each of that unit's ramp rows.
    Duplicate or missing identities raise ``ValueError``.

    Examples
    --------

    >>> historical_dispatch = pd.DataFrame({
    ... 'unit': ['A', 'B'],
    ... 'initial_output': [80.0, 100.0]})

    >>> as_bid_ramp_rates = pd.DataFrame({
    ... 'unit': ['A', 'B'],
    ... 'ramp_down_rate': [600.0, 1200.0],
    ... 'ramp_up_rate': [600.0, 1200.0]})

    >>> create_seed_ramp_rate_parameters(historical_dispatch,
    ...                                  as_bid_ramp_rates)
      unit  initial_output  ramp_down_rate  ramp_up_rate
    0    A            80.0           600.0         600.0
    1    B           100.0          1200.0        1200.0

    Parameters
    ----------
    historical_dispatch : pd.DataFrame

        ================  ========================================
        Columns:          Description:
        unit              unique identifier for units, (as `str`) \n
        initial_output    the output/consumption of the unit at \n
                          the start of the dispatch interval, \n
                          in MW, (as `np.float64`)
        ================  ========================================

    as_bid_ramp_rates

        ================  ========================================
        Columns:          Description:
        unit              unique identifier for units, (as `str`) \n
        ramp_up_rate      the ramp up rate, in MW/h, \n
                          (as `np.float64`)
        ramp_down_rate    the ramp down rate, in MW/h, \n
                          (as `np.float64`)
        ================  ========================================

    Returns
    -------
    pd.DataFrame

        ================  ========================================
        Columns:          Description:
        unit              unique identifier for units, (as `str`) \n
        initial_output    the output/consumption of the unit at \n
                          the start of the dispatch interval, \n
                          in MW, (as `np.float64`)
        ramp_up_rate      the ramp up rate, in MW/h, \n
                          (as `np.float64`)
        ramp_down_rate    the ramp down rate, in MW/h, \n
                          (as `np.float64`)
        ================  ========================================
    """
    return _merge_ramp_outputs(historical_dispatch, as_bid_ramp_rates, how='inner',
                              unit_level_seed='dispatch_type' not in historical_dispatch)
