import pandas as pd


def construct_ramp_rate_parameters(last_interval_dispatch, ramp_rates):
    """Combine dispatch and ramp rates into the ramp rate inputs compatible with the SpotMarket class.

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
    last_interval_energy_dispatch = last_interval_dispatch[last_interval_dispatch['service'] == 'energy']
    last_interval_energy_dispatch = last_interval_energy_dispatch.loc[:, ['unit', 'dispatch']]
    last_interval_energy_dispatch.columns = ['unit', 'initial_output']
    ramp_rates = pd.merge(last_interval_energy_dispatch, ramp_rates, how='right', on='unit')
    ramp_rates = ramp_rates.fillna(0.0)
    return ramp_rates


def create_seed_ramp_rate_parameters(historical_dispatch, as_bid_ramp_rates):
    """Combine historical dispatch and as bid ramp rates to get seed ramp rate parameters for a time sequential model.

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
    return pd.merge(historical_dispatch, as_bid_ramp_rates, on='unit')