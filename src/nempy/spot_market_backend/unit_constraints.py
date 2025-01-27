import pandas as pd
import numpy as np

from nempy.help_functions import helper_functions as hf


def capacity(unit_limits, next_constraint_id, bidirectional_units):
    """Create the constraints that ensure the dispatch of a unit is capped by its capacity.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched +. . .+ bid n dispatched <= capacity

    Examples
    --------

    >>> import pandas

    Defined the unit capacities.

    >>> unit_limits = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'capacity': [100.0, 200.0]})

    >>> next_constraint_id = 0

    >>> bidirectional_units = []

    Create the constraint information.

    >>> type_and_rhs, variable_map = capacity(
    ... unit_limits,
    ... next_constraint_id,
    ... bidirectional_units)

    >>> print(type_and_rhs)
      unit service dispatch_type  constraint_id type    rhs
    0    A  energy     generator              0   <=  100.0
    1    B  energy     generator              1   <=  200.0

    >>> print(variable_map)
       constraint_id unit service dispatch_type  coefficient
    0              0    A  energy     generator          1.0
    1              1    B  energy     generator          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Capacity by unit.

        =============  =====================================================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`) \n
        dispatch_type  "load" or "generator", optional default 'generator', (as `str`) \n
        capacity       The maximum output of the unit if unconstrained by ramp rate, in MW (as `np.float64`)
        =============  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    bidirectional_units: list[str]
        List of bidriectional units so the coefficients of bidirectional loads can be adjusted.

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator', (as `str`) \n
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===============================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        unit           the unit variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator', (as `str`) \n
        coefficient    the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================
    """
    type_and_rhs, variable_map = create_constraints(unit_limits, next_constraint_id, 'capacity', '<=')
    variable_map['coefficient'] = np.where(
        (variable_map['dispatch_type'] == 'load') & (
            variable_map['unit'].isin(bidirectional_units)),
        -1.0,
        1.0
    )
    return type_and_rhs, variable_map


def uigf(unit_limits, next_constraint_id):
    """Create the constraints that ensure the dispatch of a unit is capped by its capacity.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched +. . .+ bid n dispatched <= capacity

    Examples
    --------

    >>> import pandas

    Defined the unit capacities.

    >>> unit_limits = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'capacity': [100.0, 200.0]})

    >>> next_constraint_id = 0

    >>> bidirectional_units = []

    Create the constraint information.

    >>> type_and_rhs, variable_map = uigf(
    ... unit_limits,
    ... next_constraint_id)

    >>> print(type_and_rhs)
      unit service dispatch_type  constraint_id type    rhs
    0    A  energy     generator              0   <=  100.0
    1    B  energy     generator              1   <=  200.0

    >>> print(variable_map)
       constraint_id unit service dispatch_type  coefficient
    0              0    A  energy     generator          1.0
    1              1    B  energy     generator          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Capacity by unit.

        =============  =====================================================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`) \n
        dispatch_type  "load" or "generator", optional default 'generator', (as `str`) \n
        capacity       The maximum output of the unit if unconstrained by ramp rate, in MW (as `np.float64`)
        =============  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator', (as `str`) \n
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===============================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        unit           the unit variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator', (as `str`) \n
        coefficient    the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================
    """
    type_and_rhs, variable_map = create_constraints(unit_limits, next_constraint_id, 'capacity', '<=')
    return type_and_rhs, variable_map


def ramp_up(unit_limits, next_constraint_id, dispatch_interval):
    """Create the constraints that ensure the dispatch of a unit is capped by its ramp up rate.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched + . + bid n dispatched <= initial_output + ramp_up_rate / dispatch_interval

    Examples
    --------

    >>> import pandas

    Defined the unit capacities.

    >>> unit_limits = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'ramp_up_rate': [100.0, 200.0],
    ...   'initial_output': [50.0, 60.0]})

    >>> next_constraint_id = 0

    >>> dispatch_interval = 30

    Create the constraint information.

    >>> type_and_rhs, variable_map = ramp_up(unit_limits, next_constraint_id, dispatch_interval)

    >>> print(type_and_rhs)
      unit service dispatch_type  constraint_id type    rhs
    0    A  energy     generator              0   <=  100.0
    1    B  energy     generator              1   <=  160.0

    >>> print(variable_map)
       constraint_id unit service dispatch_type  coefficient
    0              0    A  energy     generator          1.0
    1              1    B  energy     generator          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Ramp up rate and initial output by unit.

        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator'
        initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`)
        ==============  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    dispatch_interval : int
        The length of the dispatch interval in minutes.

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator' (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===============================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        unit           the unit variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator' (as `str`)
        coefficient    the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================
    """
    unit_limits['max_output'] = unit_limits['initial_output'] + unit_limits['ramp_up_rate'] * (dispatch_interval / 60)
    type_and_rhs, variable_map = create_constraints(unit_limits, next_constraint_id, 'max_output', '<=')
    return type_and_rhs, variable_map


def ramp_down(unit_limits, next_constraint_id, dispatch_interval):
    """Create the constraints that ensure the dispatch of a unit is limited by its ramp down rate.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched + . + bid n dispatched >= initial_output - ramp_up_rate / dispatch_interval

    Examples
    --------

    >>> import pandas

    Defined the unit capacities.

    >>> unit_limits = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'ramp_down_rate': [40.0, 20.0],
    ...   'initial_output': [50.0, 60.0]})

    >>> next_constraint_id = 0

    >>> dispatch_interval = 30

    Create the constraint information.

    >>> type_and_rhs, variable_map = ramp_down(unit_limits, next_constraint_id, dispatch_interval)

    >>> print(type_and_rhs)
      unit service dispatch_type  constraint_id type   rhs
    0    A  energy     generator              0   >=  30.0
    1    B  energy     generator              1   >=  50.0


    >>> print(variable_map)
       constraint_id unit service dispatch_type  coefficient
    0              0    A  energy     generator          1.0
    1              1    B  energy     generator          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Ramp up rate and initial output by unit.

        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        dispatch_type  "load" or "generator", optional default 'generator' (as `str`)
        initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        ramp_down_rate  the maximum rate at which the unit can decrease output, in MW/h (as `np.float64`)
        ==============  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    dispatch_interval : int
        The length of the dispatch interval in minutes.

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===============================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        unit           the unit variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        coefficient    the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================
    """
    unit_limits['min_output'] = unit_limits['initial_output'] - unit_limits['ramp_down_rate'] * (dispatch_interval / 60)
    type_and_rhs, variable_map = create_constraints(unit_limits, next_constraint_id, 'min_output', '>=')
    return type_and_rhs, variable_map


def fcas_max_availability(fcas_availability, next_constraint_id):
    """Create the constraints that ensure the dispatch of a unit fcas is capped by its availability.

    A constraint of the following form will be created for each unit:

        bid 1 dispatched + bid 2 dispatched +. . .+ bid n dispatched <= availability

    Examples
    --------

    >>> import pandas

    Defined the unit fcas availability.

    >>> fcas_availability = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'service': ['raise_reg', 'lower_6s'],
    ...   'max_availability': [100.0, 200.0]})

    >>> next_constraint_id = 0

    Create the constraint information.

    >>> type_and_rhs, variable_map = fcas_max_availability(fcas_availability, next_constraint_id)

    >>> print(type_and_rhs)
      unit    service dispatch_type  constraint_id type    rhs
    0    A  raise_reg     generator              0   <=  100.0
    1    B   lower_6s     generator              1   <=  200.0


    >>> print(variable_map)
       constraint_id unit    service dispatch_type  coefficient
    0              0    A  raise_reg     generator          1.0
    1              1    B   lower_6s     generator          1.0


    Parameters
    ----------
    fcas_availability : pd.DataFrame
        Availability by unit and service.

        ================   ======================================================================
        Columns:           Description:
        unit               unique identifier of a dispatch unit (as `str`)
        service            the fcas service being offered (as `str`)
        max_availability   the maximum volume of the fcas service in MW (as `np.float64`)
        ================   ======================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===============================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        unit           the unit variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        coefficient    the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================
    """
    type_and_rhs, variable_map = create_constraints(fcas_availability, next_constraint_id, 'max_availability', '<=')
    return type_and_rhs, variable_map


def create_fast_start_profile_constraints(fast_start_profiles, next_constraint_id):

    # If no service column is present assume the constraints are for the energy service.
    if 'service' not in fast_start_profiles.columns:
        fast_start_profiles['service'] = 'energy'

    if 'dispatch_type' not in fast_start_profiles.columns:
        fast_start_profiles['dispatch_type'] = 'generator'

    mode_one_cons = fast_start_mode_one_constraints(fast_start_profiles)
    mode_two_cons = fast_start_mode_two_constraints(fast_start_profiles)
    mode_three_cons = fast_start_mode_three_constraints(fast_start_profiles)
    mode_four_cons = fast_start_mode_four_constraints(fast_start_profiles)

    type_and_rhs = []
    variable_map = []

    if not mode_one_cons.empty:
        mode_one_max_type_rhs, mode_one_max_variable_map = \
            create_constraints(mode_one_cons, next_constraint_id, 'max', '<=')
        next_constraint_id = mode_one_max_type_rhs['constraint_id'].max() + 1
        type_and_rhs.append(mode_one_max_type_rhs)
        variable_map.append(mode_one_max_variable_map)

    if not mode_two_cons.empty:
        mode_two_min_type_rhs, mode_two_min_variable_map = \
            create_constraints(mode_two_cons, next_constraint_id, 'min', '>=')
        next_constraint_id = mode_two_min_type_rhs['constraint_id'].max() + 1
        mode_two_max_type_rhs, mode_two_max_variable_map = \
            create_constraints(mode_two_cons, next_constraint_id, 'max', '<=')
        next_constraint_id = mode_two_max_type_rhs['constraint_id'].max() + 1
        type_and_rhs.append(mode_two_min_type_rhs)
        type_and_rhs.append(mode_two_max_type_rhs)
        variable_map.append(mode_two_max_variable_map)
        variable_map.append(mode_two_min_variable_map)

    if not mode_three_cons.empty:
        mode_three_min_type_rhs, mode_three_min_variable_map = \
            create_constraints(mode_three_cons, next_constraint_id, 'min', '>=')
        next_constraint_id = mode_three_min_type_rhs['constraint_id'].max() + 1
        type_and_rhs.append(mode_three_min_type_rhs)
        variable_map.append(mode_three_min_variable_map)

    if not mode_four_cons.empty:
        mode_four_min_type_rhs, mode_four_min_variable_map = \
            create_constraints(mode_four_cons, next_constraint_id, 'min', '>=')
        type_and_rhs.append(mode_four_min_type_rhs)
        variable_map.append(mode_four_min_variable_map)

    if len(type_and_rhs) > 0:
        type_and_rhs = pd.concat(type_and_rhs)
        variable_map = pd.concat(variable_map)
    else:
        type_and_rhs = pd.DataFrame()
        variable_map = pd.DataFrame()

    return type_and_rhs, variable_map


def create_constraints(unit_limits, next_constraint_id, rhs_col, direction):
    # If no service column is present assume the constraints are for the energy service.
    if 'service' not in unit_limits.columns:
        unit_limits['service'] = 'energy'

    if 'dispatch_type' not in unit_limits.columns:
        unit_limits['dispatch_type'] = 'generator'

    # Create a constraint for each unit in unit limits.
    type_and_rhs = hf.save_index(unit_limits.reset_index(drop=True), 'constraint_id', next_constraint_id)
    type_and_rhs = type_and_rhs.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', rhs_col]]
    type_and_rhs['type'] = direction  # the type i.e. >=, <=, or = is set by a parameter.
    type_and_rhs['rhs'] = type_and_rhs[rhs_col]  # column used to set the rhs is set by a parameter.
    type_and_rhs = type_and_rhs.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', 'type', 'rhs']]

    # These constraints always map to energy variables and have a coefficient of one.
    variable_map = type_and_rhs.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type']]
    variable_map['coefficient'] = 1.0

    return type_and_rhs, variable_map


def fast_start_mode_one_constraints(fast_start_profile):
    units_ending_in_mode_one = fast_start_profile[(fast_start_profile['end_mode'].isin([0, 1]))].copy()
    units_ending_in_mode_one['max'] = 0.0
    units_ending_in_mode_one['min'] = 0.0
    units_ending_in_mode_one = units_ending_in_mode_one.loc[:, ['unit', 'dispatch_type', 'min', 'max']]
    return units_ending_in_mode_one


def fast_start_mode_two_constraints(fast_start_profile):
    units_ending_in_mode_two = fast_start_profile[(fast_start_profile['end_mode'] == 2)].copy()
    units_ending_in_mode_two['target'] = (((units_ending_in_mode_two['time_in_end_mode'])
                                           / units_ending_in_mode_two['mode_two_length']) *
                                          units_ending_in_mode_two['min_loading'])
    units_ending_in_mode_two['min'] = units_ending_in_mode_two['target']
    units_ending_in_mode_two['max'] = units_ending_in_mode_two['target']
    units_ending_in_mode_two = units_ending_in_mode_two.loc[:, ['unit', 'dispatch_type', 'min', 'max']]
    return units_ending_in_mode_two


def fast_start_mode_three_constraints(fast_start_profile):
    units_ending_in_mode_three = fast_start_profile[(fast_start_profile['end_mode'] == 3)].copy()
    units_ending_in_mode_three['min'] = units_ending_in_mode_three['min_loading']
    units_ending_in_mode_three = units_ending_in_mode_three.loc[:, ['unit', 'dispatch_type', 'min']]
    return units_ending_in_mode_three


def fast_start_mode_four_constraints(fast_start_profile):
    units_ending_in_mode_four = fast_start_profile[fast_start_profile['end_mode'] == 4].copy()
    units_ending_in_mode_four['target'] = (units_ending_in_mode_four['min_loading'] -
                                           (((units_ending_in_mode_four['time_in_end_mode']) /
                                             units_ending_in_mode_four['mode_four_length']) *
                                            units_ending_in_mode_four['min_loading']))
    units_ending_in_mode_four['min'] = units_ending_in_mode_four['target']
    units_ending_in_mode_four['max'] = units_ending_in_mode_four['target']
    units_ending_in_mode_four = units_ending_in_mode_four.loc[:, ['unit', 'dispatch_type', 'min', 'max']]
    return units_ending_in_mode_four


def tie_break_constraints(price_bids, bid_decision_variables, unit_regions, next_constraint_id):
    energy_price_bids = price_bids[price_bids['service'] == 'energy']
    energy_price_bids = pd.merge(energy_price_bids,
                                 bid_decision_variables.loc[:, ['variable_id', 'upper_bound']],
                                 on='variable_id')
    energy_price_bids = pd.merge(energy_price_bids, unit_regions.loc[:, ['unit', 'region']], on='unit')

    constraints = pd.merge(energy_price_bids, energy_price_bids, on=['cost', 'region', 'dispatch_type'])
    constraints = constraints[constraints['unit_x'] != constraints['unit_y']]

    def make_id(unit_x, band_x, unit_y, band_y):
        name = sorted([unit_x, str(band_x), unit_y, str(band_y)])
        name = ''.join(name)
        return name

    constraints['name'] = \
        constraints.apply(lambda x: make_id(x['unit_x'], x['capacity_band_x'],
                                            x['unit_y'], x['capacity_band_y']), axis=1)

    constraints = constraints.drop_duplicates('name')

    constraints = constraints.loc[:, ['variable_id_x', 'upper_bound_x', 'variable_id_y', 'upper_bound_y']]
    constraints = hf.save_index(constraints, 'constraint_id', next_constraint_id)

    lhs_one = constraints.loc[:, ['constraint_id', 'variable_id_x', 'upper_bound_x']]
    lhs_one['variable_id'] = lhs_one['variable_id_x']
    lhs_one['coefficient'] = 1 / lhs_one['upper_bound_x']

    lhs_two = constraints.loc[:, ['constraint_id', 'variable_id_y', 'upper_bound_y']]
    lhs_two['variable_id'] = lhs_two['variable_id_y']
    lhs_two['coefficient'] = - 1 / lhs_two['upper_bound_y']

    lhs = pd.concat([lhs_one.loc[:, ['constraint_id', 'variable_id', 'coefficient']],
                     lhs_two.loc[:, ['constraint_id', 'variable_id', 'coefficient']]])

    rhs = constraints.loc[:, ['constraint_id']]

    rhs['type'] = '='
    rhs['rhs'] = 0.0
    return lhs, rhs
