import pandas as pd
from nempy import helper_functions as hf


def capacity(unit_limits, next_constraint_id):
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

    Create the constraint information.

    >>> type_and_rhs, variable_map = capacity(unit_limits, next_constraint_id)

    >>> print(type_and_rhs)
      unit  constraint_id type    rhs
    0    A              0   <=  100.0
    1    B              1   <=  200.0

    >>> print(variable_map)
       constraint_id unit service  coefficient
    0              0    A  energy          1.0
    1              1    B  energy          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Capacity by unit.

        ========  =====================================================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        capacity  The maximum output of the unit if unconstrained by ramp rate, in MW (as `np.float64`)
        ========  =====================================================================================

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
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
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
      unit  constraint_id type    rhs
    0    A              0   <=  100.0
    1    B              1   <=  160.0

    >>> print(variable_map)
       constraint_id unit service  coefficient
    0              0    A  energy          1.0
    1              1    B  energy          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Ramp up rate and initial output by unit.

        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`).
        ==============  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    dispatch_interval : float
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
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  ==========================================================================
    """
    unit_limits['max_output'] = unit_limits['initial_output'] + unit_limits['ramp_up_rate'] * (dispatch_interval / 60)
    type_and_rhs, variable_map = create_constraints(unit_limits, next_constraint_id, 'max_output', '<=')
    return  type_and_rhs, variable_map


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
      unit  constraint_id type   rhs
    0    A              0   >=  30.0
    1    B              1   >=  50.0

    >>> print(variable_map)
       constraint_id unit service  coefficient
    0              0    A  energy          1.0
    1              1    B  energy          1.0

    Parameters
    ----------
    unit_limits : pd.DataFrame
        Ramp up rate and initial output by unit.

        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        ramp_down_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`).
        ==============  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    dispatch_interval : float
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
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  ==========================================================================
    """
    unit_limits['min_output'] = unit_limits['initial_output'] - unit_limits['ramp_down_rate'] * (dispatch_interval / 60)
    type_and_rhs, variable_map = create_constraints(unit_limits, next_constraint_id, 'min_output', '>=')
    return type_and_rhs, variable_map


def create_constraints(unit_limits, next_constraint_id, rhs_col, direction):
    # Create a constraint for each unit in unit limits.
    type_and_rhs = hf.save_index(unit_limits.reset_index(drop=True), 'constraint_id', next_constraint_id)
    type_and_rhs = type_and_rhs.loc[:, ['unit', 'constraint_id', rhs_col]]
    type_and_rhs['type'] = direction  # the type i.e. >=, <=, or = is set by a parameter.
    type_and_rhs['rhs'] = type_and_rhs[rhs_col]  # column used to set the rhs is set by a parameter.
    type_and_rhs = type_and_rhs.loc[:, ['unit', 'constraint_id', 'type', 'rhs']]

    # These constraints always map to energy variables and have a coefficient of one.
    variable_map = type_and_rhs.loc[:, ['constraint_id', 'unit']]
    variable_map['service'] = 'energy'
    variable_map['coefficient'] = 1.0

    return type_and_rhs, variable_map
