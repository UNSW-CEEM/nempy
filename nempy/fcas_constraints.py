import pandas as pd
import numpy as np
from nempy import helper_functions as hf


def joint_ramping_constraints(regulation_units, unit_limits, dispatch_interval, next_constraint_id):
    """Create constraints that ensure the provision of energy and fcas are within unit ramping capabilities.

    The constraints are described in the
    :download:`FCAS MODEL IN NEMDE documentation section 6.1  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

    On a unit basis they take the form of:

        Energy dispatch + Regulation raise target <= initial output + ramp up rate / (dispatch interval / 60)

    and

        Energy dispatch + Regulation lower target <= initial output - ramp down rate / (dispatch interval / 60)

    Examples
    --------

    >>> import pandas as pd

    >>> regulation_units = pd.DataFrame({
    ...   'unit': ['A', 'B', 'B'],
    ...   'service': ['raise_reg', 'lower_reg', 'raise_reg']})

    >>> unit_limits = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'initial_output': [100.0, 80.0],
    ...   'ramp_up_rate': [20.0, 10.0],
    ...   'ramp_down_rate': [15.0, 25.0]})

    >>> dispatch_interval = 60

    >>> next_constraint_id = 1

    >>> type_and_rhs, variable_mapping = joint_ramping_constraints(regulation_units, unit_limits, dispatch_interval,
    ...                                                            next_constraint_id)

    >>> print(type_and_rhs)
      unit  constraint_id type    rhs
    0    A              1   <=  120.0
    1    B              2   >=   55.0
    2    B              3   <=   90.0

    >>> print(variable_mapping)
       constraint_id unit    service  coefficient
    0              1    A  raise_reg          1.0
    1              2    B  lower_reg          1.0
    2              3    B  raise_reg          1.0
    0              1    A     energy          1.0
    1              2    B     energy          1.0
    2              3    B     energy          1.0

    Parameters
    ----------
    regulation_units : pd.DataFrame
        The units with bids submitted to provide regulation FCAS

        ========  =======================================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        service   the regulation service being bid for raise_reg or lower_reg  (as `str`)
        ========  =======================================================================

    unit_limits : pd.DataFrame
        The initial output and ramp rates of units
        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`)
        ramp_down_rate  the maximum rate at which the unit can decrease output, in MW/h (as `np.float64`)
        ==============  =====================================================================================

    dispatch_interval : int
        The length of the dispatch interval in minutes

    next_constraint_id : int
        The next integer to start using for constraint ids

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ====================================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        service        the regulation service the constraint is associated with (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ====================================================================

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

    # Create a constraint for each regulation service being offered by a unit.
    constraints = hf.save_index(regulation_units, 'constraint_id', next_constraint_id)
    # Map the unit limit information to the constraints so the rhs values can be calculated.
    constraints = pd.merge(constraints, unit_limits, 'left', on='unit')
    constraints['rhs'] = np.where(
        constraints['service'] == 'raise_reg',
        constraints['initial_output'] + constraints['ramp_up_rate'] * (dispatch_interval / 60),
        constraints['initial_output'] - constraints['ramp_down_rate'] * (dispatch_interval / 60))
    # Set the inequality type based on the regulation service being provided.
    constraints['type'] = np.where(constraints['service'] == 'raise_reg', '<=', '>=')
    rhs_and_type = constraints.loc[:, ['unit', 'constraint_id', 'type', 'rhs']]

    # Map each constraint to it corresponding unit and regulation service.
    variable_mapping_reg = constraints.loc[:, ['constraint_id', 'unit', 'service']]
    # Also map to the energy service being provided by the unit.
    variable_mapping_energy = constraints.loc[:, ['constraint_id', 'unit', 'service']]
    variable_mapping_energy['service'] = 'energy'
    # Combine mappings.
    variable_mapping = pd.concat([variable_mapping_reg, variable_mapping_energy])
    variable_mapping['coefficient'] = 1.0

    return rhs_and_type, variable_mapping


def joint_capacity_constraints(contingency_trapeziums, unit_info, next_constraint_id):
    """Creates constraints to ensure there is adequate capacity for contingency, regulation and energy dispatch targets.

    Create two constraints for each contingency services, one ensures operation on upper slope of the fcas contingency
    trapezium is consistent with regulation raise and energy dispatch, the second ensures operation on lower slope of
    the fcas contingency trapezium is consistent with regulation lower and energy dispatch.

    The constraints are described in the
    :download:`FCAS MODEL IN NEMDE documentation section 6.2  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

    Examples
    --------
    >>> import pandas as pd

    >>> contingency_trapeziums = pd.DataFrame({
    ... 'unit': ['A'],
    ... 'service': ['raise_6s'],
    ... 'max_availability': [60.0],
    ... 'enablement_min': [20.0],
    ... 'low_break_point': [40.0],
    ... 'high_break_point': [60.0],
    ... 'enablement_max': [80.0]})

    >>> unit_info = pd.DataFrame({
    ... 'unit': ['A'],
    ... 'dispatch_type': ['generator']})

    >>> next_constraint_id = 1

    >>> type_and_rhs, variable_mapping = joint_capacity_constraints(contingency_trapeziums, unit_info,
    ...                                                             next_constraint_id)

    >>> print(type_and_rhs)
      unit   service  constraint_id type   rhs
    0    A  raise_6s              1   <=  80.0
    0    A  raise_6s              2   >=  20.0

    >>> print(variable_mapping)
       constraint_id unit    service  coefficient
    0              1    A     energy     1.000000
    0              1    A   raise_6s     0.333333
    0              1    A  raise_reg     1.000000
    0              2    A     energy     1.000000
    0              2    A   raise_6s    -0.333333
    0              2    A  lower_reg    -1.000000

    Parameters
    ----------
    contingency_trapeziums : pd.DataFrame
        The FCAS trapeziums for the contingency services being offered.

    ================   ======================================================================
    Columns:           Description:
    unit               unique identifier of a dispatch unit (as `str`)
    service            the contingency service being offered (as `str`)
    max_availability   the maximum volume of the contingency service in MW (as `np.float64`)
    enablement_min     the energy dispatch level at which the unit can begin to provide the
                       contingency service, in MW (as `np.float64`)
    low_break_point    the energy dispatch level at which the unit can provide the full
                       contingency service offered, in MW (as `np.float64`)
    high_break_point   the energy dispatch level at which the unit can no longer provide the
                       full contingency service offered, in MW (as `np.float64`)
    enablement_max     the energy dispatch level at which the unit can no longer begin
                       the contingency service, in MW (as `np.float64`)
    ================   ======================================================================

    unit_info : pd.DataFrame
    ================   ======================================================================
    Columns:           Description:
    unit               unique identifier of a dispatch unit (as `str`)
    dispatch_type      "load" or "generator" (as `str`)
    ================   ======================================================================


    next_constraint_id : int
        The next integer to start using for constraint ids

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ====================================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        service        the regulation service the constraint is associated with (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ====================================================================

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

    # Create each constraint set.
    contingency_trapeziums = pd.merge(contingency_trapeziums, unit_info, 'inner', on='unit')
    constraints_upper_slope = hf.save_index(contingency_trapeziums, 'constraint_id', next_constraint_id)
    next_constraint_id = max(constraints_upper_slope['constraint_id']) + 1
    constraints_lower_slope = hf.save_index(contingency_trapeziums, 'constraint_id', next_constraint_id)

    # Calculate the slope coefficients for the constraints.
    constraints_upper_slope['upper_slope_coefficient'] = ((constraints_upper_slope['enablement_max'] -
                                                          constraints_upper_slope['high_break_point']) /
                                                         constraints_upper_slope['max_availability'])
    constraints_lower_slope['lower_slope_coefficient'] = ((constraints_lower_slope['low_break_point'] -
                                                          constraints_lower_slope['enablement_min']) /
                                                         constraints_lower_slope['max_availability'])

    # Define the direction of the upper slope constraints and the rhs value.
    constraints_upper_slope['type'] = '<='
    constraints_upper_slope['rhs'] = constraints_upper_slope['enablement_max']
    type_and_rhs_upper_slope = constraints_upper_slope.loc[:, ['unit', 'service', 'constraint_id', 'type', 'rhs']]

    # Define the direction of the lower slope constraints and the rhs value.
    constraints_lower_slope['type'] = '>='
    constraints_lower_slope['rhs'] = constraints_lower_slope['enablement_min']
    type_and_rhs_lower_slope = constraints_lower_slope.loc[:, ['unit', 'service', 'constraint_id', 'type', 'rhs']]

    # Define the variables on the lhs of the upper slope constraints and their coefficients.
    energy_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit']]
    energy_mapping_upper_slope['service'] = 'energy'
    energy_mapping_upper_slope['coefficient'] = 1.0
    contingency_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'service',
                                                                      'upper_slope_coefficient']]
    contingency_mapping_upper_slope = \
        contingency_mapping_upper_slope.rename(columns={"upper_slope_coefficient": "coefficient"})
    regulation_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    regulation_mapping_upper_slope['service'] = np.where(regulation_mapping_upper_slope['dispatch_type'] == 'generator',
                                                         'raise_reg', 'lower_reg')
    regulation_mapping_upper_slope = regulation_mapping_upper_slope.drop('dispatch_type', axis=1)
    regulation_mapping_upper_slope['coefficient'] = 1.0

    # Define the variables on the lhs of the lower slope constraints and their coefficients.
    energy_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit']]
    energy_mapping_lower_slope['service'] = 'energy'
    energy_mapping_lower_slope['coefficient'] = 1.0
    contingency_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'service',
                                                                      'lower_slope_coefficient']]
    contingency_mapping_lower_slope = \
        contingency_mapping_lower_slope.rename(columns={"lower_slope_coefficient": "coefficient"})
    contingency_mapping_lower_slope['coefficient'] = -1 * contingency_mapping_lower_slope['coefficient']
    regulation_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    regulation_mapping_lower_slope['service'] = np.where(regulation_mapping_lower_slope['dispatch_type'] == 'generator',
                                                         'lower_reg', 'raise_reg')
    regulation_mapping_lower_slope = regulation_mapping_lower_slope.drop('dispatch_type', axis=1)
    regulation_mapping_lower_slope['coefficient'] = -1.0

    # Combine type_and_rhs and variable_mapping.
    type_and_rhs = pd.concat([type_and_rhs_upper_slope, type_and_rhs_lower_slope])
    variable_mapping = pd.concat([energy_mapping_upper_slope, contingency_mapping_upper_slope,
                                  regulation_mapping_upper_slope, energy_mapping_lower_slope,
                                  contingency_mapping_lower_slope, regulation_mapping_lower_slope])
    return type_and_rhs, variable_mapping


def energy_and_regulation_capacity_constraints(regulation_trapeziums, next_constraint_id):
    """Creates constraints to ensure there is adequate capacity for regulation and energy dispatch targets.

    Create two constraints for each regulation services, one ensures operation on upper slope of the fcas contingency
    trapezium is consistent with energy dispatch, the second ensures operation on lower slope of the fcas regulation
    trapezium is consistent with energy dispatch.

    The constraints are described in the
    :download:`FCAS MODEL IN NEMDE documentation section 6.3  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

    Examples
    --------
    >>> import pandas as pd

    >>> regulation_trapeziums = pd.DataFrame({
    ... 'unit': ['A'],
    ... 'service': ['raise_reg'],
    ... 'max_availability': [60.0],
    ... 'enablement_min': [20.0],
    ... 'low_break_point': [40.0],
    ... 'high_break_point': [60.0],
    ... 'enablement_max': [80.0]})

    >>> next_constraint_id = 1

    >>> type_and_rhs, variable_mapping = energy_and_regulation_capacity_constraints(regulation_trapeziums,
    ...                                                                             next_constraint_id)

    >>> print(type_and_rhs)
      unit    service  constraint_id type   rhs
    0    A  raise_reg              1   <=  80.0
    0    A  raise_reg              2   >=  20.0

    >>> print(variable_mapping)
       constraint_id unit    service  coefficient
    0              1    A     energy     1.000000
    0              1    A  raise_reg     0.333333
    0              2    A     energy     1.000000
    0              2    A  raise_reg    -0.333333

    Parameters
    ----------
    regulation_trapeziums : pd.DataFrame
        The FCAS trapeziums for the regulation services being offered.

    ================   ======================================================================
    Columns:           Description:
    unit               unique identifier of a dispatch unit (as `str`)
    service            the regulation service being offered (as `str`)
    max_availability   the maximum volume of the contingency service in MW (as `np.float64`)
    enablement_min     the energy dispatch level at which the unit can begin to provide the
                       contingency service, in MW (as `np.float64`)
    low_break_point    the energy dispatch level at which the unit can provide the full
                       contingency service offered, in MW (as `np.float64`)
    high_break_point   the energy dispatch level at which the unit can no longer provide the
                       full contingency service offered, in MW (as `np.float64`)
    enablement_max     the energy dispatch level at which the unit can no longer begin
                       the contingency service, in MW (as `np.float64`)
    ================   ======================================================================


    next_constraint_id : int
        The next integer to start using for constraint ids

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ====================================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        service        the regulation service the constraint is associated with (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ====================================================================

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

    # Create each constraint set.
    constraints_upper_slope = hf.save_index(regulation_trapeziums, 'constraint_id', next_constraint_id)
    next_constraint_id = max(constraints_upper_slope['constraint_id']) + 1
    constraints_lower_slope = hf.save_index(regulation_trapeziums, 'constraint_id', next_constraint_id)

    # Calculate the slope coefficients for the constraints.
    constraints_upper_slope['upper_slope_coefficient'] = ((constraints_upper_slope['enablement_max'] -
                                                          constraints_upper_slope['high_break_point']) /
                                                         constraints_upper_slope['max_availability'])
    constraints_lower_slope['lower_slope_coefficient'] = ((constraints_lower_slope['low_break_point'] -
                                                          constraints_lower_slope['enablement_min']) /
                                                         constraints_lower_slope['max_availability'])

    # Define the direction of the upper slope constraints and the rhs value.
    constraints_upper_slope['type'] = '<='
    constraints_upper_slope['rhs'] = constraints_upper_slope['enablement_max']
    type_and_rhs_upper_slope = constraints_upper_slope.loc[:, ['unit', 'service', 'constraint_id', 'type', 'rhs']]

    # Define the direction of the lower slope constraints and the rhs value.
    constraints_lower_slope['type'] = '>='
    constraints_lower_slope['rhs'] = constraints_lower_slope['enablement_min']
    type_and_rhs_lower_slope = constraints_lower_slope.loc[:, ['unit', 'service', 'constraint_id', 'type', 'rhs']]

    # Define the variables on the lhs of the upper slope constraints and their coefficients.
    energy_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit']]
    energy_mapping_upper_slope['service'] = 'energy'
    energy_mapping_upper_slope['coefficient'] = 1.0
    regulation_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'service',
                                                                      'upper_slope_coefficient']]
    regulation_mapping_upper_slope = \
        regulation_mapping_upper_slope.rename(columns={"upper_slope_coefficient": "coefficient"})

    # Define the variables on the lhs of the lower slope constraints and their coefficients.
    energy_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit']]
    energy_mapping_lower_slope['service'] = 'energy'
    energy_mapping_lower_slope['coefficient'] = 1.0
    regulation_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'service',
                                                                     'lower_slope_coefficient']]
    regulation_mapping_lower_slope = \
        regulation_mapping_lower_slope.rename(columns={"lower_slope_coefficient": "coefficient"})
    regulation_mapping_lower_slope['coefficient'] = -1 * regulation_mapping_lower_slope['coefficient']

    # Combine type_and_rhs and variable_mapping.
    type_and_rhs = pd.concat([type_and_rhs_upper_slope, type_and_rhs_lower_slope])
    variable_mapping = pd.concat([energy_mapping_upper_slope, regulation_mapping_upper_slope,
                                  energy_mapping_lower_slope, regulation_mapping_lower_slope])
    return type_and_rhs, variable_mapping


