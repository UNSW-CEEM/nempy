import pandas as pd
import numpy as np
from nempy.help_functions import helper_functions as hf


def joint_ramping_constraints_raise_reg_bdu(unit_limits, dispatch_interval, next_constraint_id):
    constraints = unit_limits.drop_duplicates(["unit"]).loc[:, ['unit', 'initial_output', 'ramp_rate']]
    constraints = hf.save_index(constraints, 'constraint_id', next_constraint_id)
    constraints['rhs'] = constraints['initial_output'] + (constraints['ramp_rate'] * dispatch_interval / 60)
    constraints['type'] = "<="
    rhs_and_type = constraints.loc[:, ['unit', 'constraint_id', 'type', 'rhs']]
    variable_mapping_reg = unit_limits.loc[:, ['unit', 'service', 'dispatch_type']]
    variable_mapping_reg = pd.merge(
        rhs_and_type.loc[:, ['constraint_id', 'unit']],
        variable_mapping_reg,
        on="unit"
    )
    variable_mapping_reg['coefficient'] = 1.0
    variable_mapping_energy = variable_mapping_reg.copy()
    variable_mapping_energy['service'] = 'energy'
    variable_mapping_energy['coefficient'] = 1.0
    variable_mapping = pd.concat([variable_mapping_reg, variable_mapping_energy])
    return rhs_and_type, variable_mapping


def joint_ramping_constraints_lower_reg_bdu(unit_limits, dispatch_interval, next_constraint_id):
    constraints = unit_limits.drop_duplicates(["unit"]).loc[:, ['unit', 'initial_output', 'ramp_rate']]
    constraints = hf.save_index(constraints, 'constraint_id', next_constraint_id)
    constraints['rhs'] = constraints['initial_output'] - (constraints['ramp_rate'] * dispatch_interval / 60)
    constraints['type'] = ">="
    rhs_and_type = constraints.loc[:, ['unit', 'constraint_id', 'type', 'rhs']]
    variable_mapping_reg = unit_limits.loc[:, ['unit', 'service', 'dispatch_type']]
    variable_mapping_reg = pd.merge(
        rhs_and_type.loc[:, ['constraint_id', 'unit']],
        variable_mapping_reg,
        on="unit"
    )
    variable_mapping_reg['coefficient'] = -1.0
    variable_mapping_energy = variable_mapping_reg.copy()
    variable_mapping_energy['service'] = 'energy'
    variable_mapping_energy['coefficient'] = 1.0
    variable_mapping = pd.concat([variable_mapping_reg, variable_mapping_energy])
    return rhs_and_type, variable_mapping


def joint_ramping_constraints_raise_reg(unit_limits, dispatch_interval, next_constraint_id):

    constraint_settings = {'generator': {'type': '<=',
                                         'reg_lhs_coefficient': 1.0,
                                         'ramp_direction': 1.0,
                                         'reg_service': 'raise_reg'},
                           'load': {'type': '>=',
                                    'reg_lhs_coefficient': -1.0,
                                    'ramp_direction': -1.0,
                                    'reg_service': 'raise_reg'}
                           }
    rhs_and_type, variable_mapping = \
        joint_ramping_constraints_load_and_generator_constructor(unit_limits, dispatch_interval,
                                                                 next_constraint_id, constraint_settings)
    return rhs_and_type, variable_mapping


def joint_ramping_constraints_lower_reg(unit_limits, dispatch_interval, next_constraint_id):

    constraint_settings = {'generator': {'type': '>=',
                                         'reg_lhs_coefficient': -1.0,
                                         'ramp_direction': -1.0,
                                         'reg_service': 'lower_reg'},
                           'load':  {'type': '<=',
                                     'reg_lhs_coefficient': 1.0,
                                     'ramp_direction': 1.0,
                                     'reg_service': 'lower_reg'}
                           }
    rhs_and_type, variable_mapping = \
        joint_ramping_constraints_load_and_generator_constructor(unit_limits, dispatch_interval,
                                                                 next_constraint_id, constraint_settings)
    return rhs_and_type, variable_mapping


def joint_ramping_constraints_load_and_generator_constructor(unit_limits, dispatch_interval,
                                                             next_constraint_id, settings):
    constraints = hf.save_index(unit_limits, 'constraint_id', next_constraint_id)
    constraints_generators = constraints[constraints['dispatch_type'] == 'generator'].copy()
    constraints_loads = constraints[constraints['dispatch_type'] == 'load'].copy()
    gen_rhs_and_type, gen_variable_mapping = \
        joint_ramping_constraints_generic_constructor(constraints_generators, settings['generator'],
                                                      dispatch_interval)
    load_rhs_and_type, load_variable_mapping = \
        joint_ramping_constraints_generic_constructor(constraints_loads, settings['load'],
                                                      dispatch_interval)
    rhs_and_type = pd.concat([gen_rhs_and_type, load_rhs_and_type])
    variable_mapping = pd.concat([gen_variable_mapping, load_variable_mapping])
    return rhs_and_type, variable_mapping


def joint_ramping_constraints_generic_constructor(constraints, settings, dispatch_interval):
    constraints['rhs'] = constraints['initial_output'] + settings['ramp_direction'] * \
                         (constraints['ramp_rate'] * dispatch_interval / 60)
    constraints['service'] = settings['reg_service']
    constraints['type'] = settings['type']
    rhs_and_type = constraints.loc[:, ['unit', 'constraint_id', 'type', 'rhs']]
    variable_mapping_reg = constraints.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type']]
    variable_mapping_reg['coefficient'] = settings['reg_lhs_coefficient']
    variable_mapping_energy = constraints.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type']]
    variable_mapping_energy['service'] = 'energy'
    variable_mapping_energy['coefficient'] = 1.0
    variable_mapping = pd.concat([variable_mapping_reg, variable_mapping_energy])
    return rhs_and_type, variable_mapping


def joint_capacity_constraints(contingency_trapeziums, bidirectional_units, next_constraint_id):
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
      unit   service dispatch_type  constraint_id type   rhs
    0    A  raise_6s     generator              1   <=  80.0
    0    A  raise_6s     generator              2   >=  20.0


    >>> print(variable_mapping)
       constraint_id unit dispatch_type    service  coefficient
    0              1    A     generator     energy     1.000000
    0              1    A     generator   raise_6s     0.333333
    0              1    A     generator  raise_reg     1.000000
    0              2    A     generator     energy     1.000000
    0              2    A     generator   raise_6s    -0.333333
    0              2    A     generator  lower_reg    -1.000000


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
        the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================

    """

    if 'dispatch_type' not in contingency_trapeziums.columns:
        contingency_trapeziums.loc[:, ['dispatch_type']] = 'generator'

    # Create each constraint set.
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
    type_and_rhs_upper_slope = \
        constraints_upper_slope.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', 'type', 'rhs']]

    # Define the direction of the lower slope constraints and the rhs value.
    constraints_lower_slope['type'] = '>='
    # constraints_lower_slope['enablement_min'] = np.where(constraints_lower_slope['enablement_min'] == 0.0, 1.0,
    #                                                      constraints_lower_slope['enablement_min'])
    constraints_lower_slope['rhs'] = constraints_lower_slope['enablement_min']
    type_and_rhs_lower_slope = \
        constraints_lower_slope.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', 'type', 'rhs']]

    # Define the variables on the lhs of the upper slope constraints and their coefficients.
    energy_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    energy_mapping_upper_slope['service'] = 'energy'
    energy_mapping_upper_slope['coefficient'] = 1.0

    # Bidirectional fcas traps come tagged as generator but apply to both gen and load side of unit.
    energy_mapping_upper_slope_load_side_of_bidirectionals = (
        energy_mapping_upper_slope[energy_mapping_upper_slope['unit'].isin(bidirectional_units)].copy())
    energy_mapping_upper_slope_load_side_of_bidirectionals['dispatch_type'] = 'load'
    energy_mapping_upper_slope_load_side_of_bidirectionals['coefficient'] = 1.0

    contingency_mapping_upper_slope = \
        constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type', 'upper_slope_coefficient']]
    contingency_mapping_upper_slope = \
        contingency_mapping_upper_slope.rename(columns={"upper_slope_coefficient": "coefficient"})

    regulation_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    regulation_mapping_upper_slope['service'] = np.where(regulation_mapping_upper_slope['dispatch_type'] == 'generator',
                                                         'raise_reg', 'lower_reg')
    regulation_mapping_upper_slope['coefficient'] = 1.0

    # Bidirectional fcas traps come tagged as generator but apply to both gen and load side of unit.
    regulation_mapping_upper_slope_load_side_of_bidirectionals = (
        regulation_mapping_upper_slope[regulation_mapping_upper_slope['unit'].isin(bidirectional_units)].copy())
    regulation_mapping_upper_slope_load_side_of_bidirectionals['dispatch_type'] = 'load'

    # Define the variables on the lhs of the lower slope constraints and their coefficients.
    energy_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    energy_mapping_lower_slope['service'] = 'energy'
    energy_mapping_lower_slope['coefficient'] = 1.0

    # Bidirectional fcas traps come tagged as generator but apply to both gen and load side of unit.
    energy_mapping_lower_slope_load_side_of_bidirectionals = (
        energy_mapping_lower_slope[energy_mapping_lower_slope['unit'].isin(bidirectional_units)].copy())
    energy_mapping_lower_slope_load_side_of_bidirectionals['dispatch_type'] = 'load'
    energy_mapping_lower_slope_load_side_of_bidirectionals['coefficient'] = 1.0

    contingency_mapping_lower_slope = \
        constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type', 'lower_slope_coefficient']]
    contingency_mapping_lower_slope = \
        contingency_mapping_lower_slope.rename(columns={"lower_slope_coefficient": "coefficient"})
    contingency_mapping_lower_slope['coefficient'] = -1 * contingency_mapping_lower_slope['coefficient']

    regulation_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    regulation_mapping_lower_slope['service'] = (
        np.where(regulation_mapping_lower_slope['dispatch_type'] == 'generator',
                                                         'lower_reg', 'raise_reg'))
    regulation_mapping_lower_slope['coefficient'] = -1.0

    # Bidirectional fcas traps come tagged as generator but apply to both gen and load side of unit.
    regulation_mapping_lower_slope_load_side_of_bidirectionals = (
        regulation_mapping_lower_slope[regulation_mapping_lower_slope['unit'].isin(bidirectional_units)].copy())
    regulation_mapping_lower_slope_load_side_of_bidirectionals['dispatch_type'] = 'load'
    regulation_mapping_lower_slope_load_side_of_bidirectionals['coefficient'] = 1.0

    # Combine type_and_rhs and variable_mapping.
    type_and_rhs = pd.concat([type_and_rhs_upper_slope, type_and_rhs_lower_slope])
    variable_mapping = pd.concat([energy_mapping_upper_slope, contingency_mapping_upper_slope,
                                  energy_mapping_upper_slope_load_side_of_bidirectionals,
                                  regulation_mapping_upper_slope_load_side_of_bidirectionals,
                                  regulation_mapping_upper_slope, energy_mapping_lower_slope,
                                  energy_mapping_lower_slope_load_side_of_bidirectionals,
                                  contingency_mapping_lower_slope, regulation_mapping_lower_slope,
                                  regulation_mapping_lower_slope_load_side_of_bidirectionals])
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
      unit    service dispatch_type  constraint_id type   rhs
    0    A  raise_reg     generator              1   <=  80.0
    0    A  raise_reg     generator              2   >=  20.0


    >>> print(variable_mapping)
       constraint_id unit dispatch_type    service  coefficient
    0              1    A     generator     energy     1.000000
    0              1    A     generator  raise_reg     0.333333
    0              2    A     generator     energy     1.000000
    0              2    A     generator  raise_reg    -0.333333


    Parameters
    ----------
    regulation_trapeziums : pd.DataFrame
        The FCAS trapeziums for the regulation services being offered.

    ================   ======================================================================
    Columns:           Description:
    unit               unique identifier of a dispatch unit (as `str`)
    service            the regulation service being offered (as `str`)
    dispatch_type      "load" or "generator", optional default (as `str`)
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
        dispatch_type  "load" or "generator", optional default (as `str`)
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
        dispatch_type  "load" or "generator", optional default (as `str`)
        coefficient    the constraint factor in the lhs coefficient (as `np.float64`)
        =============  ==========================================================================

    """

    if 'dispatch_type' not in regulation_trapeziums.columns:
        regulation_trapeziums.loc[:, ['dispatch_type']] = 'generator'

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
    type_and_rhs_upper_slope = \
        constraints_upper_slope.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', 'type', 'rhs']]

    # Define the direction of the lower slope constraints and the rhs value.
    constraints_lower_slope['type'] = '>='
    # constraints_lower_slope['enablement_min'] = np.where(constraints_lower_slope['enablement_min'] == 0.0, 1.0,
    #                                                      constraints_lower_slope['enablement_min'])
    constraints_lower_slope['rhs'] = constraints_lower_slope['enablement_min']
    type_and_rhs_lower_slope = \
        constraints_lower_slope.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', 'type', 'rhs']]

    # Define the variables on the lhs of the upper slope constraints and their coefficients.
    energy_mapping_upper_slope = constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    energy_mapping_upper_slope['service'] = 'energy'
    energy_mapping_upper_slope['coefficient'] = 1.0
    regulation_mapping_upper_slope = \
        constraints_upper_slope.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type', 'upper_slope_coefficient']]
    regulation_mapping_upper_slope = \
        regulation_mapping_upper_slope.rename(columns={"upper_slope_coefficient": "coefficient"})

    # Define the variables on the lhs of the lower slope constraints and their coefficients.
    energy_mapping_lower_slope = constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'dispatch_type']]
    energy_mapping_lower_slope['service'] = 'energy'
    energy_mapping_lower_slope['coefficient'] = 1.0
    regulation_mapping_lower_slope = \
        constraints_lower_slope.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type', 'lower_slope_coefficient']]
    regulation_mapping_lower_slope = \
        regulation_mapping_lower_slope.rename(columns={"lower_slope_coefficient": "coefficient"})
    regulation_mapping_lower_slope['coefficient'] = -1 * regulation_mapping_lower_slope['coefficient']

    # Combine type_and_rhs and variable_mapping.
    type_and_rhs = pd.concat([type_and_rhs_upper_slope, type_and_rhs_lower_slope])
    variable_mapping = pd.concat([energy_mapping_upper_slope, regulation_mapping_upper_slope,
                                  energy_mapping_lower_slope, regulation_mapping_lower_slope])
    return type_and_rhs, variable_mapping
