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
        constraints['initial_output'] + constraints['ramp_up_rate'] / (dispatch_interval / 60),
        constraints['initial_output'] - constraints['ramp_down_rate'] / (dispatch_interval / 60))
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
