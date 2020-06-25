from nempy.help_functions import helper_functions as hf
import pandas as pd


def energy(demand, next_constraint_id):
    """Create the constraints that ensure the amount of supply dispatched in each region equals demand.

    If only one region exists then the constraint will be of the form:

        unit 1 output + unit 2 output +. . .+ unit n output = region demand

    If multiple regions exist then a constraint will ne created for each region. If there were 2 units A and B in region
    X, and 2 units C and D in region Y, then the constraints would be of the form:

        constraint 1: unit A output + unit B output = region X demand
        constraint 2: unit C output + unit D output = region Y demand

    Examples
    --------

    >>> import pandas

    Defined the unit capacities.

    >>> demand = pd.DataFrame({
    ...   'region': ['X', 'Y'],
    ...   'demand': [1000.0, 2000.0]})

    >>> next_constraint_id = 0

    Create the constraint information.

    >>> type_and_rhs, variable_map = energy(demand, next_constraint_id)

    >>> print(type_and_rhs)
      region  constraint_id type     rhs
    0      X              0    =  1000.0
    1      Y              1    =  2000.0

    >>> print(variable_map)
       constraint_id region service  coefficient
    0              0      X  energy          1.0
    1              1      Y  energy          1.0

    Parameters
    ----------
    demand : pd.DataFrame
        Demand by region.

        ========  =====================================================================================
        Columns:  Description:
        region    unique identifier of a region (as `str`)
        demand    the non dispatchable demand, in MW (as `np.float64`)
        ========  =====================================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.


    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===============================================================
        Columns:       Description:
        region         unique identifier of a market region (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===============================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        region         the regional variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  ==========================================================================
    """
    # Create an index for each constraint.
    type_and_rhs = hf.save_index(demand, 'constraint_id', next_constraint_id)
    type_and_rhs['type'] = '='  # Supply and interconnector flow must exactly equal demand.
    type_and_rhs['rhs'] = type_and_rhs['demand']
    type_and_rhs = type_and_rhs.loc[:, ['region', 'constraint_id', 'type', 'rhs']]

    # Map constraints to energy variables in their region.
    variable_map = type_and_rhs.loc[:, ['constraint_id', 'region']]
    variable_map['service'] = 'energy'
    variable_map['coefficient'] = 1.0
    return type_and_rhs, variable_map


def fcas(fcas_requirements, next_constraint_id):
    """Create the constraints that ensure the amount of FCAS supply dispatched  equals requirements.

    Examples
    --------

    >>> import pandas

    Defined the unit capacities.

    >>> fcas_requirements = pd.DataFrame({
    ...     'set': ['raise_reg_main', 'raise_reg_main', 'raise_reg_main', 'raise_reg_main'],
    ...     'service': ['raise_reg', 'raise_reg', 'raise_reg', 'raise_reg'],
    ...     'region': ['QLD', 'NSW', 'VIC', 'SA'],
    ...     'volume': [100.0, 100.0, 100.0, 100.0]})

    >>> next_constraint_id = 0

    Create the constraint information.

    >>> type_and_rhs, variable_map = fcas(fcas_requirements, next_constraint_id)

    >>> print(type_and_rhs)
                  set  constraint_id type    rhs
    0  raise_reg_main              0    =  100.0

    >>> print(variable_map)
       constraint_id    service region  coefficient
    0              0  raise_reg    QLD          1.0
    1              0  raise_reg    NSW          1.0
    2              0  raise_reg    VIC          1.0
    3              0  raise_reg     SA          1.0

    Parameters
    ----------
    fcas_requirements : pd.DataFrame
        requirement by set and the regions and service the requirement applies to.

        ========  ===================================================================
        Columns:  Description:
        set       unique identifier of the requirement set (as `str`)
        service   the service or services the requirement set applies to (as `str`)
        region    unique identifier of a region (as `str`)
        volume    the amount of service required, in MW (as `np.float64`)
        type      the direction of the constrain '=', '>=' or '<=', optional, a \n
                  value of '=' is assumed if the column is missing (as `str`)
        ========  ===================================================================

    next_constraint_id : int
        The next integer to start using for constraint ids.

    Returns
    -------
    type_and_rhs : pd.DataFrame
        The type and rhs of each constraint.

        =============  ===================================================================
        Columns:       Description:
        set            unique identifier of a market region (as `str`)
        constraint_id  the id of the variable (as `int`)
        type           the type of the constraint, e.g. "=" (as `str`)
        rhs            the rhs of the constraint (as `np.float64`)
        =============  ===================================================================

    variable_map : pd.DataFrame
        The type of variables that should appear on the lhs of the constraint.

        =============  ==========================================================================
        Columns:       Description:
        constraint_id  the id of the constraint (as `np.int64`)
        region         the regional variables the constraint should map too (as `str`)
        service        the service type of the variables the constraint should map to (as `str`)
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  ==========================================================================
    """
    # Set default value if optional column is missing.
    if 'type' not in fcas_requirements.columns:
        fcas_requirements['type'] = '='

    # Create an index for each constraint.
    type_and_rhs = fcas_requirements.loc[:, ['set', 'volume', 'type']]
    type_and_rhs = type_and_rhs.drop_duplicates('set')
    type_and_rhs = hf.save_index(type_and_rhs, 'constraint_id', next_constraint_id)
    type_and_rhs['rhs'] = type_and_rhs['volume']
    type_and_rhs = type_and_rhs.loc[:, ['set', 'constraint_id', 'type', 'rhs']]

    # Map constraints to energy variables in their region.
    variable_map = fcas_requirements.loc[:, ['set', 'service', 'region']]
    variable_map = pd.merge(variable_map, type_and_rhs.loc[:, ['set', 'constraint_id']], 'inner', on='set')
    variable_map['coefficient'] = 1.0
    variable_map = variable_map.loc[:, ['constraint_id', 'service', 'region', 'coefficient']]
    return type_and_rhs, variable_map
