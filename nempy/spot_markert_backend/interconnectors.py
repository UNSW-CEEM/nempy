import numpy as np
import pandas as pd
from nempy.help_functions import helper_functions as hf


def create(definitions, next_variable_id):
    """Create decision variables, and their mapping to constraints. For modeling interconnector flows. As DataFrames.

    Examples
    --------
    Definitions for two interconnectors, one called A, that nominal flows from region X to region Y, note A can flow in
    both directions because of the way max and min are defined. The interconnector B nominal flows from Y to Z, but can
    only flow in the forward direction.

    >>> inter_definitions = pd.DataFrame({
    ...   'interconnector': ['A', 'B'],
    ...   'from_region': ['X', 'Y'],
    ...   'to_region': ['Y', 'Z'],
    ...   'max': [100.0, 400.0],
    ...   'min': [-100.0, 50.0],
    ...   'from_region_loss_factor': [0.9, 1.0],
    ...   'to_region_loss_factor': [1.0, 1.1]})

    >>> print(inter_definitions)
      interconnector from_region  ... from_region_loss_factor  to_region_loss_factor
    0              A           X  ...                     1.0                    1.0
    1              B           Y  ...                     1.0                    1.0

    Start creating new variable ids from 0.

    >>> next_variable_id = 0

    Run the function and print results.

    >>> decision_variables, constraint_map = create(inter_definitions, next_variable_id)

    >>> print(decision_variables)
      interconnector  variable_id  lower_bound  upper_bound        type
    0              A            0       -100.0        100.0  continuous
    1              B            1         50.0        400.0  continuous

    >>> print(constraint_map)
       variable_id region service  coefficient
    0            0      Y  energy          1.0
    1            1      Z  energy          1.1
    2            0      X  energy         -0.9
    3            1      Y  energy         -1.0

    Parameters
    ----------
    definitions : pd.DataFrame
        Interconnector definition.

        ==============  =====================================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        to_region       the region that receives power when flow is in the positive direction (as `str`)
        from_region     the region that power is drawn from when flow is in the positive direction (as `str`)
        max             the maximum power flow in the positive direction, in MW (as `np.float64`)
        min             the maximum power flow in the negative direction, in MW (as `np.float64`)
        ==============  =====================================================================================

    next_variable_id : int

    Returns
    -------
    decision_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        lower_bound     the lower bound of the variable, the min interconnector flow (as `np.float64`)
        upper_bound     the upper bound of the variable, the max inerconnector flow (as `np.float64`)
        type            the type of variable, is continuous for interconnectors  (as `str`)
        ==============  ==============================================================================

    constraint_map : pd.DataFrame
        Sets out which regional demand constraints the variable should be linked to.

        =============  ===================================================================================
        Columns:       Description:
        variable_id    the id of the variable (as `int`)
        region         the regional constraints to map the variable to  (as `str`)
        service        the service type constraints to map too, only energy for interconnectors (as `str`)
        coefficient    the variable side contribution to the coefficient (as `np.float64`)
        =============  ====================================================================================
    """

    # Create a variable_id for each interconnector.
    decision_variables = hf.save_index(definitions, 'variable_id', next_variable_id)

    # Create two entries in the constraint_map for each interconnector. This means the variable will be mapped to the
    # demand constraint of both connected regions.
    constraint_map = hf.stack_columns(decision_variables, ['variable_id', 'interconnector', 'max', 'min'],
                                      ['to_region', 'from_region'], 'direction', 'region')
    loss_factors = hf.stack_columns(decision_variables, ['variable_id'],
                                      ['from_region_loss_factor', 'to_region_loss_factor'], 'direction', 'loss_factor')
    loss_factors['direction'] = loss_factors['direction'].apply(lambda x: x.replace('_loss_factor', ''))
    constraint_map = pd.merge(constraint_map, loss_factors, on=['variable_id', 'direction'])


    # Define decision variable attributes.
    decision_variables['type'] = 'continuous'
    decision_variables = decision_variables.loc[:, ['interconnector', 'variable_id', 'min', 'max', 'type']]
    decision_variables.columns = ['interconnector',  'variable_id', 'lower_bound', 'upper_bound', 'type']

    # Set positive coefficient for the to_region so the interconnector flowing in the nominal direction helps meet the
    # to_region demand constraint. Negative for the from_region, same logic.
    constraint_map['coefficient'] = np.where(constraint_map['direction'] == 'to_region',
                                             1.0 * constraint_map['loss_factor'],
                                             -1.0 * constraint_map['loss_factor'])
    constraint_map['service'] = 'energy'
    constraint_map = constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]

    return decision_variables, constraint_map


def create_market_interconnector(definitions, next_variable_id):
    """Create decision variables, and their mapping to constraints. For modeling interconnector flows.

    Examples
    --------
    Definitions for two interconnectors, one called A, that nominal flows from region X to region Y, note A can flow in
    both directions because of the way max and min are defined. The interconnector B nominal flows from Y to Z, but can
    only flow in the forward direction.

    >>> interconnector = pd.DataFrame({
    ...     'link': ['A', 'B'],
    ...     'interconnector': ['inter_one', 'inter_one'],
    ...     'to_region': ['VIC', 'NSW'],
    ...     'from_region': ['NSW', 'VIC'],
    ...     'min': [0.0, 0.0],
    ...     'max': [100.0, 110.0],
    ...     'generic_constraint_factor': [1.0, -1.0],
    ...     'from_region_loss_factor': [1.0, 0.9],
    ...     'to_region_loss_factor': [0.9, 1.0]})

    >>> print(interconnector)
      link interconnector  ... from_region_loss_factor to_region_loss_factor
    0    A      inter_one  ...                     1.0                   0.9
    1    B      inter_one  ...                     0.9                   1.0
    <BLANKLINE>
    [2 rows x 9 columns]

    Start creating new variable ids from 0.

    >>> next_variable_id = 0

    Run the function and print results.

    >>> decision_variables, constraint_map = create_market_interconnector(interconnector, next_variable_id)

    >>> print(decision_variables)
      link interconnector  ...        type  generic_constraint_factor
    0    A      inter_one  ...  continuous                        1.0
    1    B      inter_one  ...  continuous                       -1.0
    <BLANKLINE>
    [2 rows x 7 columns]

    >>> print(constraint_map)
       variable_id region service  coefficient
    0            0    VIC  energy          0.9
    1            1    NSW  energy          1.0
    2            0    NSW  energy         -1.0
    3            1    VIC  energy         -0.9
    """

    # Create a variable_id for each interconnector.
    decision_variables = hf.save_index(definitions, 'variable_id', next_variable_id)

    # Create two entries in the constraint_map for each interconnector. This means the variable will be mapped to the
    # demand constraint of both connected regions.
    constraint_map = hf.stack_columns(decision_variables, ['variable_id', 'link', 'interconnector', 'max', 'min'],
                                      ['to_region', 'from_region'], 'direction', 'region')
    loss_factors = hf.stack_columns(decision_variables, ['variable_id'],
                                      ['from_region_loss_factor', 'to_region_loss_factor'], 'direction', 'loss_factor')
    loss_factors['direction'] = loss_factors['direction'].apply(lambda x: x.replace('_loss_factor', ''))
    constraint_map = pd.merge(constraint_map, loss_factors, on=['variable_id', 'direction'])


    # Define decision variable attributes.
    decision_variables['type'] = 'continuous'
    decision_variables = decision_variables.loc[:, ['link', 'interconnector', 'variable_id', 'min', 'max', 'type',
                                                    'generic_constraint_factor']]
    decision_variables.columns = ['link', 'interconnector',  'variable_id', 'lower_bound', 'upper_bound', 'type',
                                  'generic_constraint_factor']

    # Set positive coefficient for the to_region so the interconnector flowing in the nominal direction helps meet the
    # to_region demand constraint. Negative for the from_region, same logic.
    constraint_map['coefficient'] = np.where(constraint_map['direction'] == 'to_region',
                                             1.0 * constraint_map['loss_factor'],
                                             -1.0 * constraint_map['loss_factor'])
    constraint_map['service'] = 'energy'
    constraint_map = constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]

    return decision_variables, constraint_map


def link_inter_loss_to_interpolation_weights(weight_variables, loss_variables, loss_functions, next_constraint_id):
    """Create the constraints that force the interconnector losses to be set by the interpolation weights.

    For one interconnector, if we had 3 break points at -100 MW, 0 MW and 100 MW, three weight variables w1, w2, and w3,
    and a loss function f, then the constraint would be of the form.

        w1 * f(-100.0) + w2 * f(0.0) + w3 * f(100.0) = interconnector losses

    Examples
    --------

    Setup function inputs

    >>> loss_variables = pd.DataFrame({
    ...   'interconnector': ['I'],
    ...   'variable_id': [0]})

    >>> weight_variables = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'variable_id': [1, 2, 3],
    ...   'break_point': [-100.0, 0, 100.0]})

    Loss functions can arbitrary, they just need to take the flow as input and return losses as an output.

    >>> def constant_losses(flow):
    ...     return abs(flow) * 0.05

    The loss function get assigned to an interconnector by its row in the loss functions DataFrame.

    >>> loss_functions = pd.DataFrame({
    ...    'interconnector': ['I'],
    ...    'from_region_loss_share': [0.5],
    ...    'loss_function': [constant_losses]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> lhs, rhs = link_inter_loss_to_interpolation_weights(weight_variables, loss_variables, loss_functions,
    ...                                                     next_constraint_id)

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              0          5.0
    1            2              0          0.0
    2            3              0          5.0

    >>> print(rhs)
      interconnector  constraint_id type  rhs_variable_id
    0              I              0    =                0


    Parameters
    ----------
    weight_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.int64`)
        ==============  ==============================================================================

    loss_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        ==============  ==============================================================================

    loss_functions : pd.DataFrame

        ======================  ==============================================================================
        Columns:                Description:
        interconnector          unique identifier of a interconnector (as `str`)
        from_region_loss_share  The fraction of loss occuring in the from region, 0.0 to 1.0 (as `np.float64`)
        loss_function           A function that takes a flow, in MW as a float and returns the losses in MW
                                (as `callable`)
        ======================  ==============================================================================

    next_constraint_id : int

    Returns
    -------
    lhs : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        variable_id     the id of the variable (as `np.int64`)
        constraint_id   the id of the constraint (as `np.int64`)
        coefficient     the coefficient of the variable on the lhs of the constraint (as `np.float64`)
        ==============  ==============================================================================

    rhs : pd.DataFrame

        ================  ==============================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        constraint_id     the id of the constraint (as `np.int64`)
        type              the type of the constraint, e.g. "=" (as `str`)
        rhs_variable_id   the rhs of the constraint (as `np.int64`)
        ================  ==============================================================================
    """

    # Create a constraint for each set of weight variables.
    constraint_ids = weight_variables.loc[:, ['interconnector', 'inter_variable_id']].drop_duplicates('inter_variable_id')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)

    # Map weight variables to their corresponding constraints.
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'inter_variable_id', 'variable_id', 'break_point']],
                   constraint_ids, 'inner', on=['inter_variable_id', 'interconnector'])
    lhs = pd.merge(lhs, loss_functions.loc[:, ['inter_variable_id', 'loss_function']], 'inner', on='inter_variable_id')

    # Evaluate the loss function at each break point to get the lhs coefficient.
    lhs['coefficient'] = lhs.apply(lambda x: x['loss_function'](x['break_point']), axis=1)
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    # Get the loss variables that will be on the rhs of the constraints.
    rhs_variables = loss_variables.loc[:, ['interconnector', 'inter_variable_id', 'variable_id']]
    rhs_variables.columns = ['interconnector', 'inter_variable_id', 'rhs_variable_id']
    # Map the rhs variables to their constraints.
    rhs = pd.merge(constraint_ids, rhs_variables, 'inner', on=['inter_variable_id', 'interconnector'])
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'inter_variable_id', 'constraint_id', 'type', 'rhs_variable_id']]
    return lhs, rhs


def link_weights_to_inter_flow(weight_variables, flow_variables, next_constraint_id):
    """Create the constraints that link the interpolation weights to interconnector flow.

    For one interconnector, if we had 3 break points at -100 MW, 0 MW and 100 MW, three weight variables w1, w2, and w3,
    then the constraint would be of the form.

        w1 * -100.0 + w2 * 0.0 + w3 * 100.0 = interconnector flow

    Examples
    --------

    Setup function inputs

    >>> flow_variables = pd.DataFrame({
    ...   'interconnector': ['I'],
    ...   'variable_id': [0]})

    >>> weight_variables = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'variable_id': [1, 2, 3],
    ...   'break_point': [-100.0, 0, 100.0]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> lhs, rhs = link_weights_to_inter_flow(weight_variables, flow_variables, next_constraint_id)

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              0       -100.0
    1            2              0          0.0
    2            3              0        100.0

    >>> print(rhs)
      interconnector  constraint_id type  rhs_variable_id
    0              I              0    =                0


    Parameters
    ----------
    weight_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.int64`)
        ==============  ==============================================================================

    flow_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        ==============  ==============================================================================

    next_constraint_id : int

    Returns
    -------
    lhs : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        variable_id     the id of the variable (as `np.int64`)
        constraint_id   the id of the constraint (as `np.int64`)
        coefficient     the coefficient of the variable on the lhs of the constraint (as `np.float64`)
        ==============  ==============================================================================

    rhs : pd.DataFrame

        ================  ==============================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        constraint_id     the id of the constraint (as `np.int64`)
        type              the type of the constraint, e.g. "=" (as `str`)
        rhs_variable_id   the rhs of the constraint (as `np.int64`)
        ================  ==============================================================================
    """

    # Create a constraint for each set of weight variables.
    constraint_ids = weight_variables.loc[:, ['interconnector', 'inter_variable_id']].drop_duplicates('inter_variable_id')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)

    # Map weight variables to their corresponding constraints.
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'inter_variable_id', 'variable_id', 'break_point']],
                   constraint_ids, 'inner', on='inter_variable_id')
    lhs['coefficient'] = lhs['break_point']
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    # Get the interconnector variables that will be on the rhs of constraint.
    rhs_variables = flow_variables.loc[:, ['interconnector', 'inter_variable_id', 'variable_id']]
    rhs_variables.columns = ['interconnector', 'inter_variable_id', 'rhs_variable_id']
    # Map the rhs variables to their constraints.
    rhs = pd.merge(constraint_ids, rhs_variables, 'inner', on=['inter_variable_id', 'interconnector'])
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'inter_variable_id', 'constraint_id', 'type', 'rhs_variable_id']]
    return lhs, rhs


def create_weights_must_sum_to_one(weight_variables, next_constraint_id):
    """Create the constraint to force weight variable to sum to one, need for interpolation to work.

    For one interconnector, if we had  three weight variables w1, w2, and w3, then the constraint would be of the form.

        w1 * 1.0 + w2 * 1.0 + w3 * 1.0 = 1.0

    Examples
    --------

    Setup function inputs

    >>> weight_variables = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'variable_id': [1, 2, 3],
    ...   'break_point': [-100.0, 0, 100.0]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> lhs, rhs = create_weights_must_sum_to_one(weight_variables, next_constraint_id)

    >>> print(lhs)
       variable_id  constraint_id  coefficient
    0            1              0          1.0
    1            2              0          1.0
    2            3              0          1.0

    >>> print(rhs)
      interconnector  constraint_id type  rhs
    0              I              0    =  1.0


    Parameters
    ----------
    weight_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.int64`)
        ==============  ==============================================================================

    next_constraint_id : int

    Returns
    -------
    lhs : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        variable_id     the id of the variable (as `np.int64`)
        constraint_id   the id of the constraint (as `np.int64`)
        coefficient     the coefficient of the variable on the lhs of the constraint (as `np.float64`)
        ==============  ==============================================================================

    rhs : pd.DataFrame

        ================  ==============================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        constraint_id     the id of the constraint (as `np.int64`)
        type              the type of the constraint, e.g. "=" (as `str`)
        rhs               the rhs of the constraint (as `np.float64`)
        ================  ==============================================================================
    """

    # Create a constraint for each set of weight variables.
    constraint_ids = weight_variables.loc[:, ['inter_variable_id']].drop_duplicates('inter_variable_id')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)

    # Map weight variables to their corresponding constraints.
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'inter_variable_id', 'variable_id']], constraint_ids,
                   'inner', on='inter_variable_id')
    lhs['coefficient'] = 1.0
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]

    # Create rhs details for each constraint.
    rhs = constraint_ids
    rhs['type'] = '='
    rhs['rhs'] = 1.0
    return lhs, rhs


def create_weights(break_points, next_variable_id):
    """Create interpolation weight variables for each breakpoint.

    Examples
    --------

    >>> break_points = pd.DataFrame({
    ...   'interconnector': ['I', 'I', 'I'],
    ...   'loss_segment': [1, 2, 3],
    ...   'break_point': [-100.0, 0.0, 100.0]})

    >>> next_variable_id = 0

    >>> weight_variables = create_weights(break_points, next_variable_id)

    >>> print(weight_variables.loc[:, ['interconnector', 'loss_segment', 'break_point', 'variable_id']])
      interconnector  loss_segment  break_point  variable_id
    0              I             1       -100.0            0
    1              I             2          0.0            1
    2              I             3        100.0            2

    >>> print(weight_variables.loc[:, ['variable_id', 'lower_bound', 'upper_bound', 'type']])
       variable_id  lower_bound  upper_bound        type
    0            0          0.0          1.0  continuous
    1            1          0.0          1.0  continuous
    2            2          0.0          1.0  continuous

    Parameters
    ----------
    break_points : pd.DataFrame
        ==============  ================================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        loss_segment    unique identifier of a loss segment on an interconnector basis (as `np.float64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.float64`)
        ==============  ================================================================================

    next_variable_id : int

    Returns
    -------
    weight_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        loss_segment    unique identifier of a loss segment on an interconnector basis (as `np.float64`)
        break_points    the interconnector flow values to interpolate losses between (as `np.int64`)
        variable_id     the id of the variable (as `np.int64`)
        lower_bound    the lower bound of the variable, is zero for weight variables (as `np.float64`)
        upper_bound    the upper bound of the variable, is one for weight variables (as `np.float64`)
        type           the type of variable, is continuous for bids  (as `str`)
        ==============  ==============================================================================
    """
    # Create a variable for each break point.
    weight_variables = hf.save_index(break_points, 'variable_id', next_variable_id)
    weight_variables['lower_bound'] = 0.0
    weight_variables['upper_bound'] = 1.0
    weight_variables['type'] = 'continuous'
    return weight_variables


def create_loss_variables(inter_variables, inter_constraint_map, loss_shares, next_variable_id):
    """

    Examples
    --------

    Setup function inputs

    >>> inter_variables = pd.DataFrame({
    ...   'interconnector': ['I'],
    ...   'variable_id': [0],
    ...   'lower_bound': [-50.0],
    ...   'upper_bound': [100.0],
    ...   'type': ['continuous']})

    >>> inter_constraint_map = pd.DataFrame({
    ... 'variable_id': [0, 0],
    ... 'region': ['X', 'Y'],
    ... 'service': ['energy', 'energy'],
    ... 'coefficient': [1.0, -1.0]})

    >>> loss_shares = pd.DataFrame({
    ...    'interconnector': ['I'],
    ...    'from_region_loss_share': [0.5]})

    >>> next_constraint_id = 0

    Create the constraints.

    >>> loss_variables, constraint_map = create_loss_variables(inter_variables, inter_constraint_map, loss_shares,
    ...                                                        next_constraint_id)

    >>> print(loss_variables)
      interconnector  variable_id  lower_bound  upper_bound        type
    0              I            0       -100.0        100.0  continuous

    >>> print(constraint_map)
       variable_id region service  coefficient
    0            0      X  energy         -0.5
    1            0      Y  energy         -0.5


    Parameters
    ----------
    inter_variables : pd.DataFrame

        ==============  ==============================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        lower_bound     the lower bound of the variable, the min interconnector flow (as `np.float64`)
        upper_bound     the upper bound of the variable, the max inerconnector flow (as `np.float64`)
        type            the type of variable, is 'continuous' for interconnectors losses  (as `str`)
        ==============  ==============================================================================

    inter_constraint_map : pd.DataFrame

        =============  ==========================================================================
        Columns:       Description:
        variable_id  the id of the variable (as `np.int64`)
        region         the regional variables the variable should map too (as `str`)
        service        the service type of the constraints the variable should map to (as `str`)
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  ==========================================================================

    loss_shares : pd.DataFrame

        ======================  ==============================================================================
        Columns:                Description:
        interconnector          unique identifier of a interconnector (as `str`)
        from_region_loss_share  The fraction of loss occuring in the from region, 0.0 to 1.0 (as `np.float64`)
        ======================  ==============================================================================

    next_variable_id : int

    Returns
    -------
    loss_variables : pd.DataFrame

        ==============  ===============================================================================================
        Columns:        Description:
        interconnector  unique identifier of a interconnector (as `str`)
        variable_id     the id of the variable (as `np.int64`)
        lower_bound     the lower bound of the variable, negative of the absolute max of inter flow (as `np.float64`)
        upper_bound     the upper bound of the variable, the absolute max of inter flow (as `np.float64`)
        type            the type of variable, is continuous for interconnectors  (as `str`)
        ==============  ===============================================================================================

    constraint_map : pd.DataFrame

        =============  ==========================================================================
        Columns:       Description:
        variable_id  the id of the variable (as `np.int64`)
        region         the regional variables the variable should map too (as `str`)
        service        the service type of the constraints the variable should map to (as `str`)
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  ==========================================================================
    """

    # Preserve the interconnector variable id for merging later.
    columns_for_loss_variables = \
        inter_variables.loc[:, ['interconnector', 'variable_id', 'lower_bound', 'upper_bound', 'type']]
    columns_for_loss_variables.columns = ['interconnector', 'inter_variable_id', 'lower_bound', 'upper_bound', 'type']
    columns_for_loss_variables['upper_bound'] = \
        columns_for_loss_variables.loc[:, ['lower_bound', 'upper_bound']].abs().max(axis=1)
    columns_for_loss_variables['lower_bound'] = -1 * columns_for_loss_variables['upper_bound']

    inter_constraint_map = inter_constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]
    inter_constraint_map.columns = ['inter_variable_id', 'region', 'service', 'coefficient']

    # Create a variable id for loss variables
    loss_variables = hf.save_index(columns_for_loss_variables, 'variable_id', next_variable_id)
    loss_variables = pd.merge(loss_variables,
                              loss_shares.loc[:, ['inter_variable_id', 'from_region_loss_share', 'from_region']],
                              on=['inter_variable_id'])

    # Create the loss variable constraint map by combining the new variables and the flow variable constraint map.
    constraint_map = pd.merge(
        loss_variables.loc[:, ['variable_id', 'inter_variable_id', 'interconnector', 'from_region_loss_share',
                               'from_region']],
        inter_constraint_map, 'inner', on='inter_variable_id')

    # Assign losses to regions according to the from_region_loss_share
    constraint_map['coefficient'] = np.where(constraint_map['from_region'] == constraint_map['region'],
                                             - 1 * constraint_map['from_region_loss_share'],
                                             - 1 * (1 - constraint_map['from_region_loss_share']))

    loss_variables = loss_variables.loc[:, ['interconnector', 'variable_id', 'inter_variable_id', 'lower_bound',
                                            'upper_bound', 'type']]
    constraint_map = constraint_map.loc[:, ['variable_id', 'region', 'service', 'coefficient']]
    return loss_variables, constraint_map
