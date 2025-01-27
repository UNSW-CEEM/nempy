import pandas as pd
import numpy as np
from nempy.help_functions import helper_functions as hf


def bids(volume_bids, unit_info, next_variable_id, bidirectional_units):
    """Create decision variables that correspond to unit bids, for use in the linear program.

    This function defines the needed parameters for each variable, with a lower bound equal to zero, an upper bound
    equal to the bid volume, and a variable type of continuous. There is no limit on the number of bid bands and each
    column in the capacity_bids DataFrame other than unit is treated as a bid band. Volume bids should be positive.
    numeric values only.

    Examples
    --------

    >>> import pandas

    A set of capacity bids.

    >>> volume_bids = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'dispatch_type': ['generator', 'load'],
    ...   '1': [10.0, 50.0],
    ...   '2': [20.0, 30.0]})

    The locations of the units.

    >>> unit_info = pd.DataFrame({
    ...   'unit': ['A', 'B'],
    ...   'dispatch_type': ['generator', 'load'],
    ...   'region': ['X', 'Y']
    ... })

    >>> next_variable_id = 0

    >>> bidirectional_units = []

    Create the decision variables and their mapping into constraints.

    >>> decision_variables, unit_level_constraint_map, regional_constraint_map = bids(
    ...   volume_bids, unit_info, next_variable_id, bidirectional_units)

    >>> print(decision_variables)
      unit capacity_band service dispatch_type  variable_id  lower_bound  upper_bound        type
    0    A             1  energy     generator            0          0.0         10.0  continuous
    1    A             2  energy     generator            1          0.0         20.0  continuous
    2    B             1  energy          load            2          0.0         50.0  continuous
    3    B             2  energy          load            3          0.0         30.0  continuous

    >>> print(unit_level_constraint_map)
       variable_id unit service dispatch_type  coefficient
    0            0    A  energy     generator          1.0
    1            1    A  energy     generator          1.0
    2            2    B  energy          load          1.0
    3            3    B  energy          load          1.0

    >>> print(regional_constraint_map)
       variable_id region service dispatch_type  coefficient
    0            0      X  energy     generator          1.0
    1            1      X  energy     generator          1.0
    2            2      Y  energy          load         -1.0
    3            3      Y  energy          load         -1.0


    Parameters
    ----------
    volume_bids : pd.DataFrame
        Bids by unit, in MW, can contain up to n bid bands.

        ========  ===============================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        service   the service being provided, optional, if missing energy assumed
                  (as `str`)

        1         bid volume in the 1st band, in MW (as `float`)
        2         bid volume in the 2nd band, in MW (as `float`)
        n         bid volume in the nth band, in MW (as `float`)
        ========  ===============================================================

    unit_info : pd.DataFrame
        The region each unit is located in.

        ========  ======================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        region    unique identifier of a market region (as `str`)
        ========  ======================================================

    next_variable_id : int
        The next integer to start using for variables ids.

    Returns
    -------
    decision_variables : pd.DataFrame

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        capacity_band  the bid band of the variable (as `str`)
        variable_id    the id of the variable (as `int`)
        lower_bound    the lower bound of the variable, is zero for bids (as `np.float64`)
        upper_bound    the upper bound of the variable, the volume bid (as `np.float64`)
        type           the type of variable, is continuous for bids  (as `str`)
        =============  ===============================================================

    unit_level_constraint_map : pd.DataFrame

        =============  =============================================================================
        Columns:       Description:
        variable_id    the id of the variable (as `np.int64`)
        unit           the unit level constraints the variable should map to (as `str`)
        service        the service type of the constraints the variables should map to (as `str`)
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  =============================================================================

    regional_constraint_map : pd.DataFrame

        =============  =============================================================================
        Columns:       Description:
        variable_id    the id of the variable (as `np.int64`)
        region         the regional constraints the variable should map to (as `str`)
        service        the service type of the constraints the variables should map to (as `str`)
        coefficient    the upper bound of the variable, the volume bid (as `np.float64`)
        =============  =============================================================================
    """
    # If no service column is provided assume bids are for energy.
    if 'service' not in volume_bids.columns:
        volume_bids['service'] = 'energy'

    if "dispatch_type" not in volume_bids.columns:
        volume_bids['dispatch_type'] = 'generator'

    bid_id_columns = ['unit', 'service', 'dispatch_type']

    # Get a list of all the columns that contain volume bids.
    bid_bands = [col for col in volume_bids.columns if col not in bid_id_columns]
    # Reshape the table so each bid band is on it own row.
    decision_variables = hf.stack_columns(volume_bids, cols_to_keep=bid_id_columns,
                                          cols_to_stack=bid_bands, type_name='capacity_band', value_name='upper_bound')
    decision_variables = decision_variables[decision_variables['upper_bound'] >= 0.0001]
    # Group units together in the decision variable table.
    decision_variables = decision_variables.sort_values(['unit', 'capacity_band'])
    # Create a unique identifier for each decision variable.
    decision_variables = hf.save_index(decision_variables, 'variable_id', next_variable_id)
    # The lower bound of bidding decision variables will always be zero.
    decision_variables['lower_bound'] = 0.0
    decision_variables['type'] = 'continuous'

    constraint_map = decision_variables.loc[:, ['variable_id'] + bid_id_columns]

    constraint_map = pd.merge(
        constraint_map,
        unit_info.loc[:, ['unit', 'dispatch_type', 'region']],
        'inner',
        on=['unit', 'dispatch_type']
    )

    regional_constraint_map = constraint_map.loc[:,  ['variable_id', 'region', 'service', 'dispatch_type']]
    regional_constraint_map['coefficient'] = np.where((regional_constraint_map['dispatch_type'] == 'load') &
                                                      (regional_constraint_map['service'] == 'energy'), -1.0, 1.0)

    unit_level_constraint_map = constraint_map.loc[:,  ['variable_id', 'unit', 'service', 'dispatch_type']]
    unit_level_constraint_map['coefficient'] = np.where(
        unit_level_constraint_map['unit'].isin(bidirectional_units) &
        (unit_level_constraint_map['service'] == 'energy') &
        (unit_level_constraint_map['dispatch_type'] == 'load'),
        -1.0,
        1.0
    )

    decision_variables = \
        decision_variables.loc[:, ['unit', 'capacity_band', 'service', 'dispatch_type', 'variable_id', 'lower_bound',
                                   'upper_bound', 'type']]

    return decision_variables, unit_level_constraint_map, regional_constraint_map
