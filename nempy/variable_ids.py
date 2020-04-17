from nempy import helper_functions as hf


def energy(capacity_bids, next_variable_id):
    """Create decision variables that correspond to unit bids, for use in the linear program.

    This function defines the needed parameters for each variable, with a lower bound equal to zero, an upper bound
    equal to the bid volume, and a variable type of continuous. Bids that have a volume of zero are ignored and no
    variable is created. There is no limit on the number of bid bands and each column in the capacity_bids DataFrame
    other than unit is treated as a bid band. Volume bids should be positive numeric values only.

    Parameters
    ----------
    volume_bids : pd.DataFrame
        Bids by unit, in MW, can contain up to n bid bands.

        ========  ======================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        1         bid volume in the 1st band, in MW (as `float`)
        2         bid volume in the 2nd band, in MW (as `float`)
        n         bid volume in the nth band, in MW (as `float`)
        ========  ======================================================

    next_variable_id : int
        The next integer to start using for variables ids.

    Returns
    -------
    pd.DataFrame

        =============  ===============================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        capacity_band  the bid band of the variable (as `str`)
        variable_id    the id of the variable (as `int`)
        lower_bound    the lower bound of the variable, is zero for bids (as `float`)
        upper_bound    the upper bound of the variable, the volume bid (as `float`)
        type           the type of variable, is continuous for bids  (as `str`)
        =============  ===============================================================
    """
    # Get the list of columns that are bid bands.
    bid_bands = [col for col in capacity_bids.columns if col != 'unit']
    # Reshape the DataFrame such that all bids are stacked vertical e.g
    # from:
    # unit 1  2
    # A    10 20
    # B    10 10
    #
    # to:
    # unit capacity_band upper_bound
    # A    1             10
    # A    2             20
    # B    1             10
    # B    2             10
    stacked_bids = hf.stack_columns(capacity_bids, cols_to_keep=['unit'], cols_to_stack=bid_bands,
                                    type_name='capacity_band', value_name='upper_bound')
    # Remove any bids that are upper bounded at zero.
    stacked_bids = stacked_bids[stacked_bids['upper_bound'] > 0.0]
    # Order the DataFrame such that one unit bids get sequential ids.
    stacked_bids = stacked_bids.sort_values(['unit', 'capacity_band'])
    stacked_bids = stacked_bids.reset_index(drop=True)
    # Create the ids and other decision variable properties.
    stacked_bids = hf.save_index(stacked_bids, 'variable_id', next_variable_id)
    stacked_bids['lower_bound'] = 0.0
    stacked_bids['type'] = 'continuous'
    return stacked_bids
