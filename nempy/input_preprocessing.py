import pandas as pd


def create_loss_functions(interconnector_coefficients, demand_coefficients, demand):
    """Creates a loss function for each interconnector.

    Transforms the dynamic demand dependendent interconnector loss functions into functions that only depend on
    interconnector flow. i.e takes the function f and creates g by pre-calculating the demand dependent terms.

        f(inter_flow, flow_coefficient, nsw_demand, nsw_coefficient, qld_demand, qld_coefficient) = inter_losses

    becomes

        g(inter_flow) = inter_losses

    The mathematics of the demand dependent loss functions is described in the
    :download:`Marginal Loss Factors documentation section 3 to 5  <../../docs/pdfs/Marginal Loss Factors for the 2020-21 Financial year.pdf>`.

    Examples
    --------
    >>> import pandas as pd

    Some arbitrary regional demands.

    >>> demand = pd.DataFrame({
    ...   'region': ['VIC1', 'NSW1', 'QLD1', 'SA1'],
    ...   'demand': [6000.0 , 7000.0, 5000.0, 3000.0]})

    Loss model details from 2020 Jan NEM web LOSSFACTORMODEL file

    >>> demand_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'NSW1-QLD1', 'VIC1-NSW1', 'VIC1-NSW1', 'VIC1-NSW1'],
    ...   'region': ['NSW1', 'QLD1', 'NSW1', 'VIC1', 'SA1'],
    ...   'demand_coefficient': [-0.00000035146, 0.000010044, 0.000021734, -0.000031523, -0.000065967]})

    Loss model details from 2020 Jan NEM web INTERCONNECTORCONSTRAINT file

    >>> interconnector_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'VIC1-NSW1'],
    ...   'loss_constant': [0.9529, 1.0657],
    ...   'flow_coefficient': [0.00019617, 0.00017027]})

    Create the loss functions

    >>> loss_functions = create_loss_functions(interconnector_coefficients, demand_coefficients, demand)

    Lets use one of the loss functions, first get the loss function of VIC1-NSW1 and call it g

    >>> g = loss_functions[loss_functions['interconnector'] == 'VIC1-NSW1']['loss_function'].iloc[0]

    Calculate the losses at 600 MW flow

    >>> print(g(600.0))
    -70.87199999999996

    Now for NSW1-QLD1

    >>> h = loss_functions[loss_functions['interconnector'] == 'NSW1-QLD1']['loss_function'].iloc[0]

    >>> print(h(600.0))
    35.70646799999993

    Parameters
    ----------
    interconnector_coefficients : pd.DataFrame

        ================  ============================================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        loss_constant     the constant term in the interconnector loss factor equation (as np.float64)
        flow_coefficient  the coefficient of interconnector flow variable in the loss factor equation (as np.float64)
        ================  ============================================================================================

    demand_coefficients : pd.DataFrame

        ==================  =========================================================================================
        Columns:            Description:
        interconnector      unique identifier of a interconnector (as `str`)
        region              the market region whose demand the coefficient applies too, required (as `str`)
        demand_coefficient  the coefficient of regional demand variable in the loss factor equation (as `np.float64`)
        ==================  =========================================================================================

    demand : pd.DataFrame

        ========  =====================================================================================
        Columns:  Description:
        region    unique identifier of a region (as `str`)
        demand    the estimated regional demand, as calculated by initial supply + demand forecast,
                  in MW (as `np.float64`)
        ========  =====================================================================================

    Returns
    -------
    pd.DataFrame

        loss_functions

        ================  ============================================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        loss_function     a `function` object that takes interconnector flow (as `float`) an input and returns
                          interconnector losses (as `float`).
        ================  ============================================================================================




    """

    demand_loss_factor_offset = pd.merge(demand_coefficients, demand, 'inner', on=['region'])
    demand_loss_factor_offset['offset'] = demand_loss_factor_offset['demand'] * \
                                          demand_loss_factor_offset['demand_coefficient']
    demand_loss_factor_offset = demand_loss_factor_offset.groupby('interconnector', as_index=False)['offset'].sum()
    loss_functions = pd.merge(interconnector_coefficients, demand_loss_factor_offset, 'left', on=['interconnector'])
    loss_functions['loss_constant'] = loss_functions['loss_constant'] + loss_functions['offset'].fillna(0)
    loss_functions['loss_function'] = \
        loss_functions.apply(lambda x: create_function(x['loss_constant'], x['flow_coefficient']), axis=1)
    return loss_functions.loc[:, ['interconnector', 'loss_function']]


def create_function(constant, flow_coefficient):
    def loss_function(flow):
        return (constant - 1) * flow + (flow_coefficient/2) * flow ** 2
    return loss_function