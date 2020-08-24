import pandas as pd


class DemandData:
    def __init__(self, raw_inputs_loader):
        self.raw_inputs_loader = raw_inputs_loader

    def get_operational_demand(self):
        DISPATCHREGIONSUM = self.raw_inputs_loader.get_regional_loads()
        regional_demand = format_regional_demand(DISPATCHREGIONSUM)
        return regional_demand.loc[:, ['region', 'demand']]


def format_regional_demand(DISPATCHREGIONSUM):
    """Re-formats the AEMO MSS table DISPATCHREGIONSUM to be compatible with the Spot market class.

    Note the demand term used in the interconnector loss functions is calculated by summing the initial supply and the
    demand forecast.

    Examples
    --------

    >>> DISPATCHREGIONSUM = pd.DataFrame({
    ... 'REGIONID': ['NSW1', 'SA1'],
    ... 'TOTALDEMAND': [8000.0, 4000.0],
    ... 'DEMANDFORECAST': [10.0, -10.0],
    ... 'INITIALSUPPLY': [7995.0, 4006.0]})

    >>> regional_demand = format_regional_demand(DISPATCHREGIONSUM)

    >>> print(regional_demand)
      region  demand  loss_function_demand
    0   NSW1  8000.0                8005.0
    1    SA1  4000.0                3996.0

    Parameters
    ----------
    DISPATCHREGIONSUM : pd.DataFrame

        ================  ==========================================================================================
        Columns:          Description:
        REGIONID          unique identifier of a market region (as `str`)
        TOTALDEMAND       the non dispatchable demand the region, in MW (as `np.float64`)
        INITIALSUPPLY     the generation supplied in th region at the start of the interval, in MW (as `np.float64`)
        DEMANDFORECAST    the expected change in demand over dispatch interval, in MW (as `np.float64`)
        ================  ==========================================================================================

    Returns
    ----------
    regional_demand : pd.DataFrame

        ====================  ======================================================================================
        Columns:              Description:
        region                unique identifier of a market region (as `str`)
        demand                the non dispatchable demand the region, in MW (as `np.float64`)
        loss_function_demand  the measure of demand used when creating interconnector loss functions, in MW (as `np.float64`)
        ====================  ======================================================================================
    """

    DISPATCHREGIONSUM['loss_function_demand'] = DISPATCHREGIONSUM['INITIALSUPPLY'] + DISPATCHREGIONSUM['DEMANDFORECAST']
    regional_demand = DISPATCHREGIONSUM.loc[:, ['REGIONID', 'TOTALDEMAND', 'loss_function_demand']]
    regional_demand.columns = ['region', 'demand', 'loss_function_demand']
    return regional_demand



