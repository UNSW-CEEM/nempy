import pandas as pd


class DemandData:
    """Loads demand related raw data and preprocess it for complatibility with the SpotMarket class.

    Examples
    --------

    The DemandData class requries a RawInputsLoader instance.

    >>> import sqlite3
    >>> from nempy.historical_inputs import mms_db
    >>> from nempy.historical_inputs import xml_cache
    >>> from nempy.historical_inputs import loaders
    >>> con = sqlite3.connect('market_management_system.db')
    >>> mms_db_manager = mms_db.DBManager(connection=con)
    >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    >>> inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
    >>> inputs_loader.set_interval('2024/07/10 12:05:00')

    >>> demand_data = DemandData(inputs_loader)

    >>> demand_data.get_operational_demand()
      region   demand
    0   NSW1  6624.81
    1   QLD1  4750.17
    2    SA1   934.59
    3   TAS1  1260.71
    4   VIC1  5390.51

    Parameters
    ----------
    raw_inputs_loader
    """

    def __init__(self, raw_inputs_loader):
        self.raw_inputs_loader = raw_inputs_loader

    def get_operational_demand(self):
        """Get the operational demand used to determine the regional energy dispatch constraints.

        Examples
        --------

        See class level example.

        Returns
        -------
        pd.DataFrame

            ====================  ====================================
            Columns:              Description:
            region                unique identifier of a market region, \n
                                  (as `str`)
            demand                the non dispatchable demand the region, \n
                                  in MW, (as `np.float64`)
            loss_function_demand  the measure of demand used when creating \n
                                  interconnector loss functions, in MW, \n
                                  (as `np.float64`)
            ====================  ====================================


        """
        DISPATCHREGIONSUM = self.raw_inputs_loader.get_regional_loads()
        regional_demand = _format_regional_demand(DISPATCHREGIONSUM)
        return regional_demand.loc[:, ['region', 'demand']]


def _format_regional_demand(DISPATCHREGIONSUM):
    """Re-formats the AEMO MSS table DISPATCHREGIONSUM to be compatible with the SpotMarket class.

    Examples
    --------
    >>> DISPATCHREGIONSUM = pd.DataFrame({
    ... 'REGIONID': ['NSW1', 'SA1'],
    ... 'TOTALDEMAND': [8000.0, 4000.0],
    ... 'DEMANDFORECAST': [10.0, -10.0],
    ... 'INITIALSUPPLY': [7995.0, 4006.0]})

    >>> regional_demand = _format_regional_demand(DISPATCHREGIONSUM)

    >>> print(regional_demand)
      region  demand  loss_function_demand
    0   NSW1  8000.0                8005.0
    1    SA1  4000.0                3996.0
    """

    DISPATCHREGIONSUM['loss_function_demand'] = DISPATCHREGIONSUM['INITIALSUPPLY'] + DISPATCHREGIONSUM['DEMANDFORECAST']
    regional_demand = DISPATCHREGIONSUM.loc[:, ['REGIONID', 'TOTALDEMAND', 'loss_function_demand']]
    regional_demand.columns = ['region', 'demand', 'loss_function_demand']
    return regional_demand
