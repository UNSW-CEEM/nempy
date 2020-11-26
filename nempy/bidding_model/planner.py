import pandas as pd

from nempy.bidding_model import model_interface


class DispatchPlanner:
    def __init__(self, dispatch_interval, planning_horizon, model):
        pass

    def add_energy_market_model(self, sample, forecast):
        pass

    def add_flow(self, flow_definition):
        pass

    def set_storage_size(self, mwh):
        pass


def _create_profit_function(sample):
    """

    Examples
    --------
    >>> sample = pd.DataFrame({
    ...    'demand': [1000.0, 15000.0, 1750.0, 2000.0],
    ...    'output': [0.0, 50.0, 100.0, 75.0],
    ...    'price':  [100.0, 200.0, 250.0, 500.0]
    })

    Parameters
    ----------
    sample

    Returns
    -------

    """