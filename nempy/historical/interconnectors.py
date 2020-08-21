import pandas as pd
import numpy as np

from nempy import historical_spot_market_inputs as hi


class InterconnectorData:
    """Class for creating interconnector inputs for historical dispatch intervals.

    Examples
    --------

    For this example we use a fake DBManager. In production use the historical_spot_market_inputs.DBManager class.

    >>> input_manager = FakeDBManager()

    >>> interconnector_inputs = HistoricalInterconnectors(input_manager, '2019/01/01 00:00:00')

    >>> interconnector_inputs.add_loss_model()

    >>> interconnector_inputs.add_market_interconnector_transmission_loss_factors()

    >>> interconnector_inputs.split_bass_link_to_enable_dynamic_from_region_loss_shares()

    >>> interconnectors = interconnector_inputs.get_interconnector_definitions()

    >>> print(interconnectors)
          interconnector  ... to_region_loss_factor
    1                  Y  ...                  1.00
    0  T-V-MNSP1_forward  ...                  0.75
    0  T-V-MNSP1_reverse  ...                  0.75
    <BLANKLINE>
    [3 rows x 7 columns]


    >>> loss_function, interpolation_break_points  = interconnector_inputs.get_interconnector_loss_model()

    >>> print(loss_function.loc[:, ['interconnector', 'loss_function']])
          interconnector                                      loss_function
    1                  Y  <function create_function.<locals>.loss_functi...
    0  T-V-MNSP1_forward  <function create_function.<locals>.loss_functi...
    0  T-V-MNSP1_reverse  <function create_function.<locals>.loss_functi...

    >>> print(loss_function.loc[:, ['interconnector', 'from_region_loss_share']])
          interconnector  from_region_loss_share
    1                  Y                     0.3
    0  T-V-MNSP1_forward                     0.5
    0  T-V-MNSP1_reverse                     0.5

    Parameters
    ----------
    inputs_manager : historical_spot_market_inputs.DBManager
    interval : str
    """
    def __init__(self, raw_input_loader):
        self.raw_input_loader = raw_input_loader

        self.INTERCONNECTORCONSTRAINT = self.raw_input_loader.get_interconnector_constraint_parameters()

        self.INTERCONNECTORCONSTRAINT['FROMREGIONLOSSSHARE'] = \
            np.where(self.INTERCONNECTORCONSTRAINT['INTERCONNECTORID'] == 'T-V-MNSP1', 1.0,
                     self.INTERCONNECTORCONSTRAINT['FROMREGIONLOSSSHARE'])

        self.INTERCONNECTOR = self.raw_input_loader.get_interconnector_definitions()

        self.interconnectors = format_interconnector_definitions(self.INTERCONNECTOR, self.INTERCONNECTORCONSTRAINT)


        self.splitting_used = False
        self.transmission_loss_factors_added = False
        self.interpolation_break_points = None
        self.loss_functions = None

    def add_loss_model(self):
        """Retrieve data from the input manager and format it to create the inputs for adding losses to interconnectors.

        Returns
        -------

        Raises
        ------
        OrderError : If this method is called after splitting bass link into forward and reverse interconnectors.

        """
        if self.splitting_used:
            raise OrderError('Loss model must be added before splitting bass link.')

        DISPATCHREGIONSUM = self.raw_input_loader.get_regional_loads()
        LOSSFACTORMODEL = self.raw_input_loader.get_interconnector_loss_paramteters()
        LOSSMODEL = self.raw_input_loader.get_interconnector_loss_segments()

        regional_demand = hi.format_regional_demand(DISPATCHREGIONSUM)
        interconnector_loss_coefficients = format_interconnector_loss_coefficients(self.INTERCONNECTORCONSTRAINT)
        interconnector_demand_coefficients = format_interconnector_loss_demand_coefficient(LOSSFACTORMODEL)
        self.interpolation_break_points = format_interpolation_break_points(LOSSMODEL)
        self.loss_functions = create_loss_functions(interconnector_loss_coefficients,
                                                    interconnector_demand_coefficients,
                                                    regional_demand.loc[:, ['region', 'loss_function_demand']])

    def get_interconnector_definitions(self):
        """Returns inputs in the format needed to create interconnectors in the Spot market class.

        Examples
        --------

        For this example we use a fake DBManager. In production use the historical_spot_market_inputs.DBManager class.

        >>> input_manager = FakeDBManager()

        >>> interconnector_inputs = HistoricalInterconnectors(input_manager, '2019/01/01 00:00:00')

        >>> interconnectors = interconnector_inputs.get_interconnector_definitions()

        >>> print(interconnectors)
          interconnector from_region to_region    min    max
        0      T-V-MNSP1         TAS       VIC -100.0  150.0
        1              Y         VIC        SA -900.0  800.0

        Returns
        -------
        pd.DataFrame

            ========================  ==================================================================================
            Columns:                  Description:
            interconnector            unique identifier of a interconnector (as `str`)
            to_region                 the region that receives power when flow is in the positive direction (as `str`)
            from_region               the region that power is drawn from when flow is in the positive direction
                                      (as `str`)
            max                       the maximum power flow in the positive direction, in MW (as `np.float64`)
            min                       the maximum power flow in the negative direction, in MW (as `np.float64`)
            from_region_loss_factor   the loss factor between the from end of the interconnector and the regional
                                      reference node. Only returned if the
                                      add_market_interconnector_transmission_loss_factors has been used. (as `np.float`)
            to_region_loss_factor     the loss factor between the to end of the interconnector and the regional
                                      reference node. Only returned if the
                                      add_market_interconnector_transmission_loss_factors has been used. (as `np.float`)
            ========================  ==================================================================================

        """
        regulated_interconnectors = \
            self.INTERCONNECTORCONSTRAINT[self.INTERCONNECTORCONSTRAINT['ICTYPE'] == 'REGULATED']['INTERCONNECTORID']
        interconnectors = self.interconnectors[self.interconnectors['interconnector'].isin(regulated_interconnectors)]
        return interconnectors

    def get_interconnector_loss_model(self):
        """Returns inputs in the format needed to set interconnector losses in the Spot market class.

        Examples
        --------

        For this example we use a fake DBManager. In production use the historical_spot_market_inputs.DBManager class.

        >>> input_manager = FakeDBManager()

        >>> interconnector_inputs = HistoricalInterconnectors(input_manager, '2019/01/01 00:00:00')

        >>> interconnector_inputs.add_loss_model()

        >>> loss_function, interpolation_break_points  = interconnector_inputs.get_interconnector_loss_model()

        >>> print(loss_function.loc[:, ['interconnector', 'loss_function']])
          interconnector                                      loss_function
        0      T-V-MNSP1  <function create_function.<locals>.loss_functi...
        1              Y  <function create_function.<locals>.loss_functi...

        >>> print(loss_function.loc[:, ['interconnector', 'from_region_loss_share']])
          interconnector  from_region_loss_share
        0      T-V-MNSP1                     0.5
        1              Y                     0.3

        Returns
        -------

        loss_functions : pd.DataFrame

            ======================  ==============================================================================
            Columns:                Description:
            interconnector          unique identifier of a interconnector (as `str`)
            from_region_loss_share  The fraction of loss occuring in the from region, 0.0 to 1.0 (as `np.float64`)
            loss_function           A function that takes a flow, in MW as a float and returns the losses in MW
                                    (as `callable`)
            ======================  ==============================================================================

        interpolation_break_points : pd.DataFrame

            ==============  ============================================================================================
            Columns:        Description:
            interconnector  unique identifier of a interconnector (as `str`)
            loss_segment    unique identifier of a loss segment on an interconnector basis (as `np.float64`)
            break_point     points between which the loss function will be linearly interpolated, in MW
                            (as `np.float64`)
            ==============  ============================================================================================

        Raises
        ------
        OrderError : If this method is called before the add_loss_model method.

        """
        if self.loss_functions is not None:
            return self.loss_functions, self.interpolation_break_points
        else:
            raise OrderError('Loss model must be added before calling get_interconnector_loss_model.')

    def split_bass_link_to_enable_dynamic_from_region_loss_shares(self):
        """Split bass link into two interconnectors, one for each direction, allows the from region loss share to be
        relative to the actual direction of flow.

        Examples
        --------

        For this example we use a fake DBManager. In production use the historical_spot_market_inputs.DBManager class.

        >>> input_manager = FakeDBManager()

        >>> interconnector_inputs = HistoricalInterconnectors(input_manager, '2019/01/01 00:00:00')

        >>> interconnectors = interconnector_inputs.split_bass_link_to_enable_dynamic_from_region_loss_shares()

        >>> interconnectors = interconnector_inputs.get_interconnector_definitions()

        >>> print(interconnectors)
              interconnector from_region to_region    min    max
        1                  Y         VIC        SA -900.0  800.0
        0  T-V-MNSP1_forward         TAS       VIC    0.0  150.0
        0  T-V-MNSP1_reverse         TAS       VIC -100.0    0.0

        """

        bass_link, interconnectors = split_out_bass_link(self.interconnectors)
        bass_link = split_interconnectors_definitions_into_two_one_directional_links(bass_link)
        bass_link['max'] = np.where(bass_link['interconnector'] == 'T-V-MNSP1_forward', 478.0, bass_link['max'])
        self.interconnectors = pd.concat([interconnectors, bass_link])

        if self.loss_functions is not None:
            bass_link, loss_functions = split_out_bass_link(self.loss_functions)
            bass_link = split_interconnector_loss_functions_into_two_directional_links(bass_link)
            self.loss_functions = pd.concat([loss_functions, bass_link])

            bass_link, interpolation_break_points = split_out_bass_link(self.interpolation_break_points)
            bass_link = split_interconnector_interpolation_break_points_into_two_directional_links(bass_link)
            self.interpolation_break_points = pd.concat([interpolation_break_points, bass_link])

        self.splitting_used = True

    def get_market_interconnector_links(self):
        mnsp_bids = self.raw_input_loader.get_market_interconnector_link_bid_availability()
        MNSP_INTERCONNECTOR = self.raw_input_loader.get_market_interconnectors()
        mnsp_transmission_loss_factors = format_mnsp_transmission_loss_factors(MNSP_INTERCONNECTOR,
                                                                               self.INTERCONNECTORCONSTRAINT)
        mnsp_transmission_loss_factors = pd.merge(mnsp_transmission_loss_factors, mnsp_bids, on=['interconnector', 'to_region'])
        mnsp_transmission_loss_factors['max'] = np.where(~mnsp_transmission_loss_factors['availability'].isna(),
                                                         mnsp_transmission_loss_factors['availability'],
                                                         mnsp_transmission_loss_factors['max'])
        return mnsp_transmission_loss_factors.drop(columns=['availability'])



class OrderError(Exception):
    """Raise for using class methods in invalid order."""


class FakeDBManager:
    """For testing the HistoricalInterconnectors class."""
    def __init__(self):
        INTERCONNECTOR = pd.DataFrame({
            'INTERCONNECTORID': ['T-V-MNSP1', 'Y'],
            'REGIONFROM': ['TAS', 'VIC'],
            'REGIONTO': ['VIC', 'SA']})
        self.INTERCONNECTOR = FakeTable(INTERCONNECTOR)
        INTERCONNECTORCONSTRAINT = pd.DataFrame({
            'INTERCONNECTORID': ['T-V-MNSP1', 'Y'],
            'IMPORTLIMIT': [100.0, 900.0],
            'EXPORTLIMIT': [150.0, 800.0],
            'LOSSCONSTANT': [1.0, 1.1],
            'LOSSFLOWCOEFFICIENT': [0.001, 0.003],
            'FROMREGIONLOSSSHARE': [0.5, 0.3],
            'ICTYPE': ['MNSP', 'REGULATED']})
        self.INTERCONNECTORCONSTRAINT = FakeTable(INTERCONNECTORCONSTRAINT)
        MNSP_INTERCONNECTOR = pd.DataFrame({
            'INTERCONNECTORID': ['T-V-MNSP1', 'T-V-MNSP1'],
            'LINKID': ['A1', 'A2'],
            'FROMREGION': ['TAS', 'VIC'],
            'TOREGION': ['VIC', 'TAS'],
            'FROM_REGION_TLF': [1.0, 0.75],
            'TO_REGION_TLF': [0.75, 1.0]})
        self.MNSP_INTERCONNECTOR = FakeTable(MNSP_INTERCONNECTOR)
        LOSSFACTORMODEL = pd.DataFrame({
            'INTERCONNECTORID': ['T-V-MNSP1', 'Y', 'Y'],
            'REGIONID': ['A', 'B', 'C'],
            'DEMANDCOEFFICIENT': [0.001, 0.003, 0.005]})
        self.LOSSFACTORMODEL = FakeTable(LOSSFACTORMODEL)
        LOSSMODEL = pd.DataFrame({
            'INTERCONNECTORID': ['T-V-MNSP1', 'T-V-MNSP1', 'T-V-MNSP1', 'Y', 'Y', 'Y'],
            'LOSSSEGMENT': [1, 2, 3, 1, 2, 3],
            'MWBREAKPOINT': [-10.0, 0.0, 10.0, -5.0, 0.0, 5.0]})
        self.LOSSMODEL = FakeTable(LOSSMODEL)
        DISPATCHREGIONSUM = pd.DataFrame({
            'REGIONID': ['A', 'B', 'C'],
            'TOTALDEMAND': [8000.0, 4000.0, 3000.0],
            'DEMANDFORECAST': [10.0, -10.0, -20.0],
            'INITIALSUPPLY': [7995.0, 4006.0, 3020.0]})
        self.DISPATCHREGIONSUM = FakeTable(DISPATCHREGIONSUM)


class FakeTable:
    def __init__(self, df):
        self.df = df

    def get_data(self, interval):
        return self.df


def format_interconnector_definitions(INTERCONNECTOR, INTERCONNECTORCONSTRAINT):
    """
    Examples
    --------

    >>> INTERCONNECTOR = pd.DataFrame({
    ... 'INTERCONNECTORID': ['X', 'Y'],
    ... 'REGIONFROM': ['NSW', 'VIC'],
    ... 'REGIONTO': ['QLD', 'SA']})

    >>> INTERCONNECTORCONSTRAINT = pd.DataFrame({
    ... 'INTERCONNECTORID': ['X', 'Y'],
    ... 'IMPORTLIMIT': [100.0, 900.0],
    ... 'EXPORTLIMIT': [150.0, 800.0]})

    >>> interconnector_paramaters = format_interconnector_definitions(INTERCONNECTOR, INTERCONNECTORCONSTRAINT)

    >>> print(interconnector_paramaters)
      interconnector to_region from_region    min    max
    0              X       NSW         QLD -100.0  150.0
    1              Y       VIC          SA -900.0  800.0
    """
    interconnector_directions = INTERCONNECTOR.loc[:, ['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO']]
    interconnector_directions.columns = ['interconnector', 'from_region', 'to_region']
    interconnector_paramaters = INTERCONNECTORCONSTRAINT.loc[:, ['INTERCONNECTORID', 'IMPORTLIMIT', 'EXPORTLIMIT']]
    interconnector_paramaters.columns = ['interconnector', 'min', 'max']
    interconnector_paramaters['min'] = -1 * interconnector_paramaters['min']
    interconnectors = pd.merge(interconnector_directions, interconnector_paramaters, 'inner', on='interconnector')
    return interconnectors


def split_out_bass_link(interconnectors):
    bass_link = interconnectors[interconnectors['interconnector'] == 'T-V-MNSP1']
    interconnectors = interconnectors[interconnectors['interconnector'] != 'T-V-MNSP1']
    return bass_link, interconnectors


def split_interconnectors_definitions_into_two_one_directional_links(interconnectors):
    """
    Examples
    --------

    >>> interconnectors = pd.DataFrame({
    ...   'interconnector': ['inter_one', 'inter_two', 'another_inter'],
    ...   'to_region': ['A', 'B', 'C'],
    ...   'from_region': ['X', 'Y', 'Z'],
    ...    'max': [100.0, -50.0, 80.0],
    ...    'min': [-100.0, -100.0, 80.0]})

    >>> split_parameters = split_interconnectors_definitions_into_two_one_directional_links(interconnectors)

    >>> print(split_parameters)
              interconnector to_region from_region    max    min
    0      inter_one_forward         A           X  100.0    0.0
    1      inter_two_forward         B           Y    0.0    0.0
    2  another_inter_forward         C           Z   80.0   80.0
    0      inter_one_reverse         A           X    0.0 -100.0
    1      inter_two_reverse         B           Y  -50.0 -100.0
    2  another_inter_reverse         C           Z    0.0    0.0

    """
    interconnector_forward_direction = create_forward_flow_interconnectors(interconnectors)
    interconnector_reverse_direction = create_reverse_flow_interconnectors(interconnectors)

    interconnector_forward_direction['max'] = \
        np.where(interconnector_forward_direction['max'] > 0, interconnector_forward_direction['max'], 0.0)
    interconnector_forward_direction['min'] = \
        np.where(interconnector_forward_direction['min'] > 0, interconnector_forward_direction['min'], 0.0)

    interconnector_reverse_direction['max'] = \
        np.where(interconnector_reverse_direction['max'] > 0, 0, interconnector_reverse_direction['max'])
    interconnector_reverse_direction['min'] = \
        np.where(interconnector_reverse_direction['min'] < 0, interconnector_reverse_direction['min'], 0.0)

    interconnectors = pd.concat([interconnector_forward_direction, interconnector_reverse_direction])
    return interconnectors


def split_interconnector_flow_into_two_directional_links(interconnector_flow):
    """
    Examples
    --------

    >>> interconnector_flow = pd.DataFrame({
    ...   'interconnector': ['inter_one', 'another_inter'],
    ...   'flow': [10.0, -10.0]})

    >>> interconnector_flow = split_interconnector_loss_functions_into_two_directional_links(interconnector_flow)

    >>> print(interconnector_flow)
              interconnector
    0      inter_one_nominal
    1  another_inter_nominal
    0      inter_one_reverse
    1  another_inter_reverse

    """
    interconnector_flow_forward_direction = \
        create_forward_flow_interconnectors(interconnector_flow)
    interconnector_flow_reverse_direction = \
        create_reverse_flow_interconnectors(interconnector_flow)

    interconnector_flow_forward_direction['flow'] = \
        np.where(interconnector_flow_forward_direction['flow'] > 0.0,
                 interconnector_flow_forward_direction['flow'], 0.0)
    interconnector_flow_reverse_direction['flow'] = \
        np.where(interconnector_flow_reverse_direction['flow'] < 0.0,
                 interconnector_flow_reverse_direction['flow'], 0.0)

    interconnector_parameters = pd.concat([interconnector_flow_forward_direction,
                                           interconnector_flow_reverse_direction])
    return interconnector_parameters


def split_interconnector_loss_functions_into_two_directional_links(interconnector_loss_functions):
    """
    Examples
    --------
    >>> interconnector_loss_functions = pd.DataFrame({
    ...   'interconnector': ['inter_one', 'another_inter'],
    ...   'from_region_loss_share': [0.0, 0.75]})

    >>> interconnector_loss_functions = split_interconnector_loss_functions_into_two_directional_links(
    ...   interconnector_loss_functions)

    >>> print(interconnector_loss_functions)
              interconnector  from_region_loss_share
    0      inter_one_forward                    0.00
    1  another_inter_forward                    0.75
    0      inter_one_reverse                    1.00
    1  another_inter_reverse                    0.25

    """
    interconnector_loss_functions_forward_direction = \
        create_forward_flow_interconnectors(interconnector_loss_functions)
    interconnector_loss_functions_reverse_direction = \
        create_reverse_flow_interconnectors(interconnector_loss_functions)

    interconnector_loss_functions_forward_direction['from_region_loss_share'] = \
        1 - interconnector_loss_functions_forward_direction['from_region_loss_share']

    interconnectors = pd.concat([interconnector_loss_functions_forward_direction,
                                 interconnector_loss_functions_reverse_direction])
    return interconnectors


def split_interconnector_interpolation_break_points_into_two_directional_links(interpolation_break_points):
    """
    Examples
    --------

    >>> interpolation_break_points = pd.DataFrame({
    ...   'interconnector': ['inter_one', 'inter_one', 'inter_one',
    ...                      'another_inter', 'another_inter', 'another_inter'],
    ...   'loss_segement': [1, 2, 3, 1, 2, 3],
    ...   'break_point': [-10.0, 0.0, 15.0, -18.0, 0.0, 18.0]})

    >>> interpolation_break_points = split_interconnector_interpolation_break_points_into_two_directional_links(
    ...   interpolation_break_points)

    >>> print(interpolation_break_points)
              interconnector  loss_segement  break_point
    1      inter_one_forward              2          0.0
    2      inter_one_forward              3         15.0
    4  another_inter_forward              2          0.0
    5  another_inter_forward              3         18.0
    0      inter_one_reverse              1        -10.0
    1      inter_one_reverse              2          0.0
    3  another_inter_reverse              1        -18.0
    4  another_inter_reverse              2          0.0

    Parameters
    ----------
    interpolation_break_points

    Returns
    -------

    """
    interpolation_break_points_forward_direction = \
        create_forward_flow_interconnectors(interpolation_break_points)
    interpolation_break_points_reverse_direction = \
        create_reverse_flow_interconnectors(interpolation_break_points)

    interpolation_break_points_forward_direction = \
        interpolation_break_points_forward_direction[
            interpolation_break_points_forward_direction['break_point'] >= -0.001]

    interpolation_break_points_reverse_direction = \
        interpolation_break_points_reverse_direction[
            interpolation_break_points_reverse_direction['break_point'] <= 0.001]

    interconnectors = pd.concat([interpolation_break_points_forward_direction,
                                 interpolation_break_points_reverse_direction])
    return interconnectors


def create_forward_flow_interconnectors(interconnectors):
    """
    Examples
    --------

    >>> interconnectors = pd.DataFrame({
    ...   'interconnector': ['inter_one', 'another_inter']})

    >>> interconnectors = create_forward_flow_interconnectors(interconnectors)

    >>> print(interconnectors)
              interconnector
    0      inter_one_forward
    1  another_inter_forward

    """
    interconnectors = interconnectors.copy()
    interconnectors['interconnector'] = interconnectors['interconnector'] + '_forward'
    return interconnectors


def create_reverse_flow_interconnectors(interconnectors):
    """
    Examples
    --------

    >>> interconnectors = pd.DataFrame({
    ...   'interconnector': ['inter_one', 'another_inter']})

    >>> interconnectors = create_reverse_flow_interconnectors(interconnectors)

    >>> print(interconnectors)
              interconnector
    0      inter_one_reverse
    1  another_inter_reverse

    """
    interconnectors = interconnectors.copy()
    interconnectors['interconnector'] = interconnectors['interconnector'] + '_reverse'
    return interconnectors


def add_inerconnector_transmission_loss_factors(interconnectors, mnsp_transmission_loss_factors):
    """
    Examples
    --------

    >>> interconnectors = pd.DataFrame({
    ...   'interconnector': ['A', 'B']})

    >>> mnsp_transmission_loss_factors = pd.DataFrame({
    ...   'interconnector': ['A'],
    ...   'from_region_loss_factor': [0.0],
    ...   'to_region_loss_factor': [0.9]})

    >>> interconnectors = add_inerconnector_transmission_loss_factors(interconnectors, mnsp_transmission_loss_factors)

    >>> print(interconnectors)
      interconnector  from_region_loss_factor  to_region_loss_factor
    0              A                      0.0                    0.9
    1              B                      0.0                    0.0

    """
    mnsp_transmission_loss_factors = \
        mnsp_transmission_loss_factors.loc[:, ['interconnector', 'from_region_loss_factor', 'to_region_loss_factor',
                                               'to_region', 'from_region']]
    interconnectors = pd.merge(interconnectors, mnsp_transmission_loss_factors, 'left',
                               on=['interconnector', 'to_region', 'from_region'])
    interconnectors = interconnectors.fillna(1.0)
    return interconnectors


def format_mnsp_transmission_loss_factors(MNSP_INTERCONNECTOR, INTERCONNECTORCONSTRAINT):
    """
    Examples
    --------

    >>> MNSP_INTERCONNECTOR = pd.DataFrame({
    ...   'INTERCONNECTORID': ['A', 'A'],
    ...   'LINKID': ['A1', 'A2'],
    ...   'FROMREGION': ['C', 'X'],
    ...   'TOREGION': ['X', 'C'],
    ...   'FROM_REGION_TLF': [0.0, 0.75],
    ...   'TO_REGION_TLF': [0.75, 0.0]})

    >>> INTERCONNECTORCONSTRAINT = pd.DataFrame({
    ...   'INTERCONNECTORID': ['A', 'B'],
    ...   'ICTYPE': ['MNSP', 'REGULATED']})

    >>> mnsp_transmission_loss_factors = format_mnsp_transmission_loss_factors(MNSP_INTERCONNECTOR,
    ...   INTERCONNECTORCONSTRAINT)

    >>> print(mnsp_transmission_loss_factors.loc[:, ['interconnector', 'link_id', 'from_region', 'to_region']])
      interconnector link_id from_region to_region
    0              A      A1           C         X
    1              A      A2           X         C

    >>> print(mnsp_transmission_loss_factors.loc[:, ['interconnector', 'link_id', 'from_region_loss_factor',
    ...   'to_region_loss_factor']])
          interconnector link_id  from_region_loss_factor  to_region_loss_factor
    0              A      A1                     0.00                   0.75
    1              A      A2                     0.75                   0.00

    """
    INTERCONNECTORCONSTRAINT = INTERCONNECTORCONSTRAINT[INTERCONNECTORCONSTRAINT['ICTYPE'] == 'MNSP']
    MNSP_INTERCONNECTOR = pd.merge(MNSP_INTERCONNECTOR, INTERCONNECTORCONSTRAINT, on=['INTERCONNECTORID'])
    MNSP_INTERCONNECTOR = MNSP_INTERCONNECTOR.loc[:, ['INTERCONNECTORID', 'LINKID', 'FROM_REGION_TLF', 'TO_REGION_TLF',
                                                      'FROMREGION', 'TOREGION', 'LHSFACTOR', 'MAXCAPACITY']]
    mnsp_transmission_loss_factors = MNSP_INTERCONNECTOR.rename(columns={
        'INTERCONNECTORID': 'interconnector', 'LINKID': 'link', 'FROM_REGION_TLF': 'from_region_loss_factor',
        'TO_REGION_TLF': 'to_region_loss_factor', 'FROMREGION': 'from_region', 'TOREGION': 'to_region',
        'LHSFACTOR': 'generic_constraint_factor', 'MAXCAPACITY': 'max'
    })
    mnsp_transmission_loss_factors['min'] = 0.0
    return mnsp_transmission_loss_factors


def format_interconnector_loss_coefficients(INTERCONNECTORCONSTRAINT):
    """Re-formats the AEMO MSS table INTERCONNECTORCONSTRAINT to be compatible with the Spot market class.

    Examples
    --------

    >>> INTERCONNECTORCONSTRAINT = pd.DataFrame({
    ... 'INTERCONNECTORID': ['X', 'Y', 'Z'],
    ... 'LOSSCONSTANT': [1.0, 1.1, 1.0],
    ... 'LOSSFLOWCOEFFICIENT': [0.001, 0.003, 0.005],
    ... 'FROMREGIONLOSSSHARE': [0.5, 0.3, 0.7]})

    >>> interconnector_parameters = format_interconnector_loss_coefficients(INTERCONNECTORCONSTRAINT)

    >>> print(interconnector_parameters)
      interconnector  loss_constant  flow_coefficient  from_region_loss_share
    0              X            1.0             0.001                     0.5
    1              Y            1.1             0.003                     0.3
    2              Z            1.0             0.005                     0.7


    Parameters
    ----------
    INTERCONNECTORCONSTRAINT : pd.DataFrame

        ===================  =======================================================================================
        Columns:             Description:
        INTERCONNECTORID     unique identifier of a interconnector (as `str`)
        LOSSCONSTANT         the constant term in the interconnector loss factor equation (as np.float64)
        LOSSFLOWCOEFFICIENT  the coefficient of the interconnector flow variable in the loss factor equation (as np.float64)
        FROMREGIONLOSSSHARE  the proportion of loss attribute to the from region, remainder is attributed to the to region (as np.float64)
        ===================  =======================================================================================

    Returns
    ----------
    interconnector_parameters : pd.DataFrame

        ======================  ========================================================================================
        Columns:                Description:
        interconnector          unique identifier of a interconnector (as `str`)
        loss_constant           the constant term in the interconnector loss factor equation (as np.float64)
        flow_coefficient        the coefficient of the interconnector flow variable in the loss factor equation (as np.float64)
        from_region_loss_share  the proportion of loss attribute to the from region, remainer are attributed to the to region (as np.float64)
        ======================  ========================================================================================
    """

    interconnector_parameters = INTERCONNECTORCONSTRAINT.loc[:, ['INTERCONNECTORID', 'LOSSCONSTANT',
                                                                 'LOSSFLOWCOEFFICIENT', 'FROMREGIONLOSSSHARE']]
    interconnector_parameters.columns = ['interconnector', 'loss_constant', 'flow_coefficient',
                                         'from_region_loss_share']
    return interconnector_parameters


def format_interconnector_loss_demand_coefficient(LOSSFACTORMODEL):
    """Re-formats the AEMO MSS table LOSSFACTORMODEL to be compatible with the Spot market class.

    Examples
    --------

    >>> LOSSFACTORMODEL = pd.DataFrame({
    ... 'INTERCONNECTORID': ['X', 'X', 'X', 'Y', 'Y'],
    ... 'REGIONID': ['A', 'B', 'C', 'C', 'D'],
    ... 'DEMANDCOEFFICIENT': [0.001, 0.003, 0.005, 0.0001, 0.002]})

    >>> demand_coefficients = format_interconnector_loss_demand_coefficient(LOSSFACTORMODEL)

    >>> print(demand_coefficients)
      interconnector region  demand_coefficient
    0              X      A              0.0010
    1              X      B              0.0030
    2              X      C              0.0050
    3              Y      C              0.0001
    4              Y      D              0.0020


    Parameters
    ----------
    LOSSFACTORMODEL : pd.DataFrame

        =================  ======================================================================================
        Columns:           Description:
        INTERCONNECTORID   unique identifier of a interconnector (as `str`)
        REGIONID           unique identifier of a market region (as `str`)
        DEMANDCOEFFICIENT  the coefficient of regional demand variable in the loss factor equation (as `np.float64`)
        =================  ======================================================================================

    Returns
    ----------
    demand_coefficients : pd.DataFrame

        ==================  =========================================================================================
        Columns:            Description:
        interconnector      unique identifier of a interconnector (as `str`)
        region              the market region whose demand the coefficient applies too, required (as `str`)
        demand_coefficient  the coefficient of regional demand variable in the loss factor equation (as `np.float64`)
        ==================  =========================================================================================
    """
    demand_coefficients = LOSSFACTORMODEL.loc[:, ['INTERCONNECTORID', 'REGIONID', 'DEMANDCOEFFICIENT']]
    demand_coefficients.columns = ['interconnector', 'region', 'demand_coefficient']
    return demand_coefficients


def format_interpolation_break_points(LOSSMODEL):
    """Re-formats the AEMO MSS table LOSSMODEL to be compatible with the Spot market class.

    Examples
    --------

    >>> LOSSMODEL = pd.DataFrame({
    ... 'INTERCONNECTORID': ['X', 'X', 'X', 'X', 'X'],
    ... 'LOSSSEGMENT': [1, 2, 3, 4, 5],
    ... 'MWBREAKPOINT': [-100.0, -50.0, 0.0, 50.0, 100.0]})

    >>> interpolation_break_points = format_interpolation_break_points(LOSSMODEL)

    >>> print(interpolation_break_points)
      interconnector  loss_segment  break_point
    0              X             1       -100.0
    1              X             2        -50.0
    2              X             3          0.0
    3              X             4         50.0
    4              X             5        100.0

    Parameters
    ----------
    LOSSMODEL : pd.DataFrame

        ================  ======================================================================================
        Columns:          Description:
        INTERCONNECTORID  unique identifier of a interconnector (as `str`)
        LOSSSEGMENT       unique identifier of a loss segment on an interconnector basis (as `np.int64`)
        MWBREAKPOINT      points between which the loss function will be linearly interpolated, in MW
                          (as `np.float64`)
        ================  ======================================================================================

    Returns
    ----------
    interpolation_break_points : pd.DataFrame

        ================  ======================================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        break_point       points between which the loss function will be linearly interpolated, in MW (as `np.float64`)
        ================  ======================================================================================
    """

    interpolation_break_points = LOSSMODEL.loc[:, ['INTERCONNECTORID', 'LOSSSEGMENT', 'MWBREAKPOINT']]
    interpolation_break_points.columns = ['interconnector', 'loss_segment', 'break_point']
    interpolation_break_points['loss_segment'] = interpolation_break_points['loss_segment'].apply(np.int64)
    return interpolation_break_points


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
    ...   'loss_function_demand': [6000.0 , 7000.0, 5000.0, 3000.0]})

    Loss model details from 2020 Jan NEM web LOSSFACTORMODEL file

    >>> demand_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'NSW1-QLD1', 'VIC1-NSW1', 'VIC1-NSW1', 'VIC1-NSW1'],
    ...   'region': ['NSW1', 'QLD1', 'NSW1', 'VIC1', 'SA1'],
    ...   'demand_coefficient': [-0.00000035146, 0.000010044, 0.000021734, -0.000031523, -0.000065967]})

    Loss model details from 2020 Jan NEM web INTERCONNECTORCONSTRAINT file

    >>> interconnector_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'VIC1-NSW1'],
    ...   'loss_constant': [0.9529, 1.0657],
    ...   'flow_coefficient': [0.00019617, 0.00017027],
    ...   'from_region_loss_share': [0.5, 0.5]})

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

        ======================  ========================================================================================
        Columns:                Description:
        interconnector          unique identifier of a interconnector (as `str`)
        loss_constant           the constant term in the interconnector loss factor equation (as np.float64)
        flow_coefficient        the coefficient of the interconnector flow variable in the loss factor equation
                                (as np.float64)
        from_region_loss_share  the proportion of loss attribute to the from region, remainer are attributed to the to
                                region (as np.float64)
        ======================  ========================================================================================

    demand_coefficients : pd.DataFrame

        ==================  =========================================================================================
        Columns:            Description:
        interconnector      unique identifier of a interconnector (as `str`)
        region              the market region whose demand the coefficient applies too, required (as `str`)
        demand_coefficient  the coefficient of regional demand variable in the loss factor equation (as `np.float64`)
        ==================  =========================================================================================

    demand : pd.DataFrame

        ====================  =====================================================================================
        Columns:              Description:
        region                unique identifier of a region (as `str`)
        loss_function_demand  the estimated regional demand, as calculated by initial supply + demand forecast,
                              in MW (as `np.float64`)
        ====================  =====================================================================================

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
    demand_loss_factor_offset['offset'] = demand_loss_factor_offset['loss_function_demand'] * \
                                          demand_loss_factor_offset['demand_coefficient']
    demand_loss_factor_offset = demand_loss_factor_offset.groupby('interconnector', as_index=False)['offset'].sum()
    loss_functions = pd.merge(interconnector_coefficients, demand_loss_factor_offset, 'left', on=['interconnector'])
    loss_functions['loss_constant'] = loss_functions['loss_constant'] + loss_functions['offset'].fillna(0)
    loss_functions['loss_function'] = \
        loss_functions.apply(lambda x: create_function(x['loss_constant'], x['flow_coefficient']), axis=1)
    return loss_functions.loc[:, ['interconnector', 'loss_function', 'from_region_loss_share']]


def create_function(constant, flow_coefficient):
    def loss_function(flow):
        return (constant - 1) * flow + (flow_coefficient / 2) * flow ** 2

    return loss_function
