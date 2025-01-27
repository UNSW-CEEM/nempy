import pandas as pd
import numpy as np

from nempy.historical_inputs import demand, aemo_to_nempy_name_mapping


def _test_setup():
    import sqlite3
    from nempy.historical_inputs import mms_db
    from nempy.historical_inputs import xml_cache
    from nempy.historical_inputs import loaders
    con = sqlite3.connect('market_management_system.db')
    mms_db_manager = mms_db.DBManager(connection=con)
    xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
    inputs_loader.set_interval('2024/07/10 12:05:00')
    return inputs_loader


class InterconnectorData:
    """Loads interconnector related raw inputs and preprocess them for compatibility with :class:`nempy.markets.SpotMarket`

    Examples
    --------

    This example shows the setup used for the examples in the class methods. This setup is used to create a
    RawInputsLoader by calling the function _test_setup.

    >>> import sqlite3
    >>> from nempy.historical_inputs import mms_db
    >>> from nempy.historical_inputs import xml_cache
    >>> from nempy.historical_inputs import loaders

    The InterconnectorData class requries a RawInputsLoader instance.

    >>> con = sqlite3.connect('market_management_system.db')
    >>> mms_db_manager = mms_db.DBManager(connection=con)
    >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    >>> inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
    >>> inputs_loader.set_interval('2024/07/10 12:05:00')

    Create a InterconnectorData instance.

    >>> interconnector_data = InterconnectorData(inputs_loader)

    >>> interconnector_data.get_interconnector_definitions()
      interconnector from_region to_region     min     max       link  from_region_loss_factor  to_region_loss_factor  generic_constraint_factor
    0      N-Q-MNSP1        NSW1      QLD1  -264.0   264.0  N-Q-MNSP1                   1.0000                 1.0000                          1
    1      NSW1-QLD1        NSW1      QLD1 -2478.0  2204.0  NSW1-QLD1                   1.0000                 1.0000                          1
    3      V-S-MNSP1        VIC1       SA1  -270.0   270.0  V-S-MNSP1                   1.0000                 1.0000                          1
    4           V-SA        VIC1       SA1  -850.0   950.0       V-SA                   1.0000                 1.0000                          1
    5      VIC1-NSW1        VIC1      NSW1 -2299.0  2399.0  VIC1-NSW1                   1.0000                 1.0000                          1
    0      T-V-MNSP1        TAS1      VIC1     0.0   594.0    BLNKTAS                   1.0000                 0.9777                          1
    1      T-V-MNSP1        VIC1      TAS1     0.0   478.0    BLNKVIC                   0.9852                 1.0000                         -1

    Parameters
    ----------
    inputs_manager : historical_spot_market_inputs.DBManager
    """
    def __init__(self, raw_input_loader):
        self.raw_input_loader = raw_input_loader

        self.INTERCONNECTORCONSTRAINT = self.raw_input_loader.get_interconnector_constraint_parameters()

        # The from region loss share for Basslink is not properly defined in the AEMO data sources, for nempy to best
        # replicate NEMDE outcomes the from region loss share is set to one.
        self.INTERCONNECTORCONSTRAINT['FROMREGIONLOSSSHARE'] = \
            np.where(self.INTERCONNECTORCONSTRAINT['INTERCONNECTORID'] == 'T-V-MNSP1', 1.0,
                     self.INTERCONNECTORCONSTRAINT['FROMREGIONLOSSSHARE'])

        self.INTERCONNECTOR = self.raw_input_loader.get_interconnector_definitions()

        self.interconnectors = _format_interconnector_definitions(self.INTERCONNECTOR, self.INTERCONNECTORCONSTRAINT)

    def get_interconnector_loss_model(self):
        """Returns inputs in the format needed to set interconnector losses in the SpotMarket class.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> interconnector_data = InterconnectorData(inputs_loader)
        
        >>> loss_function, interpolation_break_points = \
             interconnector_data.get_interconnector_loss_model()

        >>> print(loss_function)
          interconnector       link                                      loss_function  from_region_loss_share
        0      N-Q-MNSP1  N-Q-MNSP1  <function InterconnectorData.get_interconnecto...                    0.70
        1      NSW1-QLD1  NSW1-QLD1  <function InterconnectorData.get_interconnecto...                    0.63
        2      V-S-MNSP1  V-S-MNSP1  <function InterconnectorData.get_interconnecto...                    0.70
        3           V-SA       V-SA  <function InterconnectorData.get_interconnecto...                    0.67
        4      VIC1-NSW1  VIC1-NSW1  <function InterconnectorData.get_interconnecto...                    0.36
        5      T-V-MNSP1    BLNKTAS  <function InterconnectorData.get_interconnecto...                    1.00
        6      T-V-MNSP1    BLNKVIC  <function InterconnectorData.get_interconnecto...                    1.00

        >>> print(interpolation_break_points)
            interconnector       link  loss_segment  break_point
        0        N-Q-MNSP1  N-Q-MNSP1             1       -265.0
        1        N-Q-MNSP1  N-Q-MNSP1             2       -257.0
        2        N-Q-MNSP1  N-Q-MNSP1             3       -249.0
        3        N-Q-MNSP1  N-Q-MNSP1             4       -241.0
        4        N-Q-MNSP1  N-Q-MNSP1             5       -233.0
        ..             ...        ...           ...          ...
        611      T-V-MNSP1    BLNKVIC           -80       -546.0
        612      T-V-MNSP1    BLNKVIC           -81       -559.0
        613      T-V-MNSP1    BLNKVIC           -82       -571.0
        614      T-V-MNSP1    BLNKVIC           -83       -583.0
        615      T-V-MNSP1    BLNKVIC           -84       -595.0
        <BLANKLINE>
        [616 rows x 4 columns]

        Multiple Returns
        ----------------

        loss_functions : pd.DataFrame

            ======================  ==================================
            Columns:                Description:
            interconnector          unique identifier of a interconnector, \n
                                    (as `str`)
            from_region_loss_share  The fraction of loss occuring in \n
                                    the from region, 0.0 to 1.0, \n
                                    (as `np.float64`)
            loss_function           A function that takes a flow, \n
                                    in MW as a float and returns the \n
                                    losses in MW, (as `callable`)
            ======================  ==================================

        interpolation_break_points : pd.DataFrame

            ==============  ==========================================
            Columns:        Description:
            interconnector  unique identifier of a interconnector, \n
                            (as `str`)
            loss_segment    unique identifier of a loss segment on \n
                            an interconnector basis, (as `np.float64`)
            break_point     points between which the loss function \n
                            will be linearly interpolated, in MW \n
                            (as `np.float64`)
            ==============  ==========================================

        """

        DISPATCHREGIONSUM = self.raw_input_loader.get_regional_loads()
        LOSSFACTORMODEL = self.raw_input_loader.get_interconnector_loss_parameters()
        LOSSMODEL = self.raw_input_loader.get_interconnector_loss_segments()

        regional_demand = demand._format_regional_demand(DISPATCHREGIONSUM)

        interconnector_loss_coefficients = \
            self.INTERCONNECTORCONSTRAINT.loc[:, ['INTERCONNECTORID', 'LOSSCONSTANT', 'LOSSFLOWCOEFFICIENT',
                                                  'FROMREGIONLOSSSHARE']]
        interconnector_loss_coefficients = aemo_to_nempy_name_mapping.map_aemo_column_names_to_nempy_names(
            interconnector_loss_coefficients)

        interconnector_demand_coefficients = LOSSFACTORMODEL.loc[:, ['INTERCONNECTORID', 'REGIONID',
                                                                     'DEMANDCOEFFICIENT']]
        interconnector_demand_coefficients = aemo_to_nempy_name_mapping.map_aemo_column_names_to_nempy_names(
            interconnector_demand_coefficients)


        interpolation_break_points = LOSSMODEL.loc[:, ['INTERCONNECTORID', 'LOSSSEGMENT', 'MWBREAKPOINT']]
        interpolation_break_points = aemo_to_nempy_name_mapping.map_aemo_column_names_to_nempy_names(
            interpolation_break_points)
        interpolation_break_points['loss_segment'] = interpolation_break_points['loss_segment'].apply(np.int64)


        loss_functions = create_loss_functions(interconnector_loss_coefficients,
                                               interconnector_demand_coefficients,
                                                regional_demand.loc[:, ['region', 'loss_function_demand']])

        interconnectors = self.get_interconnector_definitions()

        interpolation_break_points = pd.merge(interconnectors.loc[:, ['interconnector', 'link',
                                                                      'generic_constraint_factor']],
                                              interpolation_break_points, on='interconnector')

        interpolation_break_points['break_point'] = interpolation_break_points['break_point'] * \
            interpolation_break_points['generic_constraint_factor']
        interpolation_break_points['loss_segment'] = interpolation_break_points['loss_segment'] * \
            interpolation_break_points['generic_constraint_factor']
        interpolation_break_points = interpolation_break_points.drop('generic_constraint_factor', axis=1)

        loss_functions = pd.merge(interconnectors.loc[:, ['interconnector', 'link', 'generic_constraint_factor']],
                                  loss_functions, on='interconnector')

        def loss_function_adjuster(loss_function, generic_constraint_factor):
            def wrapper(flow):
                return loss_function(flow * generic_constraint_factor)
            return wrapper

        loss_functions['loss_function'] = \
            loss_functions.apply(lambda x: loss_function_adjuster(x['loss_function'], x['generic_constraint_factor']),
                                 axis=1)

        loss_functions = loss_functions.drop('generic_constraint_factor', axis=1)
        
        return loss_functions, interpolation_break_points

    def get_interconnector_definitions(self):
        """Returns inputs in the format needed to create interconnectors in the SpotMarket class.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> interconnector_data = InterconnectorData(inputs_loader)

        >>> interconnector_data.get_interconnector_definitions()
          interconnector from_region to_region     min     max       link  from_region_loss_factor  to_region_loss_factor  generic_constraint_factor
        0      N-Q-MNSP1        NSW1      QLD1  -264.0   264.0  N-Q-MNSP1                   1.0000                 1.0000                          1
        1      NSW1-QLD1        NSW1      QLD1 -2478.0  2204.0  NSW1-QLD1                   1.0000                 1.0000                          1
        3      V-S-MNSP1        VIC1       SA1  -270.0   270.0  V-S-MNSP1                   1.0000                 1.0000                          1
        4           V-SA        VIC1       SA1  -850.0   950.0       V-SA                   1.0000                 1.0000                          1
        5      VIC1-NSW1        VIC1      NSW1 -2299.0  2399.0  VIC1-NSW1                   1.0000                 1.0000                          1
        0      T-V-MNSP1        TAS1      VIC1     0.0   594.0    BLNKTAS                   1.0000                 0.9777                          1
        1      T-V-MNSP1        VIC1      TAS1     0.0   478.0    BLNKVIC                   0.9852                 1.0000                         -1

        Returns
        -------
        pd.DataFrame

            ========================  ================================
            Columns:                  Description:
            interconnector            unique identifier of a interconnector, \n
                                      (as `str`)
            to_region                 the region that receives power \n
                                      when flow is in the positive \n
                                      direction, (as `str`)
            from_region               the region that power is drawn \n
                                      from when flow is in the \n
                                      positive direction, (as `str`)
            max                       the maximum power flow on the \n
                                      interconnector, in MW (as `np.float64`)
            min                       the minimum power flow on the \n
                                      interconnector, if power can flow \n
                                      neative direction then this will be \n
                                      negative, in MW (as `np.float64`)
            from_region_loss_factor   the loss factor between the from \n
                                      end of the interconnector and the \n
                                      regional reference node, \n
                                      (as `np.float`)
            to_region_loss_factor     the loss factor between the to \n
                                      end of the interconnector and \n
                                      the regional reference node, \n
                                      (as `np.float`)
            ========================  ================================

        """
        regulated_interconnectors_series = \
            self.INTERCONNECTORCONSTRAINT[self.INTERCONNECTORCONSTRAINT['ICTYPE'] == 'REGULATED'].loc[:, 'INTERCONNECTORID']
        regulated_interconnectors = \
            self.interconnectors[self.interconnectors['interconnector'].isin(regulated_interconnectors_series)].copy()

        regulated_interconnectors['link'] = regulated_interconnectors['interconnector']
        regulated_interconnectors['from_region_loss_factor'] = 1.0
        regulated_interconnectors['to_region_loss_factor'] = 1.0
        regulated_interconnectors['generic_constraint_factor'] = 1

        market_interconnectors = self._get_market_interconnector_links()

        interconnectors = pd.concat([regulated_interconnectors, market_interconnectors])

        interconnectors['generic_constraint_factor'] = interconnectors['generic_constraint_factor'].astype(np.int64)

        return interconnectors

    def _get_market_interconnector_links(self):
        mnsp_bids = self.raw_input_loader.get_market_interconnector_link_bid_availability()
        MNSP_INTERCONNECTOR = self.raw_input_loader.get_market_interconnectors()
        mnsp_transmission_loss_factors = _format_mnsp_transmission_loss_factors(MNSP_INTERCONNECTOR,
                                                                                self.INTERCONNECTORCONSTRAINT)
        mnsp_transmission_loss_factors = \
            pd.merge(mnsp_transmission_loss_factors, mnsp_bids, on=['interconnector', 'to_region'])
        mnsp_transmission_loss_factors['max'] = np.where(~mnsp_transmission_loss_factors['availability'].isna(),
                                                         mnsp_transmission_loss_factors['availability'],
                                                         mnsp_transmission_loss_factors['max'])
        return mnsp_transmission_loss_factors.drop(columns=['availability'])


def _format_interconnector_definitions(INTERCONNECTOR, INTERCONNECTORCONSTRAINT):
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

    >>> interconnector_paramaters = _format_interconnector_definitions(INTERCONNECTOR, INTERCONNECTORCONSTRAINT)

    >>> print(interconnector_paramaters)
      interconnector from_region to_region    min    max
    0              X         NSW       QLD -100.0  150.0
    1              Y         VIC        SA -900.0  800.0

    """
    interconnector_directions = INTERCONNECTOR.loc[:, ['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO']]
    interconnector_directions.columns = ['interconnector', 'from_region', 'to_region']
    interconnector_paramaters = INTERCONNECTORCONSTRAINT.loc[:, ['INTERCONNECTORID', 'IMPORTLIMIT', 'EXPORTLIMIT']]
    interconnector_paramaters.columns = ['interconnector', 'min', 'max']
    interconnector_paramaters['min'] = -1 * interconnector_paramaters['min']
    interconnectors = pd.merge(interconnector_directions, interconnector_paramaters, 'inner', on='interconnector')
    return interconnectors


def _format_mnsp_transmission_loss_factors(MNSP_INTERCONNECTOR, INTERCONNECTORCONSTRAINT):
    """
    Examples
    --------

    >>> MNSP_INTERCONNECTOR = pd.DataFrame({
    ...   'INTERCONNECTORID': ['A', 'A'],
    ...   'LINKID': ['A1', 'A2'],
    ...   'FROMREGION': ['C', 'X'],
    ...   'TOREGION': ['X', 'C'],
    ...   'FROM_REGION_TLF': [0.0, 0.75],
    ...   'TO_REGION_TLF': [0.75, 0.0],
    ...   'LHSFACTOR': [1.0, 0.9],
    ...   'MAXCAPACITY': [100.0, 200.0]})

    >>> INTERCONNECTORCONSTRAINT = pd.DataFrame({
    ...   'INTERCONNECTORID': ['A', 'B'],
    ...   'ICTYPE': ['MNSP', 'REGULATED']})

    >>> mnsp_transmission_loss_factors = _format_mnsp_transmission_loss_factors(MNSP_INTERCONNECTOR,
    ...   INTERCONNECTORCONSTRAINT)

    >>> print(mnsp_transmission_loss_factors.loc[:, ['interconnector', 'link', 'from_region', 'to_region']])
      interconnector link from_region to_region
    0              A   A1           C         X
    1              A   A2           X         C

    >>> print(mnsp_transmission_loss_factors.loc[:, ['interconnector', 'link', 'from_region_loss_factor',
    ...   'to_region_loss_factor']])
      interconnector link  from_region_loss_factor  to_region_loss_factor
    0              A   A1                     0.00                   0.75
    1              A   A2                     0.75                   0.00

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
    ...   'interconnector': ['NSW1-QLD1', 'NSW1-QLD1', 'VIC1-NSW1',
    ...                      'VIC1-NSW1', 'VIC1-NSW1'],
    ...   'region': ['NSW1', 'QLD1', 'NSW1', 'VIC1', 'SA1'],
    ...   'demand_coefficient': [-0.00000035146, 0.000010044,
    ...                           0.000021734, -0.000031523,
    ...                          -0.000065967]})

    Loss model details from 2020 Jan NEM web INTERCONNECTORCONSTRAINT file

    >>> interconnector_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'VIC1-NSW1'],
    ...   'loss_constant': [0.9529, 1.0657],
    ...   'flow_coefficient': [0.00019617, 0.00017027],
    ...   'from_region_loss_share': [0.5, 0.5]})

    Create the loss functions

    >>> loss_functions = create_loss_functions(interconnector_coefficients,
    ...                                        demand_coefficients, demand)

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

        ======================  ======================================
        Columns:                Description:
        interconnector          unique identifier of a interconnector, \n
                                (as `str`)
        loss_constant           the constant term in the interconnector \n
                                loss factor equation, (as `np.float64`)
        flow_coefficient        the coefficient of the interconnector \n
                                flow variable in the loss factor equation \n
                                (as `np.float64`)
        from_region_loss_share  the proportion of loss attribute to the \n
                                from region, remainer are attributed to \n
                                the to region, (as `np.float64`)
        ======================  ======================================

    demand_coefficients : pd.DataFrame

        ==================  ==========================================
        Columns:            Description:
        interconnector      unique identifier of a interconnector, \n
                            (as `str`)
        region              the market region whose demand the coefficient \n
                            applies too (as `str`)
        demand_coefficient  the coefficient of regional demand variable \n
                            in the loss factor equation, (as `np.float64`)
        ==================  ==========================================

    demand : pd.DataFrame

        ====================  ========================================
        Columns:              Description:
        region                unique identifier of a region, (as `str`)
        loss_function_demand  the estimated regional demand, as calculated \n
                              by initial supply + demand forecast, \n
                              in MW (as `np.float64`)
        ====================  ========================================

    Returns
    -------
    pd.DataFrame

        loss_functions

        ================  ============================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector, (as `str`)
        loss_function     a `function` object that takes interconnector \n
                          flow (as `float`) an input and returns \n
                          interconnector losses (as `float`).
        ================  ============================================
    """

    demand_loss_factor_offset = pd.merge(demand_coefficients, demand, 'inner', on=['region'])
    demand_loss_factor_offset['offset'] = demand_loss_factor_offset['loss_function_demand'] * \
        demand_loss_factor_offset['demand_coefficient']
    demand_loss_factor_offset = demand_loss_factor_offset.groupby('interconnector', as_index=False)['offset'].sum()
    loss_functions = pd.merge(interconnector_coefficients, demand_loss_factor_offset, 'left', on=['interconnector'])
    loss_functions['loss_constant'] = loss_functions['loss_constant'] + loss_functions['offset'].fillna(0)
    loss_functions['loss_function'] = \
        loss_functions.apply(lambda x: _create_function(x['loss_constant'], x['flow_coefficient']), axis=1)
    return loss_functions.loc[:, ['interconnector', 'loss_function', 'from_region_loss_share']]


def _create_function(constant, flow_coefficient):
    def loss_function(flow):
        return (constant - 1) * flow + (flow_coefficient / 2) * flow ** 2

    return loss_function
