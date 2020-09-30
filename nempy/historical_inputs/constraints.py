import pandas as pd


def _test_setup():
    import sqlite3
    from nempy.historical_inputs import mms_db
    from nempy.historical_inputs import xml_cache
    from nempy.historical_inputs import loaders
    con = sqlite3.connect('market_management_system.db')
    mms_db_manager = mms_db.DBManager(connection=con)
    xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
    inputs_loader.set_interval('2019/01/01 00:00:00')
    return inputs_loader


class ConstraintData:
    """Loads generic constraint related raw inputs and preprocess them for compatibility with :class:`nempy.markets.SpotMarket`

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
    >>> inputs_loader.set_interval('2019/01/01 00:00:00')

    Create a InterconnectorData instance.

    >>> constraint_data = ConstraintData(inputs_loader)

    >>> constraint_data.get_rhs_and_type_excluding_regional_fcas_constraints()
                         set           rhs type
    0               #BANN1_E     32.000000   <=
    1              #BNGSF2_E      3.000000   <=
    2            #CROWLWF1_E     43.000000   <=
    3             #CSPVPS1_E     29.000000   <=
    4             #DAYDSF1_E      0.000000   <=
    ..                   ...           ...  ...
    704          V_OWF_NRB_0  10000.001000   <=
    705  V_OWF_TGTSNRBHTN_30  10030.000000   <=
    706        V_S_NIL_ROCOF    812.280029   <=
    707          V_T_NIL_BL1    478.000000   <=
    708        V_T_NIL_FCSPS    425.154024   <=
    <BLANKLINE>
    [574 rows x 3 columns]

    Parameters
    ----------
    inputs_manager : historical_spot_market_inputs.DBManager
    """

    def __init__(self, raw_inputs_loader):
        self.raw_inputs_loader = raw_inputs_loader

        self.generic_rhs = self.raw_inputs_loader.get_constraint_rhs()
        self.generic_type = self.raw_inputs_loader.get_constraint_type()
        self.generic_rhs = pd.merge(self.generic_rhs, self.generic_type.loc[:, ['set', 'type']], on='set')
        type_map = {'LE': '<=', 'EQ': '=', 'GE': '>='}
        self.generic_rhs['type'] = self.generic_rhs['type'].apply(lambda x: type_map[x])

        bid_type_map = dict(ENOF='energy', LDOF='energy', L5RE='lower_reg', R5RE='raise_reg', R5MI='raise_5min',
                            L5MI='lower_5min', R60S='raise_60s', L60S='lower_60s', R6SE='raise_6s',
                            L6SE='lower_6s')

        self.unit_generic_lhs = self.raw_inputs_loader.get_constraint_unit_lhs()
        self.unit_generic_lhs['service'] = self.unit_generic_lhs['service'].apply(lambda x: bid_type_map[x])
        self.region_generic_lhs = self.raw_inputs_loader.get_constraint_region_lhs()
        self.region_generic_lhs['service'] = self.region_generic_lhs['service'].apply(lambda x: bid_type_map[x])

        self.interconnector_generic_lhs = self.raw_inputs_loader.get_constraint_interconnector_lhs()

        self.fcas_requirements = pd.merge(self.region_generic_lhs, self.generic_rhs, on='set')
        self.fcas_requirements = self.fcas_requirements.loc[:, ['set', 'service', 'region', 'type', 'rhs']]
        self.fcas_requirements.columns = ['set', 'service', 'region', 'type', 'volume']

    def get_rhs_and_type_excluding_regional_fcas_constraints(self):
        """Get the rhs values and types for generic constraints, excludes regional FCAS constraints.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_rhs_and_type_excluding_regional_fcas_constraints()
                             set           rhs type
        0               #BANN1_E     32.000000   <=
        1              #BNGSF2_E      3.000000   <=
        2            #CROWLWF1_E     43.000000   <=
        3             #CSPVPS1_E     29.000000   <=
        4             #DAYDSF1_E      0.000000   <=
        ..                   ...           ...  ...
        704          V_OWF_NRB_0  10000.001000   <=
        705  V_OWF_TGTSNRBHTN_30  10030.000000   <=
        706        V_S_NIL_ROCOF    812.280029   <=
        707          V_T_NIL_BL1    478.000000   <=
        708        V_T_NIL_FCSPS    425.154024   <=
        <BLANKLINE>
        [574 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set, \n
                           (as `str`)
            type           the direction of the constraint >=, <= or \n
                           =, (as `str`)
            rhs            the right hand side value of the constraint, \n
                           (as `np.float64`)
            =============  ===========================================
        """
        return self.generic_rhs[~self.generic_rhs['set'].isin(list(self.fcas_requirements['set']))]

    def get_rhs_and_type(self):
        """Get the rhs values and types for generic constraints.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_rhs_and_type()
                             set           rhs type
        0               #BANN1_E     32.000000   <=
        1              #BNGSF2_E      3.000000   <=
        2            #CROWLWF1_E     43.000000   <=
        3             #CSPVPS1_E     29.000000   <=
        4             #DAYDSF1_E      0.000000   <=
        ..                   ...           ...  ...
        704          V_OWF_NRB_0  10000.001000   <=
        705  V_OWF_TGTSNRBHTN_30  10030.000000   <=
        706        V_S_NIL_ROCOF    812.280029   <=
        707          V_T_NIL_BL1    478.000000   <=
        708        V_T_NIL_FCSPS    425.154024   <=
        <BLANKLINE>
        [709 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set, \n
                           (as `str`)
            type           the direction of the constraint >=, <= or \n
                           =, (as `str`)
            rhs            the right hand side value of the constraint, \n
                           (as `np.float64`)
            =============  ===========================================
        """
        return self.generic_rhs

    def get_unit_lhs(self):
        """Get the lhs coefficients of units.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_unit_lhs()
                              set      unit service  coefficient
        0                #BANN1_E     BANN1  energy          1.0
        1               #BNGSF2_E    BNGSF2  energy          1.0
        2             #CROWLWF1_E  CROWLWF1  energy          1.0
        3              #CSPVPS1_E   CSPVPS1  energy          1.0
        4              #DAYDSF1_E   DAYDSF1  energy          1.0
        ...                   ...       ...     ...          ...
        5864      V_ARWF_FSTTRP_5     ARWF1  energy          1.0
        5865      V_MTGBRAND_33WT  MTGELWF1  energy          1.0
        5866     V_OAKHILL_TFB_42  OAKLAND1  energy          1.0
        5867          V_OWF_NRB_0  OAKLAND1  energy          1.0
        5868  V_OWF_TGTSNRBHTN_30  OAKLAND1  energy          1.0
        <BLANKLINE>
        [5869 rows x 4 columns]

        Returns
        -------
        pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set \n
                           to map the lhs coefficients to, (as `str`)
            unit           the unit whose variables will be mapped to \n
                           the lhs, (as `str`)
            service        the service whose variables will be mapped
                           to the lhs, (as `str`)
            coefficient    the lhs coefficient (as `np.float64`)
            =============  ===========================================
        """
        return self.unit_generic_lhs

    def get_interconnector_lhs(self):
        """Get the lhs coefficients of interconnectors.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_interconnector_lhs()
                             set interconnector  coefficient
        0               DATASNAP      N-Q-MNSP1          1.0
        1        DATASNAP_DFS_LS      N-Q-MNSP1          1.0
        2      DATASNAP_DFS_NCAN      N-Q-MNSP1          1.0
        3    DATASNAP_DFS_NCWEST      N-Q-MNSP1          1.0
        4      DATASNAP_DFS_NNTH      N-Q-MNSP1          1.0
        ..                   ...            ...          ...
        631      V^^S_NIL_TBSE_1           V-SA          1.0
        632      V^^S_NIL_TBSE_2           V-SA          1.0
        633        V_S_NIL_ROCOF           V-SA          1.0
        634          V_T_NIL_BL1      T-V-MNSP1         -1.0
        635        V_T_NIL_FCSPS      T-V-MNSP1         -1.0
        <BLANKLINE>
        [636 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            =============   ==========================================
            Columns:        Description:
            set             the unique identifier of the constraint set \n
                            to map the lhs coefficients to, (as `str`)
            interconnetor   the interconnetor whose variables will be \n
                            mapped to the lhs, (as `str`)
            coefficient     the lhs coefficient (as `np.float64`)
            =============   ==========================================
        """
        return self.interconnector_generic_lhs

    def get_region_lhs(self):
        """Get the lhs coefficients of regions.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_region_lhs()
                           set region    service  coefficient
        0        F_I+LREG_0120   NSW1  lower_reg          1.0
        1        F_I+LREG_0120   QLD1  lower_reg          1.0
        2        F_I+LREG_0120    SA1  lower_reg          1.0
        3        F_I+LREG_0120   TAS1  lower_reg          1.0
        4        F_I+LREG_0120   VIC1  lower_reg          1.0
        ..                 ...    ...        ...          ...
        478   F_T+NIL_WF_TG_R5   TAS1  raise_reg          1.0
        479   F_T+NIL_WF_TG_R6   TAS1   raise_6s          1.0
        480  F_T+NIL_WF_TG_R60   TAS1  raise_60s          1.0
        481      F_T+RREG_0050   TAS1  raise_reg          1.0
        482    F_T_NIL_MINP_R6   TAS1   raise_6s          1.0
        <BLANKLINE>
        [483 rows x 4 columns]

        Returns
        -------
        pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set \n
                           to map the lhs coefficients to, (as `str`)
            region         the region whose variables will be mapped \n
                           to the lhs, (as `str`)
            service        the service whose variables will be mapped \n
                           to the lhs, (as `str`)
            coefficient    the lhs coefficient (as `np.float64`)
            =============  ===========================================
        """
        return self.region_generic_lhs

    def get_fcas_requirements(self):
        """Get constraint details needed for setting FCAS requirements.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_fcas_requirements()
                           set    service region type       volume
        0        F_I+LREG_0120  lower_reg   NSW1   >=   120.000000
        1        F_I+LREG_0120  lower_reg   QLD1   >=   120.000000
        2        F_I+LREG_0120  lower_reg    SA1   >=   120.000000
        3        F_I+LREG_0120  lower_reg   TAS1   >=   120.000000
        4        F_I+LREG_0120  lower_reg   VIC1   >=   120.000000
        ..                 ...        ...    ...  ...          ...
        478   F_T+NIL_WF_TG_R5  raise_reg   TAS1   >=    62.899972
        479   F_T+NIL_WF_TG_R6   raise_6s   TAS1   >=    67.073327
        480  F_T+NIL_WF_TG_R60  raise_60s   TAS1   >=    83.841637
        481      F_T+RREG_0050  raise_reg   TAS1   >= -9950.000000
        482    F_T_NIL_MINP_R6   raise_6s   TAS1   >=    35.000000
        <BLANKLINE>
        [483 rows x 5 columns]

        Returns
        -------
        pd.DataFrame

            ========   ===============================================
            Columns:   Description:
            set        unique identifier of the requirement set, \n
                       (as `str`)
            service    the service or services the requirement set \n
                       applies to (as `str`)
            region     the regions that can contribute to meeting a \n
                       requirement, (as `str`)
            volume     the amount of service required, in MW, \n
                       (as `np.float64`)
            type       the direction of the constrain '=', '>=' or \n
                       '<=', optional, a value of '=' is assumed if \n
                       the column is missing (as `str`)
            ========   ===============================================
        """
        return self.fcas_requirements

    def get_violation_costs(self):
        """Get the violation costs for generic constraints.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_violation_costs()
                             set       cost
        0               #BANN1_E  5220000.0
        1              #BNGSF2_E  5220000.0
        2            #CROWLWF1_E  5220000.0
        3             #CSPVPS1_E  5220000.0
        4             #DAYDSF1_E  5220000.0
        ..                   ...        ...
        704          V_OWF_NRB_0  5220000.0
        705  V_OWF_TGTSNRBHTN_30  5220000.0
        706        V_S_NIL_ROCOF   507500.0
        707          V_T_NIL_BL1  5220000.0
        708        V_T_NIL_FCSPS   435000.0
        <BLANKLINE>
        [709 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            =============  ===========================================
            Columns:       Description:
            set            the unique identifier of the constraint set \n
                           to map the lhs coefficients to, (as `str`)
            cost           the cost to the objective function of \n
                           violating the constraint, (as `np.float64`)
            =============  ===========================================
        """
        return self.generic_type.loc[:, ['set', 'cost']]

    def get_constraint_violation_prices(self):
        """Get the violation costs of non-generic constraint groups.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.get_constraint_violation_prices()
        {'regional_demand': 2175000.0, 'interocnnector': 16675000.0, 'generic_constraint': 435000.0, 'ramp_rate': 16747500.0, 'unit_capacity': 5365000.0, 'energy_offer': 16457500.0, 'fcas_profile': 2247500.0, 'fcas_max_avail': 2247500.0, 'fcas_enablement_min': 1015000.0, 'fcas_enablement_max': 1015000.0, 'fast_start': 16385000.0, 'mnsp_ramp_rate': 16747500.0, 'msnp_offer': 16457500.0, 'mnsp_capacity': 5292500.0, 'uigf': 5582500.0, 'voll': 14500.0}

        Returns
        -------
        dict
        """

        return self.raw_inputs_loader.get_constraint_violation_prices()

    def is_over_constrained_dispatch_rerun(self):
        """Get a boolean indicating if the over constrained dispatch rerun process was used for this interval.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = ConstraintData(inputs_loader)

        >>> unit_data.is_over_constrained_dispatch_rerun()
        False

        Returns
        -------
        bool
        """
        return self.raw_inputs_loader.is_over_constrained_dispatch_rerun()

