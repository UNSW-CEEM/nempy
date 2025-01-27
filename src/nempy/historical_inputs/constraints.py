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
    inputs_loader.set_interval('2024/07/10 12:05:00')
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
    >>> inputs_loader.set_interval('2024/07/10 12:05:00')

    Create a InterconnectorData instance.

    >>> constraint_data = ConstraintData(inputs_loader)

    >>> constraint_data.get_rhs_and_type_excluding_regional_fcas_constraints()
                       set           rhs type
    0          #BANGOWF2_E     82.800000   <=
    1          #BBATRYL1_E     50.000000   <=
    2          #BBATTERY_E     50.000000   <=
    3          #BBTHREE3_E     25.000000   <=
    4           #BOWWPV1_E      6.100000   <=
    ...                ...           ...  ...
    1107       V_T_NIL_BL1 -10125.000000   >=
    1108     V_T_NIL_FCSPS    493.111848   <=
    1109    V_WDR_NO_SCADA     95.000000   <=
    1110  V_WEMENSF_FLT_20     20.000000   <=
    1111   V_YATPSF_FLT_20     20.000000   <=
    <BLANKLINE>
    [975 rows x 3 columns]


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

        bid_type_map = dict(ENOF='energy', LDOF='energy', DROF='energy', L5RE='lower_reg', R5RE='raise_reg',
                            R5MI='raise_5min', L5MI='lower_5min', R60S='raise_60s', L60S='lower_60s', R6SE='raise_6s',
                            L6SE='lower_6s', R1SE='raise_1s', L1SE='lower_1s', BDOF='energy')

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
        0          #BANGOWF2_E     82.800000   <=
        1          #BBATRYL1_E     50.000000   <=
        2          #BBATTERY_E     50.000000   <=
        3          #BBTHREE3_E     25.000000   <=
        4           #BOWWPV1_E      6.100000   <=
        ...                ...           ...  ...
        1107       V_T_NIL_BL1 -10125.000000   >=
        1108     V_T_NIL_FCSPS    493.111848   <=
        1109    V_WDR_NO_SCADA     95.000000   <=
        1110  V_WEMENSF_FLT_20     20.000000   <=
        1111   V_YATPSF_FLT_20     20.000000   <=
        <BLANKLINE>
        [975 rows x 3 columns]

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
        0          #BANGOWF2_E     82.800000   <=
        1          #BBATRYL1_E     50.000000   <=
        2          #BBATTERY_E     50.000000   <=
        3          #BBTHREE3_E     25.000000   <=
        4           #BOWWPV1_E      6.100000   <=
        ...                ...           ...  ...
        1107       V_T_NIL_BL1 -10125.000000   >=
        1108     V_T_NIL_FCSPS    493.111848   <=
        1109    V_WDR_NO_SCADA     95.000000   <=
        1110  V_WEMENSF_FLT_20     20.000000   <=
        1111   V_YATPSF_FLT_20     20.000000   <=
        <BLANKLINE>
        [1112 rows x 3 columns]


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
        0           #BANGOWF2_E  BANGOWF2  energy          1.0
        1           #BBATRYL1_E  BBATRYL1  energy          1.0
        2           #BBATTERY_E  BBATTERY  energy          1.0
        3           #BBTHREE3_E  BBTHREE3  energy          1.0
        4            #BOWWPV1_E   BOWWPV1  energy          1.0
        ...                 ...       ...     ...          ...
        17032    V_WDR_NO_SCADA  DRXVDX01  energy          1.0
        17033    V_WDR_NO_SCADA  DRXVQP01  energy          1.0
        17034    V_WDR_NO_SCADA  DRXVQX01  energy          1.0
        17035  V_WEMENSF_FLT_20  WEMENSF1  energy          1.0
        17036   V_YATPSF_FLT_20    YATSF1  energy          1.0
        <BLANKLINE>
        [17037 rows x 4 columns]

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
        0        DATASNAP_DFS_LS      N-Q-MNSP1          1.0
        1      DATASNAP_DFS_NCAN      N-Q-MNSP1          1.0
        2    DATASNAP_DFS_NCWEST      N-Q-MNSP1          1.0
        3      DATASNAP_DFS_NNTH      N-Q-MNSP1          1.0
        4      DATASNAP_DFS_NSYD      N-Q-MNSP1          1.0
        ..                   ...            ...          ...
        827     V_S_HEYWOOD_UFLS           V-SA          1.0
        828        V_S_NIL_ROCOF           V-SA          1.0
        829         V_T_FCSPS_DS      T-V-MNSP1         -1.0
        830          V_T_NIL_BL1      T-V-MNSP1          1.0
        831        V_T_NIL_FCSPS      T-V-MNSP1         -1.0
        <BLANKLINE>
        [832 rows x 3 columns]

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
        0        D_I+BIP_ML2_L1   NSW1   lower_1s          1.0
        1        D_I+BIP_ML2_L1   QLD1   lower_1s          1.0
        2        D_I+BIP_ML2_L1    SA1   lower_1s          1.0
        3        D_I+BIP_ML2_L1   TAS1   lower_1s          1.0
        4        D_I+BIP_ML2_L1   VIC1   lower_1s          1.0
        ..                  ...    ...        ...          ...
        498  F_TASCAP_RREG_0220   NSW1  raise_reg          1.0
        499  F_TASCAP_RREG_0220   QLD1  raise_reg          1.0
        500  F_TASCAP_RREG_0220    SA1  raise_reg          1.0
        501  F_TASCAP_RREG_0220   VIC1  raise_reg          1.0
        502     F_T_NIL_MINP_R6   TAS1   raise_6s          1.0
        <BLANKLINE>
        [503 rows x 4 columns]

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
                            set    service region type        volume
        0        D_I+BIP_ML2_L1   lower_1s   NSW1   >= -10000.000000
        1        D_I+BIP_ML2_L1   lower_1s   QLD1   >= -10000.000000
        2        D_I+BIP_ML2_L1   lower_1s    SA1   >= -10000.000000
        3        D_I+BIP_ML2_L1   lower_1s   TAS1   >= -10000.000000
        4        D_I+BIP_ML2_L1   lower_1s   VIC1   >= -10000.000000
        ..                  ...        ...    ...  ...           ...
        498  F_TASCAP_RREG_0220  raise_reg   NSW1   >=    170.000000
        499  F_TASCAP_RREG_0220  raise_reg   QLD1   >=    170.000000
        500  F_TASCAP_RREG_0220  raise_reg    SA1   >=    170.000000
        501  F_TASCAP_RREG_0220  raise_reg   VIC1   >=    170.000000
        502     F_T_NIL_MINP_R6   raise_6s   TAS1   >=     34.040015
        <BLANKLINE>
        [503 rows x 5 columns]

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
        0          #BANGOWF2_E  6300000.0
        1          #BBATRYL1_E  6300000.0
        2          #BBATTERY_E  6300000.0
        3          #BBTHREE3_E  6300000.0
        4           #BOWWPV1_E  6300000.0
        ...                ...        ...
        1172       V_T_NIL_BL1  6300000.0
        1173     V_T_NIL_FCSPS   525000.0
        1174    V_WDR_NO_SCADA  6300000.0
        1175  V_WEMENSF_FLT_20   612500.0
        1176   V_YATPSF_FLT_20   612500.0
        <BLANKLINE>
        [1177 rows x 2 columns]

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
        {'regional_demand': 2625000.0, 'interocnnector': 20125000.0, 'generic_constraint': 525000.0, 'ramp_rate': 20212500.0, 'unit_capacity': 6475000.0, 'energy_offer': 19862500.0, 'fcas_profile': 2712500.0, 'fcas_max_avail': 2712500.0, 'fcas_enablement_min': 1225000.0, 'fcas_enablement_max': 1225000.0, 'fast_start': 19775000.0, 'mnsp_ramp_rate': 20212500.0, 'msnp_offer': 19862500.0, 'mnsp_capacity': 6387500.0, 'uigf': 6737500.0, 'voll': 17500.0, 'tiebreak': 1e-06}

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

