class RawInputsLoader:
    """Provides single interface for accessing raw historical inputs.

    Examples
    --------

    >>> import sqlite3

    >>> from nempy.historical_inputs import mms_db
    >>> from nempy.historical_inputs import xml_cache

    For the RawInputsLoader to work we need to construct a database and inputs cache for it to load inputs from and then
    pass the interfaces to these to the inputs loader.

    >>> con = sqlite3.connect('market_management_system.db')
    >>> mms_db_manager = mms_db.DBManager(connection=con)
    >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')

    In this example the database and cache have already been populated so the input loader can be created straight
    away.

    >>> inputs_loader = RawInputsLoader(xml_cache_manager, mms_db_manager)

    Then we set the dispatch interval that we want to load inputs from.

    >>> inputs_loader.set_interval('2019/01/01 00:00:00')

    And then we can load some inputs.

    >>> inputs_loader.get_unit_volume_bids()
             DUID     BIDTYPE DIRECTION  MAXAVAIL  ENABLEMENTMIN  ENABLEMENTMAX  LOWBREAKPOINT  HIGHBREAKPOINT  BANDAVAIL1  BANDAVAIL2  BANDAVAIL3  BANDAVAIL4  BANDAVAIL5  BANDAVAIL6  BANDAVAIL7  BANDAVAIL8  BANDAVAIL9  BANDAVAIL10  RAMPDOWNRATE  RAMPUPRATE
    0      AGLHAL      ENERGY      None     173.0          173.0          173.0          173.0           173.0         0.0         0.0         0.0         0.0         0.0         0.0        60.0         0.0         0.0        160.0         720.0       720.0
    1      AGLSOM      ENERGY      None     160.0          160.0          160.0          160.0           160.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0        170.0         480.0       480.0
    2     ANGAST1      ENERGY      None      43.0           43.0           43.0           43.0            43.0         0.0         0.0         0.0         0.0         0.0        50.0         0.0         0.0         0.0         50.0         840.0       840.0
    3       APD01   LOWER5MIN      None       0.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0        300.0         300.0       300.0
    4       APD01  LOWER60SEC      None       0.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0        300.0         300.0       300.0
    ...       ...         ...       ...       ...            ...            ...            ...             ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...           ...         ...
    1021    YWPS4   LOWER6SEC      None      25.0          250.0          385.0          275.0           385.0        15.0        10.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0          0.0           0.0         0.0
    1022    YWPS4   RAISE5MIN      None       0.0          250.0          390.0          250.0           380.0         0.0         0.0         0.0         0.0         5.0         0.0         0.0         5.0         0.0         10.0          10.0        10.0
    1023    YWPS4    RAISEREG      None      15.0          250.0          385.0          250.0           370.0         0.0         0.0         0.0         0.0         0.0         0.0         5.0        10.0         0.0          5.0           5.0         5.0
    1024    YWPS4  RAISE60SEC      None      10.0          220.0          400.0          220.0           390.0         0.0         0.0         0.0         0.0         0.0         5.0         5.0         0.0         0.0         10.0          10.0        10.0
    1025    YWPS4   RAISE6SEC      None      15.0          220.0          405.0          220.0           390.0         0.0         0.0         0.0        10.0         5.0         0.0         0.0         0.0         0.0         10.0          10.0        10.0
    <BLANKLINE>
    [1026 rows x 20 columns]


    """
    def __init__(self, nemde_xml_cache_manager, market_management_system_database):
        self.xml = nemde_xml_cache_manager
        self.mms_db = market_management_system_database
        self.interval = None

    def set_interval(self, interval):
        """Set the interval to load inputs for.

        Examples
        --------

        For an example see the :func:`class level documentation <nempy.historical_inputs.loaders.RawInputsLoader>`


        Parameters
        ----------
        interval : str
            In the format '%Y/%m/%d %H:%M:%S'

        """
        self.interval = interval
        self.xml.load_interval(interval)

    def get_unit_initial_conditions(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_unit_initial_conditions <nempy.historical_inputs.xml_cache.XMLCacheManager.get_unit_initial_conditions>`
        """
        return self.xml.get_unit_initial_conditions()

    def get_unit_volume_bids(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_unit_volume_bids <nempy.historical_inputs.xml_cache.XMLCacheManager.get_unit_volume_bids>`
        """
        return self.xml.get_unit_volume_bids()

    def get_unit_price_bids(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.BIDDAYOFFER_D.get_data <nempy.historical_inputs.mms_db.DBManager.BIDDAYOFFER_D>`
        """
        # If you change this to source data from MMS then you need to also change units.UnitData.get_processed_bids
        # to not undo scaling by loss factors.
        return self.xml.get_unit_price_bids()

    def get_unit_details(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.DUDETAILSUMMARY.get_data <nempy.historical_inputs.mms_db.DBManager.DUDETAILSUMMARY>`
        """
        return self.mms_db.DUDETAILSUMMARY.get_data(self.interval)

    def get_agc_enablement_limits(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.DISPATCHLOAD.get_data <nempy.historical_inputs.mms_db.DBManager.DISPATCHLOAD>`
        """
        return self.mms_db.DISPATCHLOAD.get_data(self.interval)

    def get_UIGF_values(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_UIGF_values <nempy.historical_inputs.xml_cache.XMLCacheManager.get_UIGF_values>`
        """
        return self.xml.get_UIGF_values()

    def get_violations(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_violations <nempy.historical_inputs.xml_cache.XMLCacheManager.get_violations>`
        """
        return self.xml.get_violations()

    def get_constraint_violation_prices(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_violation_prices <nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_violation_prices>`
        """
        return self.xml.get_constraint_violation_prices()

    def get_constraint_rhs(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_rhs <nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_rhs>`
        """
        return self.xml.get_constraint_rhs()

    def get_constraint_type(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_type <nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_type>`
        """
        return self.xml.get_constraint_type()

    def get_constraint_region_lhs(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_region_lhs <nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_region_lhs>`
        """
        return self.xml.get_constraint_region_lhs()

    def get_constraint_unit_lhs(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_unit_lhs <nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_unit_lhs>`
        """
        return self.xml.get_constraint_unit_lhs()

    def get_constraint_interconnector_lhs(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_interconnector_lhs <nempy.historical_inputs.xml_cache.XMLCacheManager.get_constraint_interconnector_lhs>`
        """
        return self.xml.get_constraint_interconnector_lhs()

    def get_market_interconnectors(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.MNSP_INTERCONNECTOR.get_data <nempy.historical_inputs.mms_db.DBManager.MNSP_INTERCONNECTOR>`
        """
        return self.mms_db.MNSP_INTERCONNECTOR.get_data(self.interval)

    def get_market_interconnector_link_bid_availability(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_market_interconnector_link_bid_availability <nempy.historical_inputs.xml_cache.XMLCacheManager.get_market_interconnector_link_bid_availability>`
        """
        return self.xml.get_market_interconnector_link_bid_availability()

    def get_interconnector_constraint_parameters(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.INTERCONNECTORCONSTRAINT.get_data <nempy.historical_inputs.mms_db.DBManager.INTERCONNECTORCONSTRAINT>`
        """
        return self.mms_db.INTERCONNECTORCONSTRAINT.get_data(self.interval)

    def get_interconnector_definitions(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.INTERCONNECTOR.get_data <nempy.historical_inputs.mms_db.DBManager.INTERCONNECTOR>`
        """
        return self.mms_db.INTERCONNECTOR.get_data()

    def get_regional_loads(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.DISPATCHREGIONSUM.get_data <nempy.historical_inputs.mms_db.DBManager.DISPATCHREGIONSUM>`
        """
        return self.mms_db.DISPATCHREGIONSUM.get_data(self.interval)

    def get_interconnector_loss_segments(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.LOSSMODEL.get_data <nempy.historical_inputs.mms_db.DBManager.LOSSMODEL>`
        """
        return self.mms_db.LOSSMODEL.get_data(self.interval)

    def get_interconnector_loss_parameters(self):
        """Direct interface to :attr:`nempy.historical_inputs.mms_db.DBManager.LOSSFACTORMODEL.get_data <nempy.historical_inputs.mms_db.DBManager.LOSSFACTORMODEL>`
        """
        return self.mms_db.LOSSFACTORMODEL.get_data(self.interval)

    def get_unit_fast_start_parameters(self):
        """Direct interface to :func:`nempy.historical_inputs.xml_cache.XMLCacheManager.get_unit_fast_start_parameters <nempy.historical_inputs.xml_cache.XMLCacheManager.get_unit_fast_start_parameters>`
        """
        return self.xml.get_unit_fast_start_parameters()

    def is_over_constrained_dispatch_rerun(self):
        """Checks if the over constrained dispatch rerun process was used by AEMO to dispatch this interval.

        Examples
        --------

        >>> import sqlite3

        >>> from nempy.historical_inputs import mms_db
        >>> from nempy.historical_inputs import xml_cache

        For the RawInputsLoader to work we need to construct a database and inputs cache for it to load inputs from and then
        pass the interfaces to these to the inputs loader.

        >>> con = sqlite3.connect('market_management_system.db')
        >>> mms_db_manager = mms_db.DBManager(connection=con)
        >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')

        In this example the database and cache have already been populated so the input loader can be created straight
        away.

        >>> inputs_loader = RawInputsLoader(xml_cache_manager, mms_db_manager)

        Then we set the dispatch interval that we want to load inputs from.

        >>> inputs_loader.set_interval('2019/01/01 00:00:00')

        And then we can load some inputs.

        >>> inputs_loader.is_over_constrained_dispatch_rerun()
        False


        Returns
        -------
        bool

        """
        return 'OCD' in self.xml.get_file_name()
