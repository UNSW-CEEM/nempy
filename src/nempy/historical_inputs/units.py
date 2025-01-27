import pandas as pd
import numpy as np
import doctest
from nempy.historical_inputs import aemo_to_nempy_name_mapping as an

pd.set_option('display.width', None)

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


class MethodCallOrderError(Exception):
    """Raise for calling methods in incompatible order."""


class UnitData:
    """Loads unit related raw inputs and preprocess them for compatibility with :class:`nempy.markets.SpotMarket`

    Examples
    --------

    This example shows the setup used for the examples in the class methods.

    >>> import sqlite3
    >>> from nempy.historical_inputs import mms_db
    >>> from nempy.historical_inputs import xml_cache
    >>> from nempy.historical_inputs import loaders

    The UnitData class requries a RawInputsLoader instance.

    >>> con = sqlite3.connect('market_management_system.db')
    >>> mms_db_manager = mms_db.DBManager(connection=con)
    >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    >>> inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
    >>> inputs_loader.set_interval('2024/07/10 12:05:00')

    Create the UnitData instance.

    >>> unit_data = UnitData(inputs_loader)

    >>> unit_data.get_unit_bid_availability()
             unit dispatch_type  capacity
    0     ADPBA1G     generator       6.0
    10    ADPBA1L          load       6.0
    12     ADPPV1     generator      19.0
    13     AGLHAL     generator     139.0
    14     AGLSOM     generator     128.0
    ...       ...           ...       ...
    1713    YWPS4     generator     340.0
    210      BHB1     generator       0.0
    211      BHB1          load       0.0
    1622   WANDB1     generator       0.0
    1623   WANDB1          load       0.0
    <BLANKLINE>
    [446 rows x 3 columns]
    """

    def __init__(self, raw_input_loader):
        self.raw_input_loader = raw_input_loader
        self.dispatch_interval = 5  # minutes
        self.dispatch_type_name_map = {'GENERATOR': 'generator', 'LOAD': 'load'}
        self.service_name_mapping = {'ENERGY': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s', 'RAISE1SEC': 'raise_1s',
                                     'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min', 'LOWER6SEC': 'lower_6s',
                                     'LOWER1SEC': 'lower_1s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min'}

        self.price_bids = self.raw_input_loader.get_unit_price_bids()
        volume_bids = self.raw_input_loader.get_unit_volume_bids()
        self.volume_bids = self._map_direction_to_volume_bids(volume_bids, self.price_bids)

        self.fast_start_profiles = self.raw_input_loader.get_unit_fast_start_parameters()
        self.initial_conditions = self.raw_input_loader.get_unit_initial_conditions()
        self.uigf_values = self.raw_input_loader.get_UIGF_values()

        self.unit_details = self.raw_input_loader.get_unit_details()
        self.BIDPEROFFER_D = None
        self.fcas_trapeziums = None
        self.updated_fast_start_profiles = None

    @staticmethod
    def _map_direction_to_volume_bids(volume_bids, price_bids):
        volume_bids_with_missing_directions = volume_bids[volume_bids['DIRECTION'].isna()].copy()
        volume_bids_without_missing_directions = volume_bids[~volume_bids['DIRECTION'].isna()].copy()
        volume_bids_with_missing_directions = volume_bids_with_missing_directions.drop(columns='DIRECTION')
        volume_bids_with_missing_directions = pd.merge(
            volume_bids_with_missing_directions,
            price_bids.loc[:, ["DUID", "BIDTYPE", "DIRECTION"]],
            how='left',
            on=['DUID', 'BIDTYPE']
        )
        volume_bids = pd.concat([volume_bids_with_missing_directions, volume_bids_without_missing_directions])
        return volume_bids

    def get_unit_bid_availability(self):
        """Get the bid in maximum availability for scheduled units.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_unit_bid_availability()
                 unit dispatch_type  capacity
        0     ADPBA1G     generator       6.0
        10    ADPBA1L          load       6.0
        12     ADPPV1     generator      19.0
        13     AGLHAL     generator     139.0
        14     AGLSOM     generator     128.0
        ...       ...           ...       ...
        1713    YWPS4     generator     340.0
        210      BHB1     generator       0.0
        211      BHB1          load       0.0
        1622   WANDB1     generator       0.0
        1623   WANDB1          load       0.0
        <BLANKLINE>
        [446 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            dispatch_type     "load" or "generator", (as `str`) \n
            capacity          unit bid in max availability, in MW, \n
                              (as `str`)
            ================  ========================================

        """
        bid_availability = self.volume_bids.loc[:, ['DUID', 'BIDTYPE', 'DIRECTION', 'MAXAVAIL']]
        bid_availability = self._remove_non_energy_bids(bid_availability)
        bid_availability = bid_availability.loc[:, ['DUID', 'DIRECTION', 'MAXAVAIL']]
        if self.raw_input_loader.interval < "2023/07/07 13:40":
            bid_availability = self._remove_non_scheduled_units(bid_availability)
        bid_availability = an.map_aemo_column_names_to_nempy_names(bid_availability)
        bid_availability = an.map_aemo_column_values_to_nempy_name(bid_availability, 'dispatch_type')
        return bid_availability

    @staticmethod
    def _remove_non_energy_bids(dataframe):
        return dataframe[dataframe['BIDTYPE'] == 'ENERGY']

    def _remove_non_scheduled_units(self, dataframe):
        non_scheduled_units = self.get_unit_uigf_limits()['unit']
        dataframe = dataframe[~dataframe['DUID'].isin(non_scheduled_units)]
        return dataframe

    def get_unit_uigf_limits(self):
        """Get the maximum availability predicted by the unconstrained intermittent generation forecast.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_unit_uigf_limits()
                 unit  capacity
        0      ADPPV1  10.90800
        1       ARWF1   0.00000
        2      AVLSF1  55.26000
        3    BALDHWF1  59.81800
        4    BANGOWF1  41.89800
        ..        ...       ...
        165  WSTWYSF1  49.90000
        166    WYASF1  33.90909
        167  YARANSF1  59.55000
        168    YATSF1  20.00000
        169   YENDWF1   7.00604
        <BLANKLINE>
        [170 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            capacity          the forecast max availability, in MW, \n
                              (as `str`)
            ================  ========================================

        """
        uigf = an.map_aemo_column_names_to_nempy_names(self.uigf_values)
        return uigf

    def get_bid_ramp_rates(self):
        """Get bid in ramp rates

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_bid_ramp_rates()
                 unit dispatch_type  ramp_down_rate  ramp_up_rate  initial_output
        0     ADPBA1G     generator           120.0         120.0         0.00000
        10    ADPBA1L          load           120.0         120.0         1.40400
        12     ADPPV1     generator           120.0         120.0        10.90800
        13     AGLHAL     generator           720.0         720.0         0.00000
        14     AGLSOM     generator           480.0         480.0        60.00000
        ...       ...           ...             ...           ...             ...
        1713    YWPS4     generator           180.0         180.0       337.93546
        1722     BHB1     generator           600.0         600.0         0.00000
        1723     BHB1          load           600.0         600.0         0.00000
        1724   WANDB1     generator          1200.0        1200.0         0.00000
        1725   WANDB1          load          1200.0        1200.0         0.00000
        <BLANKLINE>
        [446 rows x 5 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            dispatch_type    "load" or "generator" (as `str`) \n
            initial_output    the output/consumption of the unit at \n
                              the start of the dispatch interval, \n
                              in MW, (as `np.float64`)
            ramp_up_rate      the ramp up rate, in MW/h, \n
                              (as `np.float64`)
            ramp_down_rate    the ramp down rate, in MW/h, \n
                              (as `np.float64`)
            ================  ========================================
        """
        bid_ramp_rates = self.volume_bids.loc[:, ['DUID', 'BIDTYPE', 'DIRECTION', 'RAMPDOWNRATE', 'RAMPUPRATE']]
        initial_mw = self.initial_conditions.loc[:, ['DUID', "INITIALMW"]]
        bid_ramp_rates = pd.merge(bid_ramp_rates, initial_mw, on='DUID')
        bid_ramp_rates = self._remove_non_energy_bids(bid_ramp_rates)
        ramp_rates = an.map_aemo_column_names_to_nempy_names(bid_ramp_rates)
        ramp_rates = an.map_aemo_column_values_to_nempy_name(ramp_rates, 'dispatch_type')
        ramp_rates = ramp_rates.drop(columns='service')
        return ramp_rates

    def get_scada_ramp_rates(self, inlude_initial_output=False):
        """Get scada ramp rates

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_scada_ramp_rates(inlude_initial_output=True)
                 unit  scada_ramp_down_rate  scada_ramp_up_rate  initial_output
        0     ADPBA1G             93.119938           93.119938         0.00000
        1     ADPBA1L             93.119938           93.119938         1.40400
        2      ADPPV1            298.499937          298.499937        10.90800
        30     BALBG1          54000.000000        54000.000000         0.00000
        31     BALBL1          54000.000000        54000.000000         0.00000
        ..        ...                   ...                 ...             ...
        481  WOOLGSF1           2112.000046         2112.000046        91.80000
        493     YWPS1            180.000000          180.000000         0.00000
        494     YWPS2            176.624994          176.624994       358.89621
        495     YWPS3            181.124997          181.124997       371.52658
        496     YWPS4            180.000000          180.000000       337.93546
        <BLANKLINE>
        [192 rows x 4 columns]


        Args:
            inlude_initial_output: boolean specifying whether or not to
            inlcude the column initial_output in the returned dataframe, default False.

        Returns
        -------
        pd.DataFrame

            ==================    ========================================
            Columns:              Description:
            unit                  unique identifier for units, (as `str`) \n
            scada_ramp_up_rate    the ramp up rate, in MW/h, \n
                                  (as `np.float64`)
            scada_ramp_down_rate  the ramp down rate, in MW/h, \n
                                  (as `np.float64`)
            initial_output        the output/consumption of the unit at \n
                                  the start of the dispatch interval, \n
                                  in MW, (as `np.float64`)
            ====================  ========================================
        """
        cols = ['DUID', 'RAMPDOWNRATE', 'RAMPUPRATE']
        if inlude_initial_output:
            cols += ['INITIALMW']
        scada_telemetered_ramp_rates = \
            self.initial_conditions.loc[:, cols]
        scada_telemetered_ramp_rates = scada_telemetered_ramp_rates.rename(
            columns={'RAMPDOWNRATE': 'SCADARAMPDOWNRATE', 'RAMPUPRATE': 'SCADARAMPUPRATE'}
        )
        ramp_rates = an.map_aemo_column_names_to_nempy_names(scada_telemetered_ramp_rates)
        ramp_rates = ramp_rates[
            ~(ramp_rates["scada_ramp_up_rate"].isna() &
              ramp_rates["scada_ramp_down_rate"].isna())
        ].copy()
        return ramp_rates

    def get_initial_unit_output(self):
        """Get unit outputs at the start of the dispatch interval.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_initial_unit_output()
                unit  initial_output
        0    ADPBA1G         0.00000
        1    ADPBA1L         1.40400
        2     ADPPV1        10.90800
        3     AGLHAL         0.00000
        4     AGLSOM        60.00000
        ..       ...             ...
        492  YENDWF1         6.75000
        493    YWPS1         0.00000
        494    YWPS2       358.89621
        495    YWPS3       371.52658
        496    YWPS4       337.93546
        <BLANKLINE>
        [497 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            initial_output    the output/consumption of the unit at \n
                              the start of the dispatch interval, \n
                              in MW, (as `np.float64`)
            ================  ========================================
        """
        initial_unit_output = self.initial_conditions.loc[:, ['DUID', 'INITIALMW']]
        initial_unit_output = an.map_aemo_column_names_to_nempy_names(initial_unit_output)
        return initial_unit_output

    def get_fast_start_profiles_for_dispatch(self, unconstrained_dispatch=None,
                                             return_all_columns=False) -> pd.DataFrame:
        """Get the parameters needed to construct the fast dispatch inflexibility profiles used for dispatch.

        If the results of a non-fast start constrained dispatch run are provided then these are used to commit fast
        start units starting the interval in mode zero, when they have a non-zero dispatch result.

        For more info on fast start dispatch inflexibility profiles :download:`see AEMO docs <../../docs/pdfs/Fast_Start_Unit_Inflexibility_Profile_Model_October_2014.pdf>`.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_fast_start_profiles_for_dispatch()
                unit  current_mode
        0     AGLHAL             0
        1     AGLSOM             4
        2   BARRON-1             4
        3   BARRON-2             0
        4   BBTHREE1             0
        ..       ...           ...
        68     VPGS4             0
        69     VPGS5             0
        70     VPGS6             0
        71   W/HOE#1             0
        72   W/HOE#2             0
        <BLANKLINE>
        [73 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            If unconstrained_dispatch is not provided, i.e. geting profiles of first run:

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n

            dispatch_type    "load" or "generator" (as `str`) \n
            current_mode      the fast start mode the unit starts the interval in \n
                              (as `np.int64`)
            ================  ========================================

            If unconstrained_dispatch is provided, i.e. geting profiles of second run:

            ==========================  ========================================
            Columns:                    Description:
            unit                        unique identifier for units, (as `str`) \n
            end_mode                    the fast start mode the unit will end \n
                                        the dispatch interval in, (as `np.int64`)
            time_in_end_mode            the amount of time the unit will have \n
                                        spend in the end mode at the end of the \n
                                        dispatch interval, (as `np.float64`)
            mode_two_length             the length the units mode two, in minutes \n
                                        (as `np.float64`)
            mode_four_length            the length the units mode four, in minutes\n
                                        (as `np.float64`)
            min_loading                 the mininum opperating level of the unit \n
                                        during mode three, in MW, (as `no.float64`)
            time_since_end_of_mode_two  the time since the unit was last operating\n
                                        in mode two in minutes , (as `np.int64`)
            ==========================  ========================================

        """
        profiles = self._get_fast_start_profiles(unconstrained_dispatch=unconstrained_dispatch)
        self.updated_fast_start_profiles = profiles
        profiles['mode_two_length'] = np.float64(profiles['mode_two_length'])
        profiles['mode_four_length'] = np.float64(profiles['mode_four_length'])
        profiles['min_loading'] = np.float64(profiles['min_loading'])
        if unconstrained_dispatch is not None and not return_all_columns:
            profiles = profiles.loc[:, ['unit', 'end_mode', 'time_in_end_mode', 'mode_two_length',
                                        'mode_four_length', 'min_loading', 'time_since_end_of_mode_two']]
        elif not return_all_columns:
            profiles = profiles.loc[:, ['unit', 'current_mode']]
        return profiles

    def _get_fast_start_profiles(self, unconstrained_dispatch=None):
        fast_start_profiles = self.fast_start_profiles
        fast_start_profiles = an.map_aemo_column_names_to_nempy_names(fast_start_profiles)
        if unconstrained_dispatch is not None:
            fast_start_profiles = self._update_modes(fast_start_profiles, unconstrained_dispatch)
        return fast_start_profiles

    @staticmethod
    def _update_modes(fast_start_profiles, unconstrained_dispatch):
        unconstrained_dispatch = unconstrained_dispatch[unconstrained_dispatch['service'] == 'energy']
        fast_start_profiles = pd.merge(fast_start_profiles, unconstrained_dispatch, on='unit')
        fsp = fast_start_profiles

        fsp['time_left_in_interval'] = 5.0
        fsp['temp_time_in_current_mode'] = fsp['time_in_current_mode']
        fsp['temp_current_mode'] = fsp['current_mode']

        # Commit uncommited units with nonzero unconstrained dispatch
        fsp['temp_current_mode'] = np.where((fsp['current_mode'] == 0) &
                                   (fsp['dispatch'] > 0.0), 1,
                                   fsp['current_mode'])

        # Move units from mode one to mode two
        # If the time left in the mode is less than 5 minutes than progress to next mode
        mask = (fsp['temp_current_mode'] == 1) & (fsp['mode_one_length'] - fsp['temp_time_in_current_mode'] <
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 2
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_one_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        fsp = pd.concat([df1, df2])

        # Move units from mode two to mode three
        mask = (fsp['temp_current_mode'] == 2) & (fsp['mode_two_length'] - fsp['temp_time_in_current_mode'] <
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 3
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_two_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        df1['time_since_end_of_mode_two'] = df1['time_left_in_interval']
        fsp = pd.concat([df1, df2])

        # Move units from mode three to mode four
        mask = (fsp['temp_current_mode'] == 3) & (fsp['mode_three_length'] - fsp['temp_time_in_current_mode'] <
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 4
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_three_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        fsp = pd.concat([df1, df2])

        fsp['time_in_end_mode'] = fsp['temp_time_in_current_mode'] + fsp['time_left_in_interval']
        fsp['end_mode'] = fsp['temp_current_mode']

        fsp['time_in_end_mode'] = np.where(
            fsp['end_mode'] == 0, 0.0, fsp['time_in_end_mode']
        )

        fsp['time_in_end_mode'] = np.where(
            (fsp['end_mode'] == 4) & (fsp['time_in_end_mode'] > fsp['mode_four_length']),
            fsp['mode_four_length'],
            fsp['time_in_end_mode']
        )

        return fsp.loc[:, ['unit', 'min_loading', 'current_mode', 'end_mode', 'time_in_current_mode',
                           'time_in_end_mode', 'mode_one_length', 'mode_two_length', 'mode_three_length',
                           'mode_four_length', 'time_since_end_of_mode_two']]

    def get_unit_info(self):
        """Get unit information.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_unit_info()
                unit dispatch_type region  loss_factor
        0    ADPBA1G     generator    SA1     1.013527
        1    ADPBA1L          load    SA1     1.013527
        2     ADPPV1     generator    SA1     1.013527
        3     AGLHAL     generator    SA1     0.956500
        4     AGLSOM     generator   VIC1     0.979065
        ..       ...           ...    ...          ...
        494  YENDWF1     generator   VIC1     0.930059
        495    YWPS1     generator   VIC1     0.962100
        496    YWPS2     generator   VIC1     0.960400
        497    YWPS3     generator   VIC1     0.960400
        498    YWPS4     generator   VIC1     0.960400
        <BLANKLINE>
        [499 rows x 4 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`)
            region            the market region in which the unit is \n
                              located, (as `str`)
            dispatch_type     whether the unit is a 'generator' or \n
                              'load', (as `str`)
            loss_factor       the combined unit transmission and \n
                              distribution loss_factor, (as np.float64)
            ================  ========================================

        """
        unit_details = self.unit_details

        directions = self.price_bids.loc[:, ["DUID", "DIRECTION"]]
        directions = directions.drop_duplicates()

        unit_details = pd.merge(
            directions,
            unit_details,
            on="DUID"
        )

        trader_type = self.initial_conditions.loc[:, ["DUID", "TRADERTYPE"]]

        unit_details = pd.merge(
            trader_type,
            unit_details,
            on="DUID"
        )

        unit_details['SECONDARY_TLF'] = pd.to_numeric(unit_details['SECONDARY_TLF']).astype(np.float64)

        unit_details['LOSSFACTOR'] = np.where(
            ~unit_details['SECONDARY_TLF'].isna() & (unit_details['DIRECTION'] == "GENERATOR") & (unit_details['TRADERTYPE'] == "BIDIRECTIONAL"),
            unit_details['SECONDARY_TLF'] * unit_details['DISTRIBUTIONLOSSFACTOR'],
            unit_details['TRANSMISSIONLOSSFACTOR'] * unit_details['DISTRIBUTIONLOSSFACTOR']
        )
        unit_details = unit_details.loc[:, ['DUID', 'DIRECTION', 'CONNECTIONPOINTID', 'REGIONID', 'LOSSFACTOR']]
        unit_details = an.map_aemo_column_names_to_nempy_names(unit_details)
        unit_details = an.map_aemo_column_values_to_nempy_name(unit_details, column='dispatch_type')
        return unit_details.loc[:, ['unit', 'dispatch_type', 'region', 'loss_factor']]

    def _get_unit_availability(self):
        bid_availability = self.get_unit_bid_availability()
        ugif_availability = self.get_unit_uigf_limits()
        return pd.concat([bid_availability, ugif_availability])

    def get_processed_bids(self):
        """Get processed unit bids.

        The bids are processed by scaling for AGC enablement limits, scaling for scada ramp rates, scaling for
        the unconstrained intermittent generation forecast and enforcing the preconditions for enabling FCAS bids. For
        more info on these processes :download:`see AEMO docs  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> volume_bids, price_bids = unit_data.get_processed_bids()

        >>> volume_bids
                unit     service dispatch_type    1     2    3      4    5     6    7    8    9     10
        0    ADPBA1G      energy     generator  0.0   0.0  0.0    0.0  0.0   0.0  0.0  6.0  0.0    0.0
        10   ADPBA1L      energy          load  0.0   0.0  0.0    0.0  6.0   0.0  0.0  0.0  0.0    0.0
        12    ADPPV1      energy     generator  0.0   0.0  1.0    1.0  4.0  13.0  0.0  0.0  0.0    0.0
        13    AGLHAL      energy     generator  0.0   0.0  0.0    0.0  0.0   0.0  0.0  0.0  0.0  255.0
        14    AGLSOM      energy     generator  0.0  60.0  0.0  110.0  0.0   0.0  0.0  0.0  0.0    0.0
        ..       ...         ...           ...  ...   ...  ...    ...  ...   ...  ...  ...  ...    ...
        619  KIAMSF1   lower_60s     generator  0.0  37.0  0.0    0.0  0.0   0.0  0.0  0.0  0.0    0.0
        620  KIAMSF1    lower_6s     generator  0.0  37.0  0.0    0.0  0.0   0.0  0.0  0.0  0.0    0.0
        621   WDGPH1  lower_5min     generator  0.0   0.0  0.0    0.0  6.0   6.0  6.0  6.0  6.0   27.0
        622   WDGPH1   lower_60s     generator  0.0   0.0  0.0    0.0  6.0   6.0  6.0  6.0  6.0   27.0
        623   WDGPH1    lower_6s     generator  0.0   0.0  0.0    0.0  6.0   6.0  6.0  6.0  6.0   27.0
        <BLANKLINE>
        [1038 rows x 13 columns]

        >>> price_bids
                 unit    service dispatch_type           1           2           3           4           5           6            7            8             9            10
        0     ADPBA1G     energy     generator  -980.00001    0.000000   54.000745   96.001325  168.002318  273.997024   374.001783   998.000259   3999.004510   9999.999485
        1     ADPBA1L     energy          load  -980.00001 -449.996075 -174.002401  -88.997850   19.003641   54.000745   133.998471   223.999713    348.998059    500.003522
        2      ADPPV1     energy     generator -1013.52750 -506.763750 -110.474497 -100.339222  -57.771067  -47.635792     0.000000   304.058250   3040.582500  17736.731250
        3      AGLHAL     energy     generator  -956.50000    0.000000  274.410285  363.460435  566.142785  956.385220  3808.677785  9469.235220  15112.585220  16738.750000
        4      AGLSOM     energy     generator  -979.06536    0.000000  109.635739  206.690488  278.054562  364.466871   454.296118   980.044425  13022.430866  17133.643800
        ...       ...        ...           ...         ...         ...         ...         ...         ...         ...          ...          ...           ...           ...
        1033   WDGPH1   lower_6s     generator     0.01000    0.130000    0.330000    0.850000    1.830000    4.870000    19.920000    97.790000    998.990000  17500.000000
        1034  WKIEWA1  lower_60s     generator     0.00000    0.030000    1.000000    2.000000   26.000000   98.900000   147.000000   300.000000   1199.000000  17500.000000
        1035  WKIEWA1   lower_6s     generator     0.00000    0.500000    1.000000    2.000000   45.000000   98.690000   144.000000   300.000000   1199.000000  17500.000000
        1036  WKIEWA1  raise_60s     generator     0.00000    0.600000    1.700000    9.500000   22.100000   99.600000   132.100000   240.100000    495.000000  17500.000000
        1037  WKIEWA1   raise_6s     generator     0.00000    0.600000    1.700000    9.500000   32.100000   99.600000   132.100000   240.100000    495.000000  17500.000000
        <BLANKLINE>
        [1038 rows x 13 columns]


        Multiple Returns
        ----------------
        volume_bids : pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`)
            service           the service the bid applies to, (as `str`)
            dispatch_type     "load" or "generator", (as `str`)
            1                 the volume bid the first bid band, in MW, \n
                              (as `np.float64`)
            :
            10                the volume in the tenth bid band, in MW, \n
                              (as `np.float64`)
            ================  ========================================

        price_bids : pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`)
            service           the service the bid applies to, (as `str`)
            dispatch_type     "load" or "generator", (as `str`)
            1                 the price of the first bid band, in MW, \n
                              (as `np.float64`)
            :
            10                the price of the the tenth bid band, in MW, \n
                              (as `np.float64`)
            ================  ========================================

        """
        BIDPEROFFER_D = self.volume_bids.drop(['RAMPDOWNRATE', 'RAMPUPRATE'], axis=1)
        initial_conditions = self.initial_conditions

        BIDDAYOFFER_D = self.price_bids
        unit_info = self.get_unit_info()
        # TODO: check if this should really be based as bid availability as it is now.
        unit_availability = self._get_unit_availability()

        agc_enablement_limits = self.raw_input_loader.get_agc_enablement_limits()
        BIDPEROFFER_D = _scaling_for_agc_enablement_limits(BIDPEROFFER_D, agc_enablement_limits)
        BIDPEROFFER_D = _scaling_for_agc_ramp_rates(BIDPEROFFER_D, initial_conditions)
        BIDPEROFFER_D = _scaling_for_uigf(BIDPEROFFER_D, self.uigf_values)
        self.BIDPEROFFER_D, BIDDAYOFFER_D = _enforce_preconditions_for_enabling_fcas(
            BIDPEROFFER_D, BIDDAYOFFER_D, initial_conditions, unit_availability)

        volume_bids = _format_volume_bids(self.BIDPEROFFER_D, self.service_name_mapping)
        price_bids = _format_price_bids(BIDDAYOFFER_D, self.service_name_mapping)
        volume_bids = volume_bids[volume_bids['unit'].isin(list(unit_info['unit']))]
        volume_bids = volume_bids.loc[:, ['unit', 'service', 'dispatch_type', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', '10']]
        price_bids = price_bids[price_bids['unit'].isin(list(unit_info['unit']))]
        price_bids = price_bids.loc[:, ['unit', 'service', 'dispatch_type', '1', '2', '3', '4', '5',
                                        '6', '7', '8', '9', '10']]

        # Price bids  coming from xml have already been scaled by loss factors, so we need to undo this.
        price_bids = self._unscale_price_bids(price_bids, unit_info)

        return volume_bids, price_bids

    @staticmethod
    def _unscale_price_bids(price_bids, unit_info):
        price_bids = pd.merge(
            price_bids,
            unit_info.loc[:, ['unit', 'dispatch_type', 'loss_factor']],
            on=['unit', 'dispatch_type']
        )
        for col in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']:
            price_bids[col] = np.where(price_bids['service'] == 'energy', price_bids[col] * price_bids['loss_factor'],
                                       price_bids[col])
        return price_bids.drop(columns=['loss_factor'])

    def add_fcas_trapezium_constraints(self):
        """Load the fcas trapezium constraints into the UnitData class so subsequent method calls can access them.

        Examples
        --------
        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)

        If we try and call add_fcas_trapezium_constraints before calling get_processed_bids we get an error.

        >>> unit_data.add_fcas_trapezium_constraints()
        Traceback (most recent call last):
           ...
        nempy.historical_inputs.units.MethodCallOrderError: Call get_processed_bids before add_fcas_trapezium_constraints.

        After calling get_processed_bids it goes away.

        >>> volume_bids, price_bids =  unit_data.get_processed_bids()

        >>> unit_data.add_fcas_trapezium_constraints()

        If we try and access the trapezium constraints before calling this method we get an error.

        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)
        >>> unit_data.get_fcas_max_availability()
        Traceback (most recent call last):
           ...
        nempy.historical_inputs.units.MethodCallOrderError: Call add_fcas_trapezium_constraints before get_fcas_max_availability.

        After calling add_fcas_trapezium_constraints the error goes away.

        >>> volume_bids, price_bids = unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        >>> unit_data.get_fcas_max_availability()
                unit     service dispatch_type  max_availability
        0    ADPBA1G  raise_5min     generator               3.0
        1    ADPBA1G   raise_60s     generator               3.0
        2    ADPBA1G    raise_6s     generator               3.0
        3    ADPBA1L  lower_5min          load               3.0
        4    ADPBA1L   lower_60s          load               3.0
        ..       ...         ...           ...               ...
        619  KIAMSF1   lower_60s     generator              37.0
        620  KIAMSF1    lower_6s     generator              37.0
        621   WDGPH1  lower_5min     generator              57.0
        622   WDGPH1   lower_60s     generator              57.0
        623   WDGPH1    lower_6s     generator              57.0
        <BLANKLINE>
        [592 rows x 4 columns]

        Returns
        -------
        None


        Raises
        ------
        MethodCallOrderError
            if called before get_processed_bids

        """

        if self.BIDPEROFFER_D is None:
            raise MethodCallOrderError('Call get_processed_bids before add_fcas_trapezium_constraints.')
        self.fcas_trapeziums = _format_fcas_trapezium_constraints(self.BIDPEROFFER_D, self.service_name_mapping)

    def get_fcas_max_availability(self):
        """Get the unit bid maximum availability of each service.

        Examples
        --------

        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)

        Required calls before calling get_fcas_max_availability.

        >>> volume_bids, price_bids =  unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        Now facs max availibility can be accessed.

        >>> unit_data.get_fcas_max_availability()
                unit     service dispatch_type  max_availability
        0    ADPBA1G  raise_5min     generator               3.0
        1    ADPBA1G   raise_60s     generator               3.0
        2    ADPBA1G    raise_6s     generator               3.0
        3    ADPBA1L  lower_5min          load               3.0
        4    ADPBA1L   lower_60s          load               3.0
        ..       ...         ...           ...               ...
        619  KIAMSF1   lower_60s     generator              37.0
        620  KIAMSF1    lower_6s     generator              37.0
        621   WDGPH1  lower_5min     generator              57.0
        622   WDGPH1   lower_60s     generator              57.0
        623   WDGPH1    lower_6s     generator              57.0
        <BLANKLINE>
        [592 rows x 4 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`)
            dispatch_type     "load" or "generator", (as `str`)
            service           the service the bid applies to, (as `str`)
            max_availability  the unit bid maximum availability, in MW, \n
                              (as `np.float64`)
            ================  ========================================

        Raises
        ------
        MethodCallOrderError
            if the method is called before add_fcas_trapezium_constraints.
        """
        if self.fcas_trapeziums is None:
            raise MethodCallOrderError('Call add_fcas_trapezium_constraints before get_fcas_max_availability.')
        return self.fcas_trapeziums.loc[:, ['unit', 'service', 'dispatch_type', 'max_availability']]

    def get_fcas_regulation_trapeziums(self):
        """Get the unit bid FCAS trapeziums for regulation services.

        Examples
        --------

        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)

        Required calls before calling get_fcas_regulation_trapeziums.

        >>> volume_bids, price_bids =  unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        Now facs max availibility can be accessed.

        >>> unit_data.get_fcas_regulation_trapeziums()
                 unit    service dispatch_type  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
        474   ADPBA1G  lower_reg     generator          6.000000             0.0              6.0          6.000000             6.0
        475   ADPBA1L  lower_reg          load          6.000000             0.0              0.0          0.000000             6.0
        476    BALBG1  lower_reg     generator         30.000000             0.0             30.0         30.000000            30.0
        477    BALBL1  lower_reg          load         30.000000             0.0              0.0          0.000000            30.0
        478   BASTYAN  lower_reg     generator         63.000000            25.0             88.0         83.000000            83.0
        ..        ...        ...           ...               ...             ...              ...               ...             ...
        611       VP6  raise_reg     generator         14.870144           250.0            250.0        535.129856           550.0
        612  WALGRVG1  raise_reg     generator         39.000000             0.0              0.0          0.000000            39.0
        613  WALGRVL1  raise_reg          load         35.000000             0.0             35.0         35.000000            35.0
        614   WANDBG1  raise_reg     generator         70.000000             0.0              0.0         30.000000           100.0
        615   WANDBL1  raise_reg          load         30.000000             0.0             30.0         75.000000            75.0
        <BLANKLINE>
        [132 rows x 8 columns]

        Returns
        -------
        pd.DataFrame

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
            dispatch_type     "load" or "generator", (as `str`)
            service            the regulation service being offered, \n
                               (as `str`)
            max_availability   the maximum volume of the contingency \n
                               service, in MW, (as `np.float64`)
            enablement_min     the energy dispatch level at which \n
                               the unit can begin to provide \n
                               the regulation service, in MW, \n
                               (as `np.float64`)
            low_break_point    the energy dispatch level at which \n
                               the unit can provide the full \n
                               regulation service offered, in MW, \n
                               (as `np.float64`)
            high_break_point   the energy dispatch level at which the \n
                               unit can no longer provide the \n
                               full regulation service offered, in MW, \n
                               (as `np.float64`)
            enablement_max     the energy dispatch level at which the \n
                               unit can no longer provide any \n
                               regulation service, in MW, \n
                               (as `np.float64`)
            ================   =======================================

        Raises
        ------
        MethodCallOrderError
            if the method is called before add_fcas_trapezium_constraints.
        """
        if self.fcas_trapeziums is None:
            raise MethodCallOrderError('Call add_fcas_trapezium_constraints before get_fcas_max_availability.')
        return self.fcas_trapeziums[self.fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]

    def get_contingency_services(self):
        """Get the unit bid FCAS trapeziums for contingency services.

        Examples
        --------

        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)

        Required calls before calling get_contingency_services.

        >>> volume_bids, price_bids =  unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        Now facs max availibility can be accessed.

        >>> unit_data.get_contingency_services()
                unit     service dispatch_type  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
        0    ADPBA1G  raise_5min     generator               3.0             0.0              0.0           3.00000          6.0000
        1    ADPBA1G   raise_60s     generator               3.0             0.0              0.0           3.00000          6.0000
        2    ADPBA1G    raise_6s     generator               3.0             0.0              0.0           3.00000          6.0000
        3    ADPBA1L  lower_5min          load               3.0             0.0              0.0           3.00000          6.0000
        4    ADPBA1L   lower_60s          load               3.0             0.0              0.0           3.00000          6.0000
        ..       ...         ...           ...               ...             ...              ...               ...             ...
        619  KIAMSF1   lower_60s     generator              37.0             0.0            200.0           0.00000          0.0000
        620  KIAMSF1    lower_6s     generator              37.0             0.0            200.0           0.00000          0.0000
        621   WDGPH1  lower_5min     generator              57.0            59.0            116.0         276.82729        276.8273
        622   WDGPH1   lower_60s     generator              57.0            59.0            116.0         276.82729        276.8273
        623   WDGPH1    lower_6s     generator              57.0            59.0            116.0         276.82729        276.8273
        <BLANKLINE>
        [460 rows x 8 columns]

        Returns
        -------
        pd.DataFrame

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
            dispatch_type     "load" or "generator", (as `str`)
            service            the contingency service being offered, \n
                               (as `str`)
            max_availability   the maximum volume of the contingency \n
                               service, in MW, (as `np.float64`)
            enablement_min     the energy dispatch level at which \n
                               the unit can begin to provide \n
                               the regulation service, in MW, \n
                               (as `np.float64`)
            low_break_point    the energy dispatch level at which \n
                               the unit can provide the full \n
                               regulation service offered, in MW, \n
                               (as `np.float64`)
            high_break_point   the energy dispatch level at which the \n
                               unit can no longer provide the \n
                               full regulation service offered, in MW, \n
                               (as `np.float64`)
            enablement_max     the energy dispatch level at which the \n
                               unit can no longer provide any \n
                               regulation service, in MW, \n
                               (as `np.float64`)
            ================   =======================================

        Raises
        ------
        MethodCallOrderError
            if the method is called before add_fcas_trapezium_constraints.
        """

        if self.fcas_trapeziums is None:
            raise MethodCallOrderError('Call add_fcas_trapezium_constraints before get_contingency_services.')
        return self.fcas_trapeziums[~self.fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]


def _format_fcas_trapezium_constraints(BIDPEROFFER_D, service_name_mapping):
    """
    Examples
    --------

    >>> BIDPEROFFER_D = pd.DataFrame({
    ... 'DUID': ['A', 'B'],
    ... 'BIDTYPE': ['RAISE60SEC', 'ENERGY'],
    ... 'DIRECTION': ['GENERATOR', 'LOAD'],
    ... 'MAXAVAIL': [60.0, 0.0],
    ... 'ENABLEMENTMIN': [20.0, 0.0],
    ... 'LOWBREAKPOINT': [40.0, 0.0],
    ... 'HIGHBREAKPOINT': [60.0, 0.0],
    ... 'ENABLEMENTMAX': [80.0, 0.0]})

    >>> service_name_mapping = {'ENERGY': 'energy', 'RAISE60SEC': 'raise_60s'}

    >>> fcas_trapeziums = _format_fcas_trapezium_constraints(BIDPEROFFER_D, service_name_mapping)

    >>> print(fcas_trapeziums)
      unit    service dispatch_type  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
    0    A  raise_60s     generator              60.0            20.0             40.0              60.0            80.0

    """
    BIDPEROFFER_D = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] != 'ENERGY']
    trapezium_cons = BIDPEROFFER_D.loc[:, ['DUID', 'BIDTYPE', 'DIRECTION', 'MAXAVAIL', 'ENABLEMENTMIN', 'LOWBREAKPOINT',
                                           'HIGHBREAKPOINT', 'ENABLEMENTMAX']]
    trapezium_cons.columns = ['unit', 'service', 'dispatch_type', 'max_availability', 'enablement_min', 'low_break_point',
                              'high_break_point', 'enablement_max']
    trapezium_cons['service'] = trapezium_cons['service'].apply(lambda x: service_name_mapping[x])
    trapezium_cons = an.map_aemo_column_values_to_nempy_name(trapezium_cons, 'dispatch_type')
    return trapezium_cons


def _format_volume_bids(BIDPEROFFER_D, service_name_mapping):
    """Re-formats the AEMO MSS table BIDDAYOFFER_D to be compatible with the Spot market class.

    Examples
    --------

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG'],
    ...   'DIRECTION': ['GENERATOR', 'LOAD'],
    ...   'BANDAVAIL1': [100.0, 50.0],
    ...   'BANDAVAIL2': [10.0, 10.0],
    ...   'BANDAVAIL3': [0.0, 0.0],
    ...   'BANDAVAIL4': [10.0, 10.0],
    ...   'BANDAVAIL5': [10.0, 10.0],
    ...   'BANDAVAIL6': [10.0, 10.0],
    ...   'BANDAVAIL7': [10.0, 10.0],
    ...   'BANDAVAIL8': [0.0, 0.0],
    ...   'BANDAVAIL9': [0.0, 0.0],
    ...   'BANDAVAIL10': [0.0, 0.0]})

    >>> service_name_mapping = {'ENERGY': 'energy', 'RAISEREG': 'raise_reg'}

    >>> volume_bids = _format_volume_bids(BIDPEROFFER_D, service_name_mapping)

    >>> print(volume_bids)
      unit    service dispatch_type      1     2    3     4     5     6     7    8    9   10
    0    A     energy     generator  100.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0
    1    B  raise_reg          load   50.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0


    Parameters
    ----------
    BIDPEROFFER_D : pd.DataFrame

        ===========  ====================================================
        Columns:     Description:
        DUID         unique identifier of a unit (as `str`)
        BIDTYPE      the service being provided (as `str`)
        DIRECTION    "LOAD" or "GENERATOR", (as `str`)
        PRICEBAND1   bid volume in the 1st band, in MW (as `np.float64`)
        PRICEBAND2   bid volume in the 2nd band, in MW (as `np.float64`)
        PRICEBAND10  bid volume in the 10th band, in MW (as `np.float64`)
        MAXAVAIL     the offered cap on dispatch, in MW (as `np.float64`)
        ===========  ====================================================

    Returns
    ----------
    demand_coefficients : pd.DataFrame

        ================  =====================================================================================
        Columns:          Description:
        unit              unique identifier of a dispatch unit (as `str`)
        service           the service being provided, optional, if missing energy assumed (as `str`)
        dispatch_type     "load" or "generator", (as `str`)
        1                 bid volume in the 1st band, in MW (as `np.float64`)
        2                 bid volume in the 2nd band, in MW (as `np.float64`)
        :
        10                bid volume in the nth band, in MW (as `np.float64`)
        max_availability  the offered cap on dispatch, only used directly for fcas bids, in MW (as `np.float64`)
        ================  ======================================================================================
    """

    volume_bids = BIDPEROFFER_D.loc[:, ['DUID', 'BIDTYPE', 'DIRECTION', 'BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3',
                                        'BANDAVAIL4', 'BANDAVAIL5', 'BANDAVAIL6', 'BANDAVAIL7', 'BANDAVAIL8',
                                        'BANDAVAIL9', 'BANDAVAIL10']]
    volume_bids.columns = ['unit', 'service', 'dispatch_type', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    volume_bids['service'] = volume_bids['service'].apply(lambda x: service_name_mapping[x])
    volume_bids = an.map_aemo_column_values_to_nempy_name(volume_bids, 'dispatch_type')
    return volume_bids


def _format_price_bids(BIDDAYOFFER_D, service_name_mapping):
    """Re-formats the AEMO MSS table BIDDAYOFFER_D to be compatible with the Spot market class.

    Examples
    --------

    >>> BIDDAYOFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG'],
    ...   'DIRECTION': ['GENERATOR', 'LOAD'],
    ...   'PRICEBAND1': [100.0, 50.0],
    ...   'PRICEBAND2': [10.0, 10.0],
    ...   'PRICEBAND3': [0.0, 0.0],
    ...   'PRICEBAND4': [10.0, 10.0],
    ...   'PRICEBAND5': [10.0, 10.0],
    ...   'PRICEBAND6': [10.0, 10.0],
    ...   'PRICEBAND7': [10.0, 10.0],
    ...   'PRICEBAND8': [0.0, 0.0],
    ...   'PRICEBAND9': [0.0, 0.0],
    ...   'PRICEBAND10': [0.0, 0.0]})

    >>> service_name_mapping = {'ENERGY': 'energy', 'RAISEREG': 'raise_reg'}

    >>> price_bids = _format_price_bids(BIDDAYOFFER_D, service_name_mapping)

    >>> print(price_bids)
      unit    service dispatch_type      1     2    3     4     5     6     7    8    9   10
    0    A     energy     generator  100.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0
    1    B  raise_reg          load   50.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0

    Parameters
    ----------
    BIDDAYOFFER_D : pd.DataFrame

        ===========  ====================================================
        Columns:     Description:
        DUID         unique identifier of a unit (as `str`)
        BIDTYPE      the service being provided (as `str`)
        DIRECTION    "LOAD" or "GENERATOR", (as `str`)
        PRICEBAND1   bid price in the 1st band, in MW (as `np.float64`)
        PRICEBAND2   bid price in the 2nd band, in MW (as `np.float64`)
        PRICEBAND10  bid price in the 10th band, in MW (as `np.float64`)
        ===========  ====================================================

    Returns
    ----------
    demand_coefficients : pd.DataFrame

        =============  ================================================================
        Columns:       Description:
        unit           unique identifier of a dispatch unit (as `str`)
        service        the service being provided, optional, if missing energy assumed (as `str`)
        dispatch_type  "load" or "generator", (as `str`)
        1              bid price in the 1st band, in MW (as `np.float64`)
        2              bid price in the 2nd band, in MW (as `np.float64`)
        10             bid price in the nth band, in MW (as `np.float64`)
        =============  ================================================================
    """

    price_bids = \
        BIDDAYOFFER_D.loc[:, ['DUID', 'BIDTYPE', 'DIRECTION', 'PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3', 'PRICEBAND4',
                              'PRICEBAND5', 'PRICEBAND6', 'PRICEBAND7', 'PRICEBAND8', 'PRICEBAND9', 'PRICEBAND10']]
    price_bids.columns = ['unit', 'service', 'dispatch_type', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    price_bids['service'] = price_bids['service'].apply(lambda x: service_name_mapping[x])
    price_bids = an.map_aemo_column_values_to_nempy_name(price_bids, 'dispatch_type')
    return price_bids


def _scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD):
    """Scale regulating FCAS enablement and break points where AGC enablement limits are more restrictive than offers.

    The scaling is caried out as per the
    :download:`FCAS MODEL IN NEMDE documentation section 4.1  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

    Examples
    --------
    In this case AGC limits more restrictive then offered values so the trapezium slopes are scaled.

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['B', 'B', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG', 'LOWERREG'],
    ...   'ENABLEMENTMIN': [0.0, 20.0, 30.0],
    ...   'LOWBREAKPOINT': [0.0, 50.0, 50.0],
    ...   'HIGHBREAKPOINT': [0.0, 70.0, 70.0],
    ...   'ENABLEMENTMAX': [0.0, 100.0, 90.0]})

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['B'],
    ...   'RAISEREGENABLEMENTMAX': [90.0],
    ...   'RAISEREGENABLEMENTMIN': [30.0],
    ...   'LOWERREGENABLEMENTMAX': [80.0],
    ...   'LOWERREGENABLEMENTMIN': [40.0]})

    >>> BIDPEROFFER_D_out = _scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    B    ENERGY            0.0            0.0             0.0            0.0
    0    B  LOWERREG           40.0           60.0            60.0           80.0
    0    B  RAISEREG           30.0           60.0            60.0           90.0

    In this case we change the AGC limits to be less restrictive then offered values so the trapezium slopes are not
    scaled.

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['B'],
    ...   'RAISEREGENABLEMENTMAX': [110.0],
    ...   'RAISEREGENABLEMENTMIN': [10.0],
    ...   'LOWERREGENABLEMENTMAX': [100.0],
    ...   'LOWERREGENABLEMENTMIN': [20.0]})

    >>> BIDPEROFFER_D = _scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)

    >>> print(BIDPEROFFER_D)
      DUID   BIDTYPE  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    B    ENERGY            0.0            0.0             0.0            0.0
    0    B  LOWERREG           30.0           50.0            70.0           90.0
    0    B  RAISEREG           20.0           50.0            70.0          100.0

    Parameters
    ----------

    BIDPEROFFER_D : pd.DataFrame

        ==============  ====================================================
        Columns:        Description:
        DUID            unique identifier of a unit (as `str`)
        BIDTYPE         the service being provided (as `str`)
        ENABLEMENTMIN   the energy dispatch level at which the unit can begin to
                        provide the FCAS service, in MW (as `np.float64`)
        LOWBREAKPOINT   the energy dispatch level at which the unit can provide
                        the full FCAS offered, in MW (as `np.float64`)
        HIGHBREAKPOINT  the energy dispatch level at which the unit can no
                        longer provide the full FCAS service offered,
                        in MW (as `np.float64`)
        ENABLEMENTMAX   the energy dispatch level at which the unit can
                        no longer provide any FCAS service,
                        in MW (as `np.float64`)
        ==============  ====================================================

    DISPATCHLOAD : pd.DataFrame

        =====================  ======================================================================================
        Columns:               Description:
        DUID                   unique identifier of a dispatch unit (as `str`)
        RAISEREGENABLEMENTMAX  AGC telemetered ENABLEMENTMAX for raise regulation, in MW (as `np.float64`)
        RAISEREGENABLEMENTMIN  AGC telemetered ENABLEMENTMIN for raise regulation, in MW (as `np.float64`)
        LOWERREGENABLEMENTMAX  AGC telemetered ENABLEMENTMAX for lower regulation, in MW (as `np.float64`)
        LOWERREGENABLEMENTMIN  AGC telemetered ENABLEMENTMIN for lower regulation, in MW (as `np.float64`)
        =====================  ======================================================================================

    """
    # Split bid based on the scaling that needs to be done.
    lower_reg = BIDPEROFFER_D[(BIDPEROFFER_D['BIDTYPE'] == 'LOWERREG')]
    raise_reg = BIDPEROFFER_D[(BIDPEROFFER_D['BIDTYPE'] == 'RAISEREG')]
    bids_not_subject_to_scaling = BIDPEROFFER_D[~BIDPEROFFER_D['BIDTYPE'].isin(['RAISEREG', 'LOWERREG'])]

    # Merge in AGC enablement values from dispatch load so they can be compared to offer values.
    lower_reg = pd.merge(lower_reg, DISPATCHLOAD.loc[:, ['DUID', 'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN']],
                         'inner', on='DUID')
    raise_reg = pd.merge(raise_reg, DISPATCHLOAD.loc[:, ['DUID', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN']],
                         'inner', on='DUID')

    # Scale lower reg lower trapezium slope.
    lower_reg['LOWBREAKPOINT'] = np.where((lower_reg['LOWERREGENABLEMENTMIN'] > lower_reg['ENABLEMENTMIN']) &
                                          (lower_reg['LOWERREGENABLEMENTMIN'] > 0.0),
                                          lower_reg['LOWBREAKPOINT'] +
                                          (lower_reg['LOWERREGENABLEMENTMIN'] - lower_reg['ENABLEMENTMIN']),
                                          lower_reg['LOWBREAKPOINT'])
    lower_reg['ENABLEMENTMIN'] = np.where((lower_reg['LOWERREGENABLEMENTMIN'] > lower_reg['ENABLEMENTMIN']) &
                                          (lower_reg['LOWERREGENABLEMENTMIN'] > 0.0),
                                          lower_reg['LOWERREGENABLEMENTMIN'], lower_reg['ENABLEMENTMIN'])
    # Scale lower reg upper trapezium slope.
    lower_reg['HIGHBREAKPOINT'] = np.where((lower_reg['LOWERREGENABLEMENTMAX'] < lower_reg['ENABLEMENTMAX']) &
                                           (lower_reg['LOWERREGENABLEMENTMAX'] > 0.0),
                                           lower_reg['HIGHBREAKPOINT'] -
                                           (lower_reg['ENABLEMENTMAX'] - lower_reg['LOWERREGENABLEMENTMAX']),
                                           lower_reg['HIGHBREAKPOINT'])
    lower_reg['ENABLEMENTMAX'] = np.where((lower_reg['LOWERREGENABLEMENTMAX'] < lower_reg['ENABLEMENTMAX']) &
                                          (lower_reg['LOWERREGENABLEMENTMAX'] > 0.0),
                                          lower_reg['LOWERREGENABLEMENTMAX'], lower_reg['ENABLEMENTMAX'])

    # Scale raise reg lower trapezium slope.
    raise_reg['LOWBREAKPOINT'] = np.where((raise_reg['RAISEREGENABLEMENTMIN'] > raise_reg['ENABLEMENTMIN']) &
                                          (raise_reg['RAISEREGENABLEMENTMIN'] > 0.0),
                                          raise_reg['LOWBREAKPOINT'] +
                                          (raise_reg['RAISEREGENABLEMENTMIN'] - raise_reg['ENABLEMENTMIN']),
                                          raise_reg['LOWBREAKPOINT'])
    raise_reg['ENABLEMENTMIN'] = np.where((raise_reg['RAISEREGENABLEMENTMIN'] > raise_reg['ENABLEMENTMIN']) &
                                          (raise_reg['RAISEREGENABLEMENTMIN'] > 0.0),
                                          raise_reg['RAISEREGENABLEMENTMIN'], raise_reg['ENABLEMENTMIN'])
    # Scale raise reg upper trapezium slope.
    raise_reg['HIGHBREAKPOINT'] = np.where((raise_reg['RAISEREGENABLEMENTMAX'] < raise_reg['ENABLEMENTMAX']) &
                                           (raise_reg['RAISEREGENABLEMENTMAX'] > 0.0),
                                           raise_reg['HIGHBREAKPOINT'] -
                                           (raise_reg['ENABLEMENTMAX'] - raise_reg['RAISEREGENABLEMENTMAX']),
                                           raise_reg['HIGHBREAKPOINT'])
    raise_reg['ENABLEMENTMAX'] = np.where((raise_reg['RAISEREGENABLEMENTMAX'] < raise_reg['ENABLEMENTMAX']) &
                                          (raise_reg['RAISEREGENABLEMENTMAX'] > 0.0),
                                          raise_reg['RAISEREGENABLEMENTMAX'], raise_reg['ENABLEMENTMAX'])

    # Drop un need columns
    raise_reg = raise_reg.drop(['RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN'], axis=1)
    lower_reg = lower_reg.drop(['LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN'], axis=1)

    # Combined bids back together.
    BIDPEROFFER_D = pd.concat([bids_not_subject_to_scaling, lower_reg, raise_reg])

    return BIDPEROFFER_D


def _scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD):
    """Scale regulating FCAS max availability and break points where AGC ramp rates are less than offered availability.

    The scaling is caried out as per the
    :download:`FCAS MODEL IN NEMDE documentation section 4.2  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

    Examples
    --------
    In this case the ramp rates do not allow the full deilvery of the offered FCAS, because of this the offered MAXAIL
    is adjusted down and break points are adjusted to matain the slopes of the trapezium sides.

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['B', 'B', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG', 'LOWERREG'],
    ...   'MAXAVAIL': [0.0, 20.0, 20.0],
    ...   'ENABLEMENTMIN': [0.0, 20.0, 30.0],
    ...   'LOWBREAKPOINT': [0.0, 40.0, 50.0],
    ...   'HIGHBREAKPOINT': [0.0, 80.0, 70.0],
    ...   'ENABLEMENTMAX': [0.0, 100.0, 90.0]})

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['B'],
    ...   'RAMPUPRATE': [120.0],
    ...   'RAMPDOWNRATE': [120.0],
    ...   'LOWERREGACTUALAVAILABILITY': [10.0],
    ...   'RAISEREGACTUALAVAILABILITY': [10.0]})

    >>> BIDPEROFFER_D_out = _scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD)

    >>> print(BIDPEROFFER_D_out.loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL', 'LOWBREAKPOINT', 'HIGHBREAKPOINT']])
      DUID   BIDTYPE  MAXAVAIL  LOWBREAKPOINT  HIGHBREAKPOINT
    0    B    ENERGY       0.0            0.0             0.0
    0    B  LOWERREG      10.0           40.0            80.0
    0    B  RAISEREG      10.0           30.0            90.0

    In this case we change the AGC limits to be less restrictive then offered values so the trapezium slopes are not
    scaled.

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['B'],
    ...   'INITIALMW': [50.0],
    ...   'RAMPUPRATE': [360.0],
    ...   'RAMPDOWNRATE': [360.0],
    ...   'LOWERREGACTUALAVAILABILITY': [30.0],
    ...   'RAISEREGACTUALAVAILABILITY': [30.0]})

    >>> BIDPEROFFER_D_out = _scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD)

    >>> print(BIDPEROFFER_D_out.loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL', 'LOWBREAKPOINT', 'HIGHBREAKPOINT']])
      DUID   BIDTYPE  MAXAVAIL  LOWBREAKPOINT  HIGHBREAKPOINT
    0    B    ENERGY       0.0            0.0             0.0
    0    B  LOWERREG      20.0           50.0            70.0
    0    B  RAISEREG      20.0           40.0            80.0

    Parameters
    ----------

    BIDPEROFFER_D : pd.DataFrame

        ==============  ====================================================
        Columns:        Description:
        DUID            unique identifier of a unit (as `str`)
        BIDTYPE         the service being provided (as `str`)
        MAXAVAIL        the offered maximum capacity, in MW (as `np.float64`)
        ENABLEMENTMIN   the energy dispatch level at which the unit can begin to
                        provide the FCAS service, in MW (as `np.float64`)
        LOWBREAKPOINT   the energy dispatch level at which the unit can provide
                        the full FCAS offered, in MW (as `np.float64`)
        HIGHBREAKPOINT  the energy dispatch level at which the unit can no
                        longer provide the full FCAS service offered,
                        in MW (as `np.float64`)
        ENABLEMENTMAX   the energy dispatch level at which the unit can
                        no longer provide any FCAS service,
                        in MW (as `np.float64`)
        ==============  ====================================================

    DISPATCHLOAD : pd.DataFrame

        ===============  ======================================================================================
        Columns:         Description:
        DUID             unique identifier of a dispatch unit (as `str`)
        RAMPDOWNRATE     the maximum rate at which the unit can decrease output, in MW/h (as `np.float64`)
        RAMPUPRATE       the maximum rate at which the unit can increase output, in MW/h (as `np.float64`)
        ===============  ======================================================================================

    """
    units_with_scada_ramp_up_rates = list(
        DISPATCHLOAD[(~DISPATCHLOAD['RAMPUPRATE'].isna()) & DISPATCHLOAD['RAMPUPRATE'] != 0]['DUID'])
    units_with_no_scada_ramp_up_rates = list(
        DISPATCHLOAD[~DISPATCHLOAD['DUID'].isin(units_with_scada_ramp_up_rates)]['DUID'])
    units_with_scada_ramp_down_rates = list(
        DISPATCHLOAD[(~DISPATCHLOAD['RAMPDOWNRATE'].isna()) & DISPATCHLOAD['RAMPDOWNRATE'] != 0]['DUID'])
    units_with_no_scada_ramp_down_rates = list(
        DISPATCHLOAD[~DISPATCHLOAD['DUID'].isin(units_with_scada_ramp_down_rates)]['DUID'])
    DISPATCHLOAD = DISPATCHLOAD[DISPATCHLOAD['DUID'].isin(units_with_scada_ramp_up_rates +
                                                          units_with_scada_ramp_down_rates)]

    # Split bid based on the scaling that needs to be done.
    lower_reg = BIDPEROFFER_D[(BIDPEROFFER_D['BIDTYPE'] == 'LOWERREG') &
                              BIDPEROFFER_D['DUID'].isin(units_with_scada_ramp_down_rates)]
    raise_reg = BIDPEROFFER_D[(BIDPEROFFER_D['BIDTYPE'] == 'RAISEREG') &
                              BIDPEROFFER_D['DUID'].isin(units_with_scada_ramp_up_rates)]
    bids_not_subject_to_scaling_1 = BIDPEROFFER_D[~BIDPEROFFER_D['BIDTYPE'].isin(['RAISEREG', 'LOWERREG'])]
    bids_not_subject_to_scaling_2 = BIDPEROFFER_D[(BIDPEROFFER_D['BIDTYPE'] == 'RAISEREG') &
                                                  (BIDPEROFFER_D['DUID'].isin(units_with_no_scada_ramp_up_rates))]
    bids_not_subject_to_scaling_3 = BIDPEROFFER_D[(BIDPEROFFER_D['BIDTYPE'] == 'LOWERREG') &
                                                  (BIDPEROFFER_D['DUID'].isin(units_with_no_scada_ramp_down_rates))]
    bids_not_subject_to_scaling = pd.concat([bids_not_subject_to_scaling_1,
                                             bids_not_subject_to_scaling_2,
                                             bids_not_subject_to_scaling_3])

    # Merge in AGC enablement values from dispatch load so they can be compared to offer values.
    lower_reg = pd.merge(lower_reg, DISPATCHLOAD.loc[:, ['DUID', 'RAMPDOWNRATE']], 'inner', on='DUID')
    raise_reg = pd.merge(raise_reg, DISPATCHLOAD.loc[:, ['DUID', 'RAMPUPRATE']], 'inner', on='DUID')

    # Calculate the max FCAS possible based on ramp rates.
    lower_reg['RAMPMAX'] = lower_reg['RAMPDOWNRATE'] * (5 / 60)
    raise_reg['RAMPMAX'] = raise_reg['RAMPUPRATE'] * (5 / 60)

    # Check these ramp maxs are consistent with other AEMO outputs, otherwise increase untill consistency is achieved.
    # lower_reg['RAMPMAX'] = np.where(lower_reg['RAMPMAX'] < lower_reg['LOWERREGACTUALAVAILABILITY'],
    #                                 lower_reg['LOWERREGACTUALAVAILABILITY'], lower_reg['RAMPMAX'])
    # raise_reg['RAMPMAX'] = np.where(raise_reg['RAMPMAX'] < raise_reg['RAISEREGACTUALAVAILABILITY'],
    #                                 raise_reg['RAISEREGACTUALAVAILABILITY'], raise_reg['RAMPMAX'])

    lower_reg = lower_reg.drop(['RAMPDOWNRATE'], axis=1)
    raise_reg = raise_reg.drop(['RAMPUPRATE'], axis=1)

    reg = pd.concat([lower_reg, raise_reg])

    def get_new_low_break_point(old_max, ramp_max, low_break_point, enablement_min):
        if old_max > ramp_max and (low_break_point - enablement_min) != 0.0:
            # Get slope of trapezium
            m = old_max / (low_break_point - enablement_min)
            # Substitute new_max into the slope equation and re-arrange to find the new break point needed to keep the
            # slope the same.
            low_break_point = ramp_max / m + enablement_min
        return low_break_point

    def get_new_high_break_point(old_max, ramp_max, high_break_point, enablement_max):
        if old_max > ramp_max and (enablement_max - high_break_point) != 0.0:
            # Get slope of trapezium
            m = old_max / (enablement_max - high_break_point)
            # Substitute new_max into the slope equation and re-arrange to find the new break point needed to keep the
            # slope the same.
            high_break_point = enablement_max - (ramp_max / m)
        return high_break_point

    # Scale break points to maintain slopes.
    reg['LOWBREAKPOINT'] = reg.apply(lambda x: get_new_low_break_point(x['MAXAVAIL'], x['RAMPMAX'], x['LOWBREAKPOINT'],
                                                                       x['ENABLEMENTMIN']),
                                     axis=1)
    reg['HIGHBREAKPOINT'] = reg.apply(lambda x: get_new_high_break_point(x['MAXAVAIL'], x['RAMPMAX'],
                                                                         x['HIGHBREAKPOINT'], x['ENABLEMENTMAX']),
                                      axis=1)

    # Adjust max FCAS availability.
    reg['MAXAVAIL'] = np.where(reg['MAXAVAIL'] > reg['RAMPMAX'], reg['RAMPMAX'], reg['MAXAVAIL'])

    reg.drop(['RAMPMAX'], axis=1)

    # Combined bids back together.
    BIDPEROFFER_D = pd.concat([bids_not_subject_to_scaling, reg])

    return BIDPEROFFER_D


def _scaling_for_uigf(BIDPEROFFER_D, ugif_values):
    """Scale semi-schedualed units FCAS enablement max and break points where their UIGF is less than enablement max.

    The scaling is caried out as per the
    :download:`FCAS MODEL IN NEMDE documentation section 4.3  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`.

    Examples
    --------
    In this case the semi-scheduled unit has an availability less than its enablement max so it upper slope is scalled.

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG', 'LOWER60SEC'],
    ...   'HIGHBREAKPOINT': [0.0, 80.0, 70.0],
    ...   'ENABLEMENTMAX': [0.0, 100.0, 90.0]})

    >>> ugif_values = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'UIGF': [120.0, 90.0, 80.0]})

    >>> BIDPEROFFER_D_out = _scaling_for_uigf(BIDPEROFFER_D, ugif_values)

    >>> print(BIDPEROFFER_D_out.loc[:, ['DUID', 'BIDTYPE', 'HIGHBREAKPOINT', 'ENABLEMENTMAX']])
      DUID     BIDTYPE  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A      ENERGY             0.0            0.0
    0    B    RAISEREG            70.0           90.0
    1    C  LOWER60SEC            60.0           80.0

    In this case we change the availability of unit C so it does not need scaling.

    >>> ugif_values = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'UIGF': [120.0, 90.0, 91.0]})

    >>> BIDPEROFFER_D_out = _scaling_for_uigf(BIDPEROFFER_D, ugif_values)

    >>> print(BIDPEROFFER_D_out.loc[:, ['DUID', 'BIDTYPE', 'HIGHBREAKPOINT', 'ENABLEMENTMAX']])
      DUID     BIDTYPE  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A      ENERGY             0.0            0.0
    0    B    RAISEREG            70.0           90.0
    1    C  LOWER60SEC            70.0           90.0

    """
    # Split bid based on the scaling that needs to be done.
    semi_scheduled_units = ugif_values['DUID'].unique()
    energy_bids = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'ENERGY']
    fcas_bids = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] != 'ENERGY']
    fcas_semi_scheduled = fcas_bids[fcas_bids['DUID'].isin(semi_scheduled_units)]
    fcas_not_semi_scheduled = fcas_bids[~fcas_bids['DUID'].isin(semi_scheduled_units)]

    fcas_semi_scheduled = pd.merge(fcas_semi_scheduled, ugif_values.loc[:, ['DUID', 'UIGF']],
                                   'inner', on='DUID')

    def get_new_high_break_point(availability, high_break_point, enablement_max):
        if enablement_max > availability:
            high_break_point = high_break_point - (enablement_max - availability)
        return high_break_point

    if not fcas_semi_scheduled.empty:
        # Scale high break points.
        fcas_semi_scheduled['HIGHBREAKPOINT'] = \
            fcas_semi_scheduled.apply(lambda x: get_new_high_break_point(x['UIGF'], x['HIGHBREAKPOINT'],
                                                                         x['ENABLEMENTMAX']),
                                      axis=1)

        # Adjust ENABLEMENTMAX.
        fcas_semi_scheduled['ENABLEMENTMAX'] = \
            np.where(fcas_semi_scheduled['ENABLEMENTMAX'] > fcas_semi_scheduled['UIGF'],
                     fcas_semi_scheduled['UIGF'], fcas_semi_scheduled['ENABLEMENTMAX'])

        fcas_semi_scheduled.drop(['UIGF'], axis=1)

        # Combined bids back together.
        BIDPEROFFER_D = pd.concat([energy_bids, fcas_not_semi_scheduled, fcas_semi_scheduled])

    else:
        # Combined bids back together.
        BIDPEROFFER_D = pd.concat([energy_bids, fcas_not_semi_scheduled])

    return BIDPEROFFER_D


def _enforce_preconditions_for_enabling_fcas(BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits):
    """Checks that fcas bids meet criteria for being considered, returns a filtered version of volume and price bids.

    The criteria are based on the
    :download:`FCAS MODEL IN NEMDE documentation section 5  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`. Note the
    remaining energy condition is not applied because it relates to pre-dispatch. Also note that for the Energy
    Max Availability term we use the interval specific capacity determined in the determine_unit_limits function.

    The criteria used are
     - FCAS MAX AVAILABILITY > 0.0
     - At least one price band must have a no zero volume bid
     - The maximum energy availability >= FCAS enablement min
     - FCAS enablement max > 0.0
     - FCAS enablement min <= initial ouput <= FCAS enablement max
     - AGCSTATUS == 0.0

    Examples
    --------
    Inputs for three unit who meet all criteria for being enabled.

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'DIRECTION': ['GENERATOR', 'GENERATOR', 'GENERATOR'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG', 'RAISEREG'],
    ...   'BANDAVAIL1': [100.0, 50.0, 50.0],
    ...   'BANDAVAIL2': [10.0, 10.0, 0.0],
    ...   'MAXAVAIL': [0.0, 100.0, 100.0],
    ...   'ENABLEMENTMIN': [0.0, 20.0, 20.0],
    ...   'LOWBREAKPOINT': [0.0, 50.0, 50.0],
    ...   'HIGHBREAKPOINT': [0.0, 70.0, 70.0],
    ...   'ENABLEMENTMAX': [0.0, 100.0, 100.0],})

    >>> BIDDAYOFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'DIRECTION': ['GENERATOR', 'GENERATOR', 'GENERATOR'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG', 'RAISEREG'],
    ...   'PRICEBAND1': [100.0, 50.0, 60.0],
    ...   'PRICEBAND2': [110.0, 60.0, 80.0]})

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'INITIALMW': [50.0, 60.0, 60.0],
    ...   'AGCSTATUS': [0.0, 1.0, 1.0],
    ...   'TRADERTYPE': ['GENERATOR', 'GENERATOR', 'GENERATOR']})

    >>> capacity_limits = pd.DataFrame({
    ...   'unit': ['A', 'B', 'C'],
    ...   'dispatch_type': ['generator', 'generator', 'generator'],
    ...   'capacity': [50.0, 120.0, 80.0]})

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0
    1    C  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0         0.0

    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0
    1    C  GENERATOR  RAISEREG        60.0        80.0

    If unit C's FCAS MAX AVAILABILITY is changed to zero then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> BIDPEROFFER_D_mod['MAXAVAIL'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0, BIDPEROFFER_D_mod['MAXAVAIL'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0


    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0

    If unit C's BANDAVAIL1 is changed to zero then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> BIDPEROFFER_D_mod['BANDAVAIL1'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0,
    ...                                            BIDPEROFFER_D_mod['BANDAVAIL1'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0

    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0

    If unit C's capacity is changed to less than its enablement min then it gets filtered out.

    >>> capacity_limits_mod = capacity_limits.copy()

    >>> capacity_limits_mod['capacity'] = np.where(capacity_limits_mod['unit'] == 'C', 0.0,
    ...                                            capacity_limits_mod['capacity'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits_mod)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0


    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0

    If unit C's ENABLEMENTMIN ENABLEMENTMAX and INITIALMW are changed to zero and its then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> BIDPEROFFER_D_mod['ENABLEMENTMAX'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0,
    ...   BIDPEROFFER_D_mod['ENABLEMENTMAX'])

    >>> BIDPEROFFER_D_mod['ENABLEMENTMIN'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0,
    ...   BIDPEROFFER_D_mod['ENABLEMENTMIN'])

    >>> DISPATCHLOAD_mod['INITIALMW'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 0.0, DISPATCHLOAD_mod['INITIALMW'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0
    1    C  GENERATOR  RAISEREG     100.0            0.0           50.0            70.0            0.0        50.0         0.0

    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0
    1    C  GENERATOR  RAISEREG        60.0        80.0

    If unit C's INITIALMW is changed to less than its enablement min then it gets filtered out.

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> DISPATCHLOAD_mod['INITIALMW'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 19.0, DISPATCHLOAD_mod['INITIALMW'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0

    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0

    If unit C's AGCSTATUS is changed to  0.0 then it gets filtered out.

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> DISPATCHLOAD_mod['AGCSTATUS'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 0.0, DISPATCHLOAD_mod['AGCSTATUS'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID  DIRECTION   BIDTYPE  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX  BANDAVAIL1  BANDAVAIL2
    0    A  GENERATOR    ENERGY       0.0            0.0            0.0             0.0            0.0       100.0        10.0
    0    B  GENERATOR  RAISEREG     100.0           20.0           50.0            70.0          100.0        50.0        10.0

    >>> print(BIDDAYOFFER_D_out)
      DUID  DIRECTION   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A  GENERATOR    ENERGY       100.0       110.0
    0    B  GENERATOR  RAISEREG        50.0        60.0

    Parameters
    ----------
    BIDPEROFFER_D : pd.DataFrame

        ==============  ====================================================
        Columns:        Description:
        DUID            unique identifier of a unit (as `str`)
        BIDTYPE         the service being provided (as `str`)
        PRICEBAND1      bid volume in the 1st band, in MW (as `np.float64`)
        PRICEBAND2      bid volume in the 2nd band, in MW (as `np.float64`)
        PRICEBAND10     bid volume in the 10th band, in MW (as `np.float64`)
        MAXAVAIL        the offered maximum capacity, in MW (as `np.float64`)
        ENABLEMENTMIN   the energy dispatch level at which the unit can begin to
                        provide the contingency service, in MW (as `np.float64`)
        LOWBREAKPOINT   the energy dispatch level at which the unit can provide
                        the full contingency service offered, in MW (as `np.float64`)
        HIGHBREAKPOINT  the energy dispatch level at which the unit can no
                        longer provide the full contingency service offered,
                        in MW (as `np.float64`)
        ENABLEMENTMAX   the energy dispatch level at which the unit can
                        no longer provide any contingency service,
                        in MW (as `np.float64`)
        ==============  ====================================================

    BIDDAYOFFER_D : pd.DataFrame

        ===========  ====================================================
        Columns:     Description:
        DUID         unique identifier of a unit (as `str`)
        BIDTYPE      the service being provided (as `str`)
        PRICEBAND1   bid price in the 1st band, in MW (as `np.float64`)
        PRICEBAND2   bid price in the 2nd band, in MW (as `np.float64`)
        PRICEBAND10  bid price in the 10th band, in MW (as `np.float64`)
        ===========  ====================================================

    DISPATCHLOAD : pd.DataFrame

        ===============  ======================================================================================
        Columns:         Description:
        DUID             unique identifier of a dispatch unit (as `str`)
        INITIALMW        the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        AGCSTATUS        flag for if the units automatic generation control is enabled 0.0 for no, 1.0 for yes,
                         (as `np.float64`)
        ===============  ======================================================================================

    capacity_limits : pd.DataFrame

        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        capacity        the maximum output of the unit if unconstrained by ramp rate, in MW (as `np.float64`)
        ==============  =====================================================================================

    Returns
    -------
    BIDPEROFFER_D : pd.DataFrame

    BIDDAYOFFER_D : pd.DataFrame

    """
    # Split bids based on type, no filtering will occur to energy bids.
    energy_bids = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'ENERGY']
    fcas_bids = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] != 'ENERGY']
    energy_price_bids = BIDDAYOFFER_D[BIDDAYOFFER_D['BIDTYPE'] == 'ENERGY']
    fcas_price_bids = BIDDAYOFFER_D[BIDDAYOFFER_D['BIDTYPE'] != 'ENERGY']

    # Filter out fcas_bids that don't have an offered availability greater than zero.
    fcas_bids = fcas_bids[fcas_bids['MAXAVAIL'] > 0.0]

    # Filter out fcas_bids that do not have one bid band availability greater than zero.
    fcas_bids['band_greater_than_zero'] = 0.0
    for band in ['BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4', 'BANDAVAIL5', 'BANDAVAIL6', 'BANDAVAIL7',
                 'BANDAVAIL8', 'BANDAVAIL9', 'BANDAVAIL10']:
        if band in fcas_bids.columns:
            fcas_bids['band_greater_than_zero'] = np.where(fcas_bids[band] > 0.0, 1.0,
                                                           fcas_bids['band_greater_than_zero'])
    fcas_bids = fcas_bids[fcas_bids['band_greater_than_zero'] > 0.0]
    fcas_bids = fcas_bids.drop(['band_greater_than_zero'], axis=1)

    # Filter out fcas_bids where their maximum energy output is less than the fcas enablement minimum value. If the
    capacity_limits['dispatch_type'] = capacity_limits['dispatch_type'].str.upper()
    fcas_bids = pd.merge(
        fcas_bids,
        capacity_limits,
        'left',
        left_on=['DUID', 'DIRECTION'],
        right_on=['unit', 'dispatch_type']
    )
    fcas_bids = fcas_bids[(fcas_bids['capacity'] >= fcas_bids['ENABLEMENTMIN']) | (fcas_bids['capacity'].isna())]

    fcas_bids = pd.merge(
        fcas_bids,
        DISPATCHLOAD.loc[:, ['DUID', 'INITIALMW', 'AGCSTATUS', 'TRADERTYPE']],
        'inner',
        on='DUID'
    )

    fcas_bids_bdu = fcas_bids[fcas_bids['TRADERTYPE'] == 'BIDIRECTIONAL']
    fcas_bids_not_bdu = fcas_bids[~(fcas_bids['TRADERTYPE'] == 'BIDIRECTIONAL')].copy()
    fcas_bids_bdu_gen = fcas_bids_bdu[fcas_bids_bdu['DIRECTION'] == 'GENERATOR']
    fcas_bids_bdu_load = fcas_bids_bdu[fcas_bids_bdu['DIRECTION'] == 'LOAD']
    fcas_bids_bdu_gen_reg = fcas_bids_bdu_gen[fcas_bids_bdu_gen['BIDTYPE'].str.find('REG') != -1].copy()
    fcas_bids_bdu_load_reg = fcas_bids_bdu_load[fcas_bids_bdu_load['BIDTYPE'].str.find('REG') != -1].copy()
    fcas_bids_bdu_gen_con = fcas_bids_bdu_gen[~(fcas_bids_bdu_gen['BIDTYPE'].str.find('REG') != -1)].copy()
    fcas_bids_bdu_load_con = fcas_bids_bdu_load[~(fcas_bids_bdu_load['BIDTYPE'].str.find('REG') != -1)].copy()

    # Filter out fcas_bids where the enablement max is not greater than zero.
    fcas_bids_not_bdu = fcas_bids_not_bdu[fcas_bids_not_bdu['ENABLEMENTMAX'] >= 0.0]
    fcas_bids_bdu_gen_con = fcas_bids_bdu_gen_con[fcas_bids_bdu_gen_con['ENABLEMENTMAX'] >= 0.0]

    fcas_bids_not_bdu = fcas_bids_not_bdu[(fcas_bids_not_bdu['capacity'] >= fcas_bids_not_bdu['ENABLEMENTMIN']) | (
        fcas_bids_not_bdu['capacity'].isna())]

    fcas_bids_bdu_gen_con = fcas_bids_bdu_gen_con[
        (fcas_bids_bdu_gen_con['capacity'] >= fcas_bids_bdu_gen_con['ENABLEMENTMIN']) | (
            fcas_bids_bdu_gen_con['capacity'].isna())]
    fcas_bids_bdu_gen_con = fcas_bids_bdu_gen_con.drop(columns=['capacity'])

    fcas_bids_bdu_gen_con['DIRECTION'] = 'LOAD'
    fcas_bids_bdu_gen_con = pd.merge(
        fcas_bids_bdu_gen_con,
        capacity_limits,
        'left',
        left_on=['DUID', 'DIRECTION'],
        right_on=['unit', 'dispatch_type']
    )
    fcas_bids_bdu_gen_con = fcas_bids_bdu_gen_con[
        (-1 * fcas_bids_bdu_gen_con['capacity'] <= fcas_bids_bdu_gen_con['ENABLEMENTMAX']) | (
            fcas_bids_bdu_gen_con['capacity'].isna())]
    fcas_bids_bdu_gen_con['DIRECTION'] = 'GENERATOR'

    # Filter out fcas_bids where the enablement min is not less than zero.
    fcas_bids_bdu_load_con = fcas_bids_bdu_load_con[fcas_bids_bdu_load_con['ENABLEMENTMIN'] <= 0.0]

    fcas_bids_bdu_load_reg['FILTERENABLMENTMIN'] = fcas_bids_bdu_load_reg['ENABLEMENTMIN']
    fcas_bids_bdu_load_reg_f = fcas_bids_bdu_load_reg.loc[:, ['DUID', 'BIDTYPE', 'FILTERENABLMENTMIN']]

    fcas_bids_bdu_gen_reg['FILTERENABLMENTMAX'] = fcas_bids_bdu_gen_reg['ENABLEMENTMAX']
    fcas_bids_bdu_gen_reg_f = fcas_bids_bdu_gen_reg.loc[:, ['DUID', 'BIDTYPE', 'FILTERENABLMENTMAX']]

    fcas_bids_bdu_load_reg = pd.merge(
        fcas_bids_bdu_load_reg,
        fcas_bids_bdu_gen_reg_f,
        how='left',
        on=['DUID', 'BIDTYPE']
    )

    fcas_bids_bdu_load_reg['FILTERENABLMENTMAX'] = np.where(
        fcas_bids_bdu_load_reg['FILTERENABLMENTMAX'].isna(),
        fcas_bids_bdu_load_reg['ENABLEMENTMAX'],
        fcas_bids_bdu_load_reg['FILTERENABLMENTMAX']
    )

    fcas_bids_bdu_gen_reg = pd.merge(
        fcas_bids_bdu_gen_reg,
        fcas_bids_bdu_load_reg_f,
        how='left',
        on=['DUID', 'BIDTYPE']
    )

    fcas_bids_bdu_gen_reg['FILTERENABLMENTMIN'] = np.where(
        fcas_bids_bdu_gen_reg['FILTERENABLMENTMIN'].isna(),
        fcas_bids_bdu_gen_reg['ENABLEMENTMIN'],
        fcas_bids_bdu_gen_reg['FILTERENABLMENTMIN']
    )

    fcas_bids_bdu_gen_reg = fcas_bids_bdu_gen_reg[fcas_bids_bdu_gen_reg['ENABLEMENTMAX'] >= 0.0]

    fcas_bids_bdu_gen_reg = fcas_bids_bdu_gen_reg[
        (fcas_bids_bdu_gen_reg['capacity'] >= fcas_bids_bdu_gen_reg['ENABLEMENTMIN']) | (
            fcas_bids_bdu_gen_reg['capacity'].isna())]

    fcas_bids_bdu_load_reg = fcas_bids_bdu_load_reg[fcas_bids_bdu_load_reg['ENABLEMENTMIN'] <= 0.0]

    fcas_bids_bdu_load_reg = fcas_bids_bdu_load_reg[
        (-1 * fcas_bids_bdu_load_reg['capacity'] <= fcas_bids_bdu_load_reg['ENABLEMENTMAX']) | (
            fcas_bids_bdu_load_reg['capacity'].isna())]

    fcas_bids = pd.concat([fcas_bids_bdu_load_con, fcas_bids_bdu_gen_con, fcas_bids_not_bdu])

    fcas_bids_bdu_reg = pd.concat([fcas_bids_bdu_gen_reg, fcas_bids_bdu_load_reg])

    # Filter out fcas_bids where the unit is not initially operating between the enablement min and max.
    # Round initial ouput to 5 decimial places because the enablement min and max are given to this number, without
    # this some units are dropped that shouldn't be.
    fcas_bids['INITIALMW'] = np.where(
        (fcas_bids['INITIALMW'] < 0.0) & (fcas_bids['TRADERTYPE'] != 'BIDIRECTIONAL'),
        0.0,
        fcas_bids['INITIALMW']
    )

    fcas_bids = fcas_bids[(fcas_bids['ENABLEMENTMAX'] >= fcas_bids['INITIALMW'].round(5)) &
                          (fcas_bids['ENABLEMENTMIN'] <= fcas_bids['INITIALMW'].round(5))]

    fcas_bids_bdu_reg = \
        fcas_bids_bdu_reg[(fcas_bids_bdu_reg['FILTERENABLMENTMAX'] >= fcas_bids_bdu_reg['INITIALMW'].round(5)) &
                          (fcas_bids_bdu_reg['FILTERENABLMENTMIN'] <= fcas_bids_bdu_reg['INITIALMW'].round(5))]

    fcas_bids = pd.concat([fcas_bids, fcas_bids_bdu_reg])

    # Filter out fcas_bids where the AGC status is not set to 1.0
    fcas_bids = fcas_bids[~((fcas_bids['AGCSTATUS'] == 0.0) & (fcas_bids['BIDTYPE'].isin(['RAISEREG', 'LOWERREG'])))]
    fcas_bids = fcas_bids.drop(['AGCSTATUS', 'INITIALMW', 'capacity'], axis=1)

    # Filter the fcas price bids use the remaining volume bids.
    fcas_price_bids = pd.merge(
        fcas_price_bids,
        fcas_bids.loc[:, ['DUID', 'BIDTYPE', 'DIRECTION']],
        'inner',
        on=['DUID', 'BIDTYPE', 'DIRECTION']
    )

    # Combine fcas and energy bid back together.
    BIDDAYOFFER_D = pd.concat([energy_price_bids, fcas_price_bids])
    BIDPEROFFER_D = pd.concat([energy_bids, fcas_bids])

    band_cols = [col for col in BIDPEROFFER_D.columns if "BAND" in col]

    BIDPEROFFER_D = BIDPEROFFER_D.loc[:, [
        "DUID", "DIRECTION", "BIDTYPE", "MAXAVAIL",
        "ENABLEMENTMIN", "LOWBREAKPOINT", "HIGHBREAKPOINT", "ENABLEMENTMAX"
    ] + band_cols]

    return BIDPEROFFER_D, BIDDAYOFFER_D
