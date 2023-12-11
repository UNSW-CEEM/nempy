import pandas as pd
import numpy as np
import doctest
from nempy.historical_inputs import aemo_to_nempy_name_mapping as an


def _test_setup():
    import sqlite3
    from nempy.historical_inputs import mms_db
    from nempy.historical_inputs import xml_cache
    from nempy.historical_inputs import loaders
    con = sqlite3.connect('market_management_system.db')
    mms_db_manager = mms_db.DBManager(connection=con)
    xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    inputs_loader = loaders.RawInputsLoader(xml_cache_manager, mms_db_manager)
    inputs_loader.set_interval('2019/01/10 12:05:00')
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
    >>> inputs_loader.set_interval('2019/01/10 12:05:00')

    Create the UnitData instance.

    >>> unit_data = UnitData(inputs_loader)

    >>> unit_data.get_unit_bid_availability()
              unit  capacity
    0       AGLHAL     170.0
    1       AGLSOM     160.0
    2      ANGAST1      44.0
    23      BALBG1       0.0
    33      BALBL1       0.0
    ...        ...       ...
    989   YARWUN_1     165.0
    990      YWPS1     380.0
    999      YWPS2     180.0
    1008     YWPS3     350.0
    1017     YWPS4     340.0
    <BLANKLINE>
    [218 rows x 2 columns]
    """

    def __init__(self, raw_input_loader):
        self.raw_input_loader = raw_input_loader
        self.dispatch_interval = 5  # minutes
        self.dispatch_type_name_map = {'GENERATOR': 'generator', 'LOAD': 'load'}
        self.service_name_mapping = {'ENERGY': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s', 'RAISE1SEC': 'raise_1s',
                                     'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min', 'LOWER6SEC': 'lower_6s',
                                     'LOWER1SEC': 'lower_1s', 'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min'}

        self.volume_bids = self.raw_input_loader.get_unit_volume_bids()
        self.fast_start_profiles = self.raw_input_loader.get_unit_fast_start_parameters()
        self.initial_conditions = self.raw_input_loader.get_unit_initial_conditions()
        self.uigf_values = self.raw_input_loader.get_UIGF_values()

        self.price_bids = self.raw_input_loader.get_unit_price_bids()
        self.unit_details = self.raw_input_loader.get_unit_details()

        self.BIDPEROFFER_D = None
        self.fcas_trapeziums = None
        self.updated_fast_start_profiles = None

    def get_unit_bid_availability(self):
        """Get the bid in maximum availability for scheduled units.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_unit_bid_availability()
                  unit  capacity
        0       AGLHAL     170.0
        1       AGLSOM     160.0
        2      ANGAST1      44.0
        23      BALBG1       0.0
        33      BALBL1       0.0
        ...        ...       ...
        989   YARWUN_1     165.0
        990      YWPS1     380.0
        999      YWPS2     180.0
        1008     YWPS3     350.0
        1017     YWPS4     340.0
        <BLANKLINE>
        [218 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            capacity          unit bid in max availability, in MW, \n
                              (as `str`)
            ================  ========================================

        """
        bid_availability = self.volume_bids.loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL']]
        bid_availability = self._remove_non_energy_bids(bid_availability)
        bid_availability = bid_availability.loc[:, ['DUID', 'MAXAVAIL']]
        bid_availability = self._remove_non_scheduled_units(bid_availability)
        bid_availability = an.map_aemo_column_names_to_nempy_names(bid_availability)
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
        0      ARWF1    18.654
        1   BALDHWF1    11.675
        2      BANN1    53.661
        3     BLUFF1     8.655
        4     BNGSF1    98.877
        ..       ...       ...
        57     WGWF1     7.649
        58   WHITSF1     6.075
        59  WOODLWN1    11.659
        60     WRSF1    20.000
        61     WRWF1     7.180
        <BLANKLINE>
        [62 rows x 2 columns]

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

    def get_ramp_rates_used_for_energy_dispatch(self, run_type='no_fast_start_units'):
        """Get ramp rates used for constraining energy dispatch.

        The minimum of bid in ramp rates and scada telemetered ramp rates are used. If 'no_fast_start_units' is given as
        the run_type then no extra process is applied to the ramp rates based on the fast start inflexibility profiles.
        If 'fast_start_first_run' is given then the ramp rates of units starting in fast start modes 0, 1, and 2 are
        excluded. If 'fast_start_second_run' is given then the ramp rates of units ending the interval in fast start
        modes 0, 1, and 2 are excluded, and the ramp rates of units that started interval in mode 2 or smaller, but
        end in mode 3 or greater, have there ramp rates adjusted to account for speeding a portion of the interval
        constrained from ramping up by their dispatch inflexibility profile.


        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_ramp_rates_used_for_energy_dispatch()
                 unit  initial_output  ramp_up_rate  ramp_down_rate
        0      AGLHAL        0.000000    720.000000      720.000000
        1      AGLSOM        0.000000    480.000000      480.000000
        2     ANGAST1        0.000000    840.000000      840.000000
        3       ARWF1       15.800001   1200.000000      600.000000
        4      BALBG1        0.000000   6000.000000     6000.000000
        ..        ...             ...           ...             ...
        275  YARWUN_1      157.019989      0.000000        0.000000
        276     YWPS1      383.959503    177.750006      177.750006
        277     YWPS2      180.445572    177.750006      177.750006
        278     YWPS3      353.460754    175.499997      175.499997
        279     YWPS4      338.782288    180.000000      180.000000
        <BLANKLINE>
        [280 rows x 4 columns]

        Parameters
        ----------
        run_type: str specifying the run type should be one of 'no_fast_start_units', 'fast_start_first_run', or
            'fast_start_second_run'.

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            initial_output    the output/consumption of the unit at \n
                              the start of the dispatch interval, \n
                              in MW, (as `np.float64`)
            ramp_up_rate      the ramp up rate, in MW/h, \n
                              (as `np.float64`)
            ramp_down_rate    the ramp down rate, in MW/h, \n
                              (as `np.float64`)
            ================  ========================================
        """
        ramp_rates = self._get_minimum_of_bid_and_scada_telemetered_ramp_rates()
        # ramp_rates = self._remove_fast_start_units_ending_dispatch_interval_in_mode_two(ramp_rates)
        if run_type == 'fast_start_first_run':
            ramp_rates = self._remove_fast_start_units_starting_in_mode_0_1_2(ramp_rates)
        elif run_type == 'fast_start_second_run':
            if self.updated_fast_start_profiles is None:
                raise ValueError("Can't use run type fast_start_second_run before calling "
                                 "get_fast_start_profiles_for_dispatch.")
            ramp_rates = self._remove_fast_start_units_ending_in_mode_0_1_2(ramp_rates)
            ramp_rates = self._adjust_ramp_rates_of_units_ending_in_mode_three_and_four(ramp_rates,
                                                                                        self.dispatch_interval)
        elif run_type != 'no_fast_start_units':
            raise ValueError("run_type provided not recognised.")

        ramp_rates = ramp_rates.loc[:, ['DUID', 'INITIALMW', 'RAMPUPRATE', 'RAMPDOWNRATE']]
        ramp_rates.columns = ['unit', 'initial_output', 'ramp_up_rate', 'ramp_down_rate']
        return ramp_rates

    def _get_minimum_of_bid_and_scada_telemetered_ramp_rates(self):
        bid_ramp_rates = self.volume_bids.loc[:, ['DUID', 'BIDTYPE', 'RAMPDOWNRATE', 'RAMPUPRATE']]
        bid_ramp_rates = self._remove_non_energy_bids(bid_ramp_rates)
        scada_telemetered_ramp_rates = self.initial_conditions.loc[:, ['DUID', 'INITIALMW', 'RAMPDOWNRATE',
                                                                       'RAMPUPRATE']]
        ramp_rates = pd.merge(bid_ramp_rates, scada_telemetered_ramp_rates, 'left', on='DUID')
        ramp_rates['RAMPDOWNRATE'] = np.fmin(ramp_rates['RAMPDOWNRATE_x'], ramp_rates['RAMPDOWNRATE_y'])
        ramp_rates['RAMPUPRATE'] = np.fmin(ramp_rates['RAMPUPRATE_x'], ramp_rates['RAMPUPRATE_y'])
        return ramp_rates

    def _remove_fast_start_units_starting_in_mode_0_1_2(self, dataframe):
        fast_start_profiles = self._get_fast_start_profiles()
        units_starting_in_mode_0_1_2 = list(
            fast_start_profiles[fast_start_profiles['current_mode'].isin([0, 1, 2])]['unit'].unique())
        dataframe = dataframe[~dataframe['DUID'].isin(units_starting_in_mode_0_1_2)]
        return dataframe

    def _remove_fast_start_units_ending_in_mode_0_1_2(self, dataframe):
        fast_start_profiles = self.updated_fast_start_profiles.copy()
        units_starting_in_mode_0_1_2 = list(
            fast_start_profiles[fast_start_profiles['end_mode'].isin([0, 1, 2])]['unit'].unique())
        dataframe = dataframe[~dataframe['DUID'].isin(units_starting_in_mode_0_1_2)]
        return dataframe

    def _remove_fast_start_units_ending_dispatch_interval_in_mode_two(self, dataframe):
        fast_start_profiles = self._get_fast_start_profiles()
        units_ending_in_mode_two = list(fast_start_profiles[fast_start_profiles['end_mode'] == 2]['unit'].unique())
        dataframe = dataframe[~dataframe['DUID'].isin(units_ending_in_mode_two)]
        return dataframe

    def _adjust_ramp_rates_of_units_ending_in_mode_three_and_four(self, ramp_rates, dispatch_interval):
        """
        If a unit is ending in mode three of four but it has been less than 5 minutes since leaving mode 2 or 1 then
        adjust their ramp rate to account for the limited time operating without a dispatch inflexibility profile
        upper bound
        """
        fast_start_profiles = self.updated_fast_start_profiles.copy()
        if not fast_start_profiles.empty:
            profiles_to_adjust = fast_start_profiles[~fast_start_profiles['time_since_end_of_mode_two'].isna()]
            profiles_to_adjust = profiles_to_adjust.loc[:, ['unit', 'min_loading', 'time_since_end_of_mode_two']]
            profiles_to_adjust = pd.merge(ramp_rates, profiles_to_adjust, 'inner', left_on='DUID',
                                          right_on='unit')
            profiles_to_adjust['ramp_mw_per_min'] = profiles_to_adjust['RAMPUPRATE'] / 60
            profiles_to_adjust['ramp_max'] = profiles_to_adjust['time_since_end_of_mode_two'] * \
                                                   profiles_to_adjust['ramp_mw_per_min'] + \
                                                   profiles_to_adjust['min_loading']
            profiles_to_adjust['RAMPUPRATE'] = (profiles_to_adjust['ramp_max'] -
                                                profiles_to_adjust['INITIALMW']) * \
                                                (60 / dispatch_interval)
            profiles_to_adjust = profiles_to_adjust.drop(
                columns=["unit", "min_loading", "time_since_end_of_mode_two", "unit", "ramp_mw_per_min",
                         "ramp_max"])
            ramp_rates_not_adjusted = ramp_rates[~ramp_rates['DUID'].isin(profiles_to_adjust['DUID'])]
            ramp_rates = pd.concat([profiles_to_adjust, ramp_rates_not_adjusted])
        return ramp_rates

    def _adjust_ramp_rates_to_account_for_fast_start_mode_two_inflexibility_profile(self, ramp_rates):
        fast_start_profiles = self._get_fast_start_profiles()
        if not fast_start_profiles.empty:
            fast_start_profiles = self._fast_start_mode_two_initial_mw(fast_start_profiles)
            fast_start_target = self._fast_start_adjusted_ramp_up_rate(ramp_rates, fast_start_profiles,
                                                                       self.dispatch_interval)
            ramp_rates = pd.merge(ramp_rates, fast_start_target, 'left', left_on='DUID', right_on='unit')
            ramp_rates['INITIALMW'] = np.where(~ramp_rates['fast_start_initial_mw'].isna(),
                                               ramp_rates['fast_start_initial_mw'], ramp_rates['INITIALMW'])
            ramp_rates['RAMPUPRATE'] = np.where(~ramp_rates['new_ramp_up_rate'].isna(),
                                                ramp_rates['new_ramp_up_rate'], ramp_rates['RAMPUPRATE'])
        return ramp_rates

    @staticmethod
    def _fast_start_mode_two_initial_mw(fast_start_profile):
        """Calculates the initial conditions of the unit had it adhering to the dispatch inflexibility profile."""
        units_in_mode_two = \
            fast_start_profile[(fast_start_profile['current_mode'] == 2)].copy()
        units_in_mode_two['fast_start_initial_mw'] = (((units_in_mode_two['time_in_current_mode'])
                                                       / units_in_mode_two['mode_two_length']) *
                                                      units_in_mode_two['min_loading'])
        return units_in_mode_two

    @staticmethod
    def _fast_start_adjusted_ramp_up_rate(ramp_rates, fast_start_end_condition, dispatch_interval):
        """Calculate the ramp rate required to adjust for a unit spending the first part of the dispatch interval
           constrained by its dispatch inflexibility profile."""
        fast_start_end_condition = fast_start_end_condition[(fast_start_end_condition['current_mode'] == 2) &
                                                            (fast_start_end_condition['end_mode'] > 2)]
        fast_start_end_condition = pd.merge(ramp_rates, fast_start_end_condition, left_on='DUID', right_on='unit')
        fast_start_end_condition['ramp_mw_per_min'] = fast_start_end_condition['RAMPUPRATE'] / 60
        fast_start_end_condition['ramp_max'] = fast_start_end_condition['time_after_mode_two'] * \
                                               fast_start_end_condition['ramp_mw_per_min'] + fast_start_end_condition[
                                                   'min_loading']
        fast_start_end_condition['new_ramp_up_rate'] = (fast_start_end_condition['ramp_max'] -
                                                        fast_start_end_condition['fast_start_initial_mw']) * \
                                                       (60 / dispatch_interval)
        return fast_start_end_condition.loc[:, ['unit', 'fast_start_initial_mw', 'new_ramp_up_rate']]

    def get_as_bid_ramp_rates(self):
        """Get ramp rates used as bid by units.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_as_bid_ramp_rates()
                  unit  ramp_up_rate  ramp_down_rate
        0       AGLHAL         720.0           720.0
        1       AGLSOM         480.0           480.0
        2      ANGAST1         840.0           840.0
        9        ARWF1        1200.0           600.0
        23      BALBG1        6000.0          6000.0
        ...        ...           ...             ...
        989   YARWUN_1           0.0             0.0
        990      YWPS1         180.0           180.0
        999      YWPS2         180.0           180.0
        1008     YWPS3         180.0           180.0
        1017     YWPS4         180.0           180.0
        <BLANKLINE>
        [280 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            ramp_up_rate      the ramp up rate, in MW/h, \n
                              (as `np.float64`)
            ramp_down_rate    the ramp down rate, in MW/h, \n
                              (as `np.float64`)
            ================  ========================================
        """
        ramp_rates = self.volume_bids.loc[:, ['DUID', 'BIDTYPE', 'RAMPDOWNRATE', 'RAMPUPRATE']]
        ramp_rates = ramp_rates[ramp_rates['BIDTYPE'] == 'ENERGY'].copy()
        ramp_rates = ramp_rates.loc[:, ['DUID', 'RAMPUPRATE', 'RAMPDOWNRATE']]
        ramp_rates = an.map_aemo_column_names_to_nempy_names(ramp_rates)
        return ramp_rates

    def get_initial_unit_output(self):
        """Get unit outputs at the start of the dispatch interval.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_initial_unit_output()
                 unit  initial_output
        0      AGLHAL        0.000000
        1      AGLSOM        0.000000
        2     ANGAST1        0.000000
        3       APD01        0.000000
        4       ARWF1       15.800001
        ..        ...             ...
        283  YARWUN_1      157.019989
        284     YWPS1      383.959503
        285     YWPS2      180.445572
        286     YWPS3      353.460754
        287     YWPS4      338.782288
        <BLANKLINE>
        [288 rows x 2 columns]

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

    def get_fast_start_profiles_for_dispatch(self, unconstrained_dispatch=None):
        """Get the parameters needed to construct the fast dispatch inflexibility profiles used for dispatch.

        If the results of an non fast start constrained dispatch run are provided then these are used to commit fast
        start units starting the interval in mode zero, when the they have a non-zero dispatch result.

        For more info on fast start dispatch inflexibility profiles :download:`see AEMO docs <../../docs/pdfs/Fast_Start_Unit_Inflexibility_Profile_Model_October_2014.pdf>`.

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            end_mode          the fast start mode the unit will end \n
                              the dispatch interval in, (as `np.int64`)
            time_in_end_mode  the amount of time the unit will have \n
                              spend in the end mode at the end of the \n
                              dispatch interval, (as `np.float64`)
            mode_two_length   the length the units mode two, in minutes \n
                              (as `np.float64`)
            mode_four_length  the length the units mode four, in minutes \n
                              (as `np.float64`)
            min_loading       the mininum opperating level of the unit \n
                              during mode three, in MW, (as `no.float64`)
            ================  ========================================
        """
        profiles = self._get_fast_start_profiles(unconstrained_dispatch=unconstrained_dispatch)
        self.updated_fast_start_profiles = profiles
        profiles['mode_two_length'] = np.float64(profiles['mode_two_length'])
        profiles['mode_four_length'] = np.float64(profiles['mode_four_length'])
        profiles['min_loading'] = np.float64(profiles['min_loading'])
        return profiles.loc[:, ['unit', 'end_mode', 'time_in_end_mode', 'mode_two_length',
                                'mode_four_length', 'min_loading']]

    def _get_fast_start_profiles(self, unconstrained_dispatch=None):
        fast_start_profiles = self.fast_start_profiles
        fast_start_profiles = an.map_aemo_column_names_to_nempy_names(fast_start_profiles)
        if unconstrained_dispatch is not None:
            fast_start_profiles = self.update_modes(fast_start_profiles, unconstrained_dispatch)
        return fast_start_profiles

    @staticmethod
    def update_modes(fast_start_profiles, unconstrained_dispatch):
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
        mask = (fsp['temp_current_mode'] == 1) & (fsp['mode_one_length'] - fsp['temp_time_in_current_mode'] <=
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 2
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_one_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        fsp = pd.concat([df1, df2])

        # Move units from mode two to mode three
        mask = (fsp['temp_current_mode'] == 2) & (fsp['mode_two_length'] - fsp['temp_time_in_current_mode'] <=
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 3
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_two_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        df1['time_since_end_of_mode_two'] = df1['time_left_in_interval']
        fsp = pd.concat([df1, df2])

        # Move units from mode three to mode four
        mask = (fsp['temp_current_mode'] == 3) & (fsp['mode_three_length'] - fsp['temp_time_in_current_mode'] <=
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 4
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_three_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        fsp = pd.concat([df1, df2])

        # Move units from mode four to mode five
        mask = (fsp['temp_current_mode'] == 4) & (fsp['mode_four_length'] - fsp['temp_time_in_current_mode'] <=
                                             fsp['time_left_in_interval'])
        df1 = fsp[mask].copy()
        df2 = fsp[~mask].copy()
        df1['temp_current_mode'] = 5
        df1['time_left_in_interval'] = df1['time_left_in_interval'] - (df1['mode_four_length'] - df1['temp_time_in_current_mode'])
        df1['temp_time_in_current_mode'] = 0.0
        fsp = pd.concat([df1, df2])

        fsp['end_mode'] = fsp['temp_current_mode']
        fsp['time_in_end_mode'] = fsp['temp_time_in_current_mode'] + fsp['time_left_in_interval']

        return fsp.loc[:, ['unit', 'min_loading', 'current_mode', 'end_mode', 'time_in_current_mode',
                           'time_in_end_mode', 'mode_one_length', 'mode_two_length', 'mode_three_length',
                           'mode_four_length', 'time_since_end_of_mode_two']]

    @staticmethod
    def _commit_fast_start_units_in_mode_zero_if_they_have_non_zero_unconstrained_dispatch(fast_start_profiles,
                                                                                           unconstrained_dispatch):
        if unconstrained_dispatch is not None:
            unconstrained_dispatch = unconstrained_dispatch[unconstrained_dispatch['service'] == 'energy']
            fast_start_profiles = pd.merge(fast_start_profiles, unconstrained_dispatch, on='unit')
            fast_start_profiles['current_mode'] = np.where((fast_start_profiles['current_mode'] == 0) &
                                                           (fast_start_profiles['dispatch'] > 0.0), 1,
                                                           fast_start_profiles['current_mode'])
        return fast_start_profiles

    @staticmethod
    def _fast_start_calc_end_interval_state(fast_start_profile, dispatch_interval):

        def clac_mode_length(data):
            if data['previous_mode'] == 1:
                return data['mode_one_length']
            elif data['previous_mode'] == 2:
                return data['mode_two_length']
            elif data['previous_mode'] == 3:
                return data['mode_three_length']
            elif data['previous_mode'] == 4:
                return data['mode_four_length']
            else:
                return np.inf

        fast_start_profile['previous_mode'] = fast_start_profile['current_mode']

        fast_start_profile['current_mode_length'] = fast_start_profile.apply(lambda x: clac_mode_length(x), axis=1)

        fast_start_profile['time_in_current_mode_at_end'] = \
            fast_start_profile['time_in_current_mode'] + dispatch_interval

        fast_start_profile['end_mode'] = np.where(fast_start_profile['time_in_current_mode_at_end'] >
                                                  fast_start_profile['current_mode_length'],
                                                  fast_start_profile['current_mode'] + 1,
                                                  fast_start_profile['current_mode'])

        fast_start_profile['time_in_end_mode'] = np.where(
            fast_start_profile['end_mode'] != fast_start_profile['current_mode'],
            fast_start_profile['time_in_current_mode_at_end'] -
            fast_start_profile['current_mode_length'],
            fast_start_profile['time_in_current_mode_at_end'])

        fast_start_profile['time_after_mode_two'] = np.where((fast_start_profile['current_mode'] == 2) &
                                                             (fast_start_profile['end_mode'] == 3),
                                                             fast_start_profile['time_in_end_mode'],
                                                             np.NAN)

        for i in range(1, 10):
            fast_start_profile['previous_mode'] = fast_start_profile['end_mode']

            fast_start_profile['current_mode_length'] = fast_start_profile.apply(lambda x: clac_mode_length(x), axis=1)

            fast_start_profile['end_mode'] = np.where(fast_start_profile['time_in_end_mode'] >
                                                      fast_start_profile['current_mode_length'],
                                                      fast_start_profile['previous_mode'] + 1,
                                                      fast_start_profile['previous_mode'])

            fast_start_profile['time_in_end_mode'] = np.where(fast_start_profile['end_mode'] !=
                                                              fast_start_profile['previous_mode'],
                                                              fast_start_profile['time_in_end_mode'] -
                                                              fast_start_profile['current_mode_length'],
                                                              fast_start_profile['time_in_end_mode'])

            fast_start_profile['time_after_mode_two'] = np.where((fast_start_profile['current_mode'] == 2) &
                                                                 (fast_start_profile['end_mode'] == 3),
                                                                 fast_start_profile['time_in_end_mode'],
                                                                 fast_start_profile['time_after_mode_two'])

        fast_start_profile['mode_two_length'] = fast_start_profile['mode_two_length'].astype(np.float64)
        fast_start_profile['mode_four_length'] = fast_start_profile['mode_four_length'].astype(np.float64)
        fast_start_profile['min_loading'] = fast_start_profile['min_loading'].astype(np.float64)
        return fast_start_profile.loc[:, ['unit', 'min_loading', 'current_mode', 'end_mode', 'time_in_current_mode',
                                          'time_in_end_mode', 'mode_one_length', 'mode_two_length', 'mode_three_length',
                                          'mode_four_length', 'time_after_mode_two']]

    def get_unit_info(self):
        """Get unit information.

        Examples
        --------

        >>> inputs_loader = _test_setup()

        >>> unit_data = UnitData(inputs_loader)

        >>> unit_data.get_unit_info()
                 unit region dispatch_type  loss_factor
        0      AGLHAL    SA1     generator     0.971500
        1     AGLNOW1   NSW1     generator     1.003700
        2    AGLSITA1   NSW1     generator     1.002400
        3      AGLSOM   VIC1     generator     0.984743
        4     ANGAST1    SA1     generator     1.005674
        ..        ...    ...           ...          ...
        477     YWNL1   VIC1     generator     0.957300
        478     YWPS1   VIC1     generator     0.969600
        479     YWPS2   VIC1     generator     0.957300
        480     YWPS3   VIC1     generator     0.957300
        481     YWPS4   VIC1     generator     0.957300
        <BLANKLINE>
        [482 rows x 4 columns]

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
        unit_details['LOSSFACTOR'] = unit_details['TRANSMISSIONLOSSFACTOR'] * unit_details['DISTRIBUTIONLOSSFACTOR']
        unit_details = unit_details.loc[:, ['DUID', 'DISPATCHTYPE', 'CONNECTIONPOINTID', 'REGIONID', 'LOSSFACTOR']]
        unit_details = an.map_aemo_column_names_to_nempy_names(unit_details)
        unit_details = an.map_aemo_column_values_to_nempy_name(unit_details, column='dispatch_type')
        return unit_details.loc[:, ['unit', 'region', 'dispatch_type', 'loss_factor']]

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
                unit    service    1      2    3     4    5     6     7     8    9     10
        0     AGLHAL     energy  0.0    0.0  0.0   0.0  0.0   0.0  60.0   0.0  0.0  160.0
        1     AGLSOM     energy  0.0    0.0  0.0   0.0  0.0   0.0   0.0   0.0  0.0  170.0
        2    ANGAST1     energy  0.0    0.0  0.0   0.0  0.0  50.0   0.0   0.0  0.0   50.0
        9      ARWF1     energy  0.0  241.0  0.0   0.0  0.0   0.0   0.0   0.0  0.0    0.0
        23    BALBG1     energy  0.0    0.0  0.0   0.0  0.0   0.0   0.0   0.0  0.0   30.0
        ..       ...        ...  ...    ...  ...   ...  ...   ...   ...   ...  ...    ...
        364    YWPS4   raise_6s  0.0    0.0  0.0  10.0  5.0   0.0   0.0   0.0  0.0   10.0
        365    YWPS4  lower_reg  0.0    0.0  0.0   0.0  0.0   0.0   0.0  20.0  0.0    0.0
        366    YWPS4  raise_reg  0.0    0.0  0.0   0.0  0.0   0.0   5.0  10.0  0.0    5.0
        369   SWAN_E  lower_reg  0.0    0.0  0.0   0.0  0.0   0.0   5.0   0.0  0.0   52.0
        370   SWAN_E  raise_reg  0.0    0.0  0.0   5.0  0.0   0.0   3.0   0.0  0.0   49.0
        <BLANKLINE>
        [591 rows x 12 columns]

        >>> price_bids
                unit     service           1          2           3           4           5           6           7            8             9            10
        0     AGLHAL      energy  -971.50000   0.000000  270.863915  358.298915  406.873915  484.593915  562.313915  1326.641540  10277.372205  13600.018785
        1     AGLSOM      energy  -984.74292   0.000000   83.703148  108.321721  142.787723  279.666989  444.119057   985.727663  13097.937562  14278.732950
        2    ANGAST1      energy -1005.67390   0.000000  125.709237  201.335915  300.887574  382.135969  593.337544  1382.650761  10678.245470  14582.271550
        3      ARWF1      energy  -969.10000 -63.001191    1.996346    4.002383    8.004766   15.999841   31.999682    63.999364    127.998728  14051.950000
        4     BALBG1      energy  -994.80000   0.000000   19.915896   47.372376   75.177036  109.447896  298.440000   443.133660  10047.489948  14424.600000
        ..       ...         ...         ...        ...         ...         ...         ...         ...         ...          ...           ...           ...
        586  ASQENC1    raise_6s     0.03000   0.300000    0.730000    0.990000    1.980000    5.000000    9.900000    17.700000    100.000000  10000.000000
        587  ASTHYD1    raise_6s     0.00000   0.490000    1.450000    4.950000    9.950000   15.000000   60.000000   200.000000   1000.000000  14000.000000
        588   VENUS1  raise_5min     0.00000   1.000000    2.780000    3.980000    4.980000    8.600000    9.300000    14.600000     20.000000   1000.000000
        589   VENUS1   raise_60s     0.00000   1.000000    2.780000    3.980000    4.980000    8.600000    9.300000    14.600000     20.000000   1000.000000
        590   VENUS1    raise_6s     0.01000   0.600000    2.780000    3.980000    4.980000    8.600000    9.300000    14.000000     20.000000   1000.000000
        <BLANKLINE>
        [591 rows x 12 columns]

        Multiple Returns
        ----------------
        volume_bids : pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`)
            service           the service the bid applies to, (as `str`)
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
        volume_bids = volume_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', '10']]
        price_bids = price_bids[price_bids['unit'].isin(list(unit_info['unit']))]
        price_bids = price_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                        '6', '7', '8', '9', '10']]

        # Price bids  coming from xml have already been scaled by loss factors, so we need to undo this.
        price_bids = self._unscale_price_bids(price_bids, unit_info)

        return volume_bids, price_bids

    @staticmethod
    def _unscale_price_bids(price_bids, unit_info):
        price_bids = pd.merge(price_bids, unit_info.loc[:, ['unit', 'loss_factor']], on='unit')
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

        After calling it the error goes away.

        >>> volume_bids, price_bids = unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        >>> unit_data.get_fcas_max_availability()
                unit     service  max_availability
        0      APD01  raise_5min              34.0
        1      APD01   raise_60s              34.0
        2      APD01    raise_6s              17.0
        3    ASNENC1  raise_5min              12.0
        4    ASNENC1   raise_60s               4.0
        ..       ...         ...               ...
        364    YWPS4    raise_6s              15.0
        365    YWPS4   lower_reg              15.0
        366    YWPS4   raise_reg              15.0
        369   SWAN_E   lower_reg              10.0
        370   SWAN_E   raise_reg              25.0
        <BLANKLINE>
        [311 rows x 3 columns]

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
                unit     service  max_availability
        0      APD01  raise_5min              34.0
        1      APD01   raise_60s              34.0
        2      APD01    raise_6s              17.0
        3    ASNENC1  raise_5min              12.0
        4    ASNENC1   raise_60s               4.0
        ..       ...         ...               ...
        364    YWPS4    raise_6s              15.0
        365    YWPS4   lower_reg              15.0
        366    YWPS4   raise_reg              15.0
        369   SWAN_E   lower_reg              10.0
        370   SWAN_E   raise_reg              25.0
        <BLANKLINE>
        [311 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`)
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
        return self.fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]

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
                 unit    service  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
        16       BW01  lower_reg         35.015640       309.27185       344.287490         520.80701       520.80701
        17       BW01  raise_reg         35.015640       309.27185       309.271850         485.79137       520.80701
        24   CALL_B_1  lower_reg         15.000000       180.00000       195.000000         270.30002       270.30002
        25   CALL_B_1  raise_reg         15.000000       180.00000       180.000000         205.00000       220.00000
        55       ER01  lower_reg         24.906273       490.02502       514.931293         680.00000       680.00000
        ..        ...        ...               ...             ...              ...               ...             ...
        359     YWPS3  raise_reg         14.625000       250.00000       250.000000         370.37500       385.00000
        365     YWPS4  lower_reg         15.000000       250.00000       265.000000         385.00000       385.00000
        366     YWPS4  raise_reg         15.000000       250.00000       250.000000         370.00000       385.00000
        369    SWAN_E  lower_reg         10.000000       145.00000       202.000000         362.50000       362.50000
        370    SWAN_E  raise_reg         25.000000       145.00000       145.000000         305.50000       362.50000
        <BLANKLINE>
        [75 rows x 7 columns]

        Returns
        -------
        pd.DataFrame

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
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

    def _get_scada_ramp_up_rates(self):
        initial_cons = self.initial_conditions.loc[:, ['DUID', 'INITIALMW', 'RAMPUPRATE']]
        units_with_scada_ramp_rates = list(
            initial_cons[(~initial_cons['RAMPUPRATE'].isna()) & initial_cons['RAMPUPRATE'] != 0]['DUID'])
        initial_cons = initial_cons[initial_cons['DUID'].isin(units_with_scada_ramp_rates)]
        return initial_cons

    def _get_scada_ramp_down_rates(self):
        initial_cons = self.initial_conditions.loc[:, ['DUID', 'INITIALMW', 'RAMPDOWNRATE']]
        units_with_scada_ramp_rates = list(
            initial_cons[(~initial_cons['RAMPDOWNRATE'].isna()) & initial_cons['RAMPDOWNRATE'] != 0]['DUID'])
        initial_cons = initial_cons[initial_cons['DUID'].isin(units_with_scada_ramp_rates)]
        return initial_cons

    def _get_raise_reg_units_with_scada_ramp_rates(self):
        reg_units = self.get_fcas_regulation_trapeziums().loc[:, ['unit', 'service']]
        scada_ramp_up_rates =  an.map_aemo_column_names_to_nempy_names(self._get_scada_ramp_up_rates())
        reg_units = pd.merge(scada_ramp_up_rates, reg_units, 'inner', on='unit')
        reg_units = reg_units[(reg_units['service'] == 'raise_reg') & (~reg_units['ramp_up_rate'].isna())]
        reg_units = reg_units.loc[:, ['unit', 'service']]
        return reg_units

    def _get_lower_reg_units_with_scada_ramp_rates(self):
        reg_units = self.get_fcas_regulation_trapeziums().loc[:, ['unit', 'service']]
        scada_ramp_down_rates =  an.map_aemo_column_names_to_nempy_names(self._get_scada_ramp_down_rates())
        reg_units = pd.merge(scada_ramp_down_rates, reg_units, 'inner', on='unit')
        reg_units = reg_units[(reg_units['service'] == 'lower_reg') & (~reg_units['ramp_down_rate'].isna())]
        reg_units = reg_units.loc[:, ['unit', 'service']]
        return reg_units

    def get_scada_ramp_down_rates_of_lower_reg_units(self, run_type='no_fast_start_units'):
        """Get the scada ramp down rates for unit with a lower regulation bid.

        Only units with scada ramp rates and a lower regulation bid that passes enablement criteria are returned.

        Examples
        --------

        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)

        Required calls before calling get_scada_ramp_down_rates_of_lower_reg_units.

        >>> volume_bids, price_bids =  unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        Now the method can be called.

        >>> unit_data.get_scada_ramp_down_rates_of_lower_reg_units().head()
                unit  initial_output  ramp_down_rate
        36      BW01      425.125000      420.187683
        40  CALL_B_1      219.699997      240.000000
        74      ER01      636.000000      298.875275
        76      ER03      678.925049      297.187500
        77      ER04      518.550049      298.312225

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            initial_output    the output/consumption of the unit at \n
                              the start of the dispatch interval, \n
                              in MW, (as `np.float64`)
            ramp_down_rate    the ramp down rate, in MW/h, \n
                              (as `np.float64`)
            ================  ========================================

        Raises
        ------
        MethodCallOrderError
            if the method is called before add_fcas_trapezium_constraints.
        """
        if self.fcas_trapeziums is None:
            raise MethodCallOrderError(
                'Call add_fcas_trapezium_constraints before get_scada_ramp_down_rates_of_lower_reg_units.')
        lower_reg_units = self._get_lower_reg_units_with_scada_ramp_rates()
        scada_ramp_down_rates = self._get_scada_ramp_down_rates()
        scada_ramp_down_rates = scada_ramp_down_rates[scada_ramp_down_rates['DUID'].isin(lower_reg_units['unit'])]
        if run_type == 'fast_start_first_run':
            scada_ramp_down_rates = self._remove_fast_start_units_starting_in_mode_0_1_2(scada_ramp_down_rates)
        elif run_type == 'fast_start_second_run':
            if self.updated_fast_start_profiles is None:
                raise ValueError("Can't use run type fast_start_second_run before calling "
                                 "get_fast_start_profiles_for_dispatch.")
            scada_ramp_down_rates = self._remove_fast_start_units_ending_in_mode_0_1_2(scada_ramp_down_rates)
        elif run_type != 'no_fast_start_units':
            raise ValueError("run_type provided not recognised.")
        return an.map_aemo_column_names_to_nempy_names(scada_ramp_down_rates)

    def get_scada_ramp_up_rates_of_raise_reg_units(self, run_type='no_fast_start_units'):
        """Get the scada ramp up rates for unit with a raise regulation bid.

        Only units with scada ramp rates and a raise regulation bid that passes enablement criteria are returned.

        Examples
        --------

        >>> inputs_loader = _test_setup()
        >>> unit_data = UnitData(inputs_loader)

        Required calls before calling get_scada_ramp_up_rates_of_raise_reg_units.

        >>> volume_bids, price_bids =  unit_data.get_processed_bids()
        >>> unit_data.add_fcas_trapezium_constraints()

        Now the method can be called.

        >>> unit_data.get_scada_ramp_up_rates_of_raise_reg_units().head()
                unit  initial_output  ramp_up_rate
        36      BW01      425.125000    420.187683
        40  CALL_B_1      219.699997    240.000000
        74      ER01      636.000000    299.999542
        76      ER03      678.925049    297.750092
        77      ER04      518.550049    298.875275

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            unit              unique identifier for units, (as `str`) \n
            initial_output    the output/consumption of the unit at \n
                              the start of the dispatch interval, \n
                              in MW, (as `np.float64`)
            ramp_up_rate      the ramp up rate, in MW/h, \n
                              (as `np.float64`)
            ================  ========================================

        Raises
        ------
        MethodCallOrderError
            if the method is called before add_fcas_trapezium_constraints.
        """
        if self.fcas_trapeziums is None:
            raise MethodCallOrderError(
                'Call add_fcas_trapezium_constraints before get_scada_ramp_up_rates_of_raise_reg_units.')
        scada_ramp_up_rates = self._get_scada_ramp_up_rates()
        raise_reg_units = self._get_raise_reg_units_with_scada_ramp_rates()
        scada_ramp_up_rates = scada_ramp_up_rates[scada_ramp_up_rates['DUID'].isin(raise_reg_units['unit'])]
        if run_type == 'fast_start_first_run':
            scada_ramp_up_rates = self._remove_fast_start_units_starting_in_mode_0_1_2(scada_ramp_up_rates)
        elif run_type == 'fast_start_second_run':
            if self.updated_fast_start_profiles is None:
                raise ValueError("Can't use run type fast_start_second_run before calling "
                                 "get_fast_start_profiles_for_dispatch.")
            scada_ramp_up_rates = self._remove_fast_start_units_ending_in_mode_0_1_2(scada_ramp_up_rates)
            scada_ramp_up_rates = (
                self._adjust_ramp_rates_of_units_ending_in_mode_three_and_four(scada_ramp_up_rates,
                                                                               self.dispatch_interval))
        elif run_type != 'no_fast_start_units':
            raise ValueError("run_type provided not recognised.")
        return an.map_aemo_column_names_to_nempy_names(scada_ramp_up_rates)

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
                unit     service  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
        0      APD01  raise_5min              34.0             0.0              0.0               0.0             0.0
        1      APD01   raise_60s              34.0             0.0              0.0               0.0             0.0
        2      APD01    raise_6s              17.0             0.0              0.0               0.0             0.0
        3    ASNENC1  raise_5min              12.0             0.0              0.0               0.0             0.0
        4    ASNENC1   raise_60s               4.0             0.0              0.0               0.0             0.0
        ..       ...         ...               ...             ...              ...               ...             ...
        360    YWPS4  lower_5min              15.0           250.0            265.0             385.0           385.0
        361    YWPS4   lower_60s              20.0           250.0            270.0             385.0           385.0
        362    YWPS4    lower_6s              25.0           250.0            275.0             385.0           385.0
        363    YWPS4   raise_60s              10.0           220.0            220.0             390.0           400.0
        364    YWPS4    raise_6s              15.0           220.0            220.0             390.0           405.0
        <BLANKLINE>
        [236 rows x 7 columns]

        Returns
        -------
        pd.DataFrame

            ================   =======================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit, \n
                               (as `str`)
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
    ... 'MAXAVAIL': [60.0, 0.0],
    ... 'ENABLEMENTMIN': [20.0, 0.0],
    ... 'LOWBREAKPOINT': [40.0, 0.0],
    ... 'HIGHBREAKPOINT': [60.0, 0.0],
    ... 'ENABLEMENTMAX': [80.0, 0.0]})

    >>> service_name_mapping = {'ENERGY': 'energy', 'RAISE60SEC': 'raise_60s'}

    >>> fcas_trapeziums = _format_fcas_trapezium_constraints(BIDPEROFFER_D, service_name_mapping)

    >>> print(fcas_trapeziums)
      unit    service  max_availability  enablement_min  low_break_point  high_break_point  enablement_max
    0    A  raise_60s              60.0            20.0             40.0              60.0            80.0

    """
    BIDPEROFFER_D = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] != 'ENERGY']
    trapezium_cons = BIDPEROFFER_D.loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL', 'ENABLEMENTMIN', 'LOWBREAKPOINT',
                                           'HIGHBREAKPOINT', 'ENABLEMENTMAX']]
    trapezium_cons.columns = ['unit', 'service', 'max_availability', 'enablement_min', 'low_break_point',
                              'high_break_point', 'enablement_max']
    trapezium_cons['service'] = trapezium_cons['service'].apply(lambda x: service_name_mapping[x])
    return trapezium_cons


def _format_volume_bids(BIDPEROFFER_D, service_name_mapping):
    """Re-formats the AEMO MSS table BIDDAYOFFER_D to be compatible with the Spot market class.

    Examples
    --------

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG'],
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
      unit    service      1     2    3     4     5     6     7    8    9   10
    0    A     energy  100.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0
    1    B  raise_reg   50.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0

    Parameters
    ----------
    BIDPEROFFER_D : pd.DataFrame

        ===========  ====================================================
        Columns:     Description:
        DUID         unique identifier of a unit (as `str`)
        BIDTYPE      the service being provided (as `str`)
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
        1                 bid volume in the 1st band, in MW (as `np.float64`)
        2                 bid volume in the 2nd band, in MW (as `np.float64`)
        :
        10                bid volume in the nth band, in MW (as `np.float64`)
        max_availability  the offered cap on dispatch, only used directly for fcas bids, in MW (as `np.float64`)
        ================  ======================================================================================
    """

    volume_bids = BIDPEROFFER_D.loc[:, ['DUID', 'BIDTYPE', 'BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4',
                                        'BANDAVAIL5', 'BANDAVAIL6', 'BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9',
                                        'BANDAVAIL10']]
    volume_bids.columns = ['unit', 'service', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    volume_bids['service'] = volume_bids['service'].apply(lambda x: service_name_mapping[x])
    return volume_bids


def _format_price_bids(BIDDAYOFFER_D, service_name_mapping):
    """Re-formats the AEMO MSS table BIDDAYOFFER_D to be compatible with the Spot market class.

    Examples
    --------

    >>> BIDDAYOFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG'],
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
      unit    service      1     2    3     4     5     6     7    8    9   10
    0    A     energy  100.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0
    1    B  raise_reg   50.0  10.0  0.0  10.0  10.0  10.0  10.0  0.0  0.0  0.0

    Parameters
    ----------
    BIDDAYOFFER_D : pd.DataFrame

        ===========  ====================================================
        Columns:     Description:
        DUID         unique identifier of a unit (as `str`)
        BIDTYPE      the service being provided (as `str`)
        PRICEBAND1   bid price in the 1st band, in MW (as `np.float64`)
        PRICEBAND2   bid price in the 2nd band, in MW (as `np.float64`)
        PRICEBAND10  bid price in the 10th band, in MW (as `np.float64`)
        ===========  ====================================================

    Returns
    ----------
    demand_coefficients : pd.DataFrame

        ========  ================================================================
        Columns:  Description:
        unit      unique identifier of a dispatch unit (as `str`)
        service   the service being provided, optional, if missing energy assumed (as `str`)
        1         bid price in the 1st band, in MW (as `np.float64`)
        2         bid price in the 2nd band, in MW (as `np.float64`)
        10        bid price in the nth band, in MW (as `np.float64`)
        ========  ================================================================
    """

    price_bids = BIDDAYOFFER_D.loc[:, ['DUID', 'BIDTYPE', 'PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3', 'PRICEBAND4',
                                       'PRICEBAND5', 'PRICEBAND6', 'PRICEBAND7', 'PRICEBAND8', 'PRICEBAND9',
                                       'PRICEBAND10']]
    price_bids.columns = ['unit', 'service', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    price_bids['service'] = price_bids['service'].apply(lambda x: service_name_mapping[x])
    return price_bids


def _format_unit_info(DUDETAILSUMMARY, dispatch_type_name_map):
    """Re-formats the AEMO MSS table DUDETAILSUMMARY to be compatible with the Spot market class.

    Loss factors get combined into a single value.

    Examples
    --------

    >>> DUDETAILSUMMARY = pd.DataFrame({
    ...   'DUID': ['A', 'B'],
    ...   'DISPATCHTYPE': ['GENERATOR', 'LOAD'],
    ...   'CONNECTIONPOINTID': ['X2', 'Z30'],
    ...   'REGIONID': ['NSW1', 'SA1'],
    ...   'TRANSMISSIONLOSSFACTOR': [0.9, 0.85],
    ...   'DISTRIBUTIONLOSSFACTOR': [0.9, 0.99]})

    >>> dispatch_type_name_map = {'GENERATOR': 'generator', 'LOAD': 'load'}

    >>> unit_info = _format_unit_info(DUDETAILSUMMARY, dispatch_type_name_map)

    >>> print(unit_info)
      unit dispatch_type connection_point region  loss_factor
    0    A     generator               X2   NSW1       0.8100
    1    B          load              Z30    SA1       0.8415

    Parameters
    ----------
    BIDPEROFFER_D : pd.DataFrame

        ======================  =================================================================
        Columns:                Description:
        DUID                    unique identifier of a unit (as `str`)
        DISPATCHTYPE            whether the unit is GENERATOR or LOAD (as `str`)
        CONNECTIONPOINTID       the unique identifier of the units location (as `str`)
        REGIONID                the unique identifier of the units market region (as `str`)
        TRANSMISSIONLOSSFACTOR  the units loss factor at the transmission level (as `np.float64`)
        DISTRIBUTIONLOSSFACTOR  the units loss factor at the distribution level (as `np.float64`)
        ======================  =================================================================

    Returns
    ----------
    unit_info : pd.DataFrame

        ======================  ==============================================================================
        Columns:                Description:
        unit                    unique identifier of a unit (as `str`)
        dispatch_type           whether the unit is 'generator' or 'load' (as `str`)
        connection_point        the unique identifier of the units location (as `str`)
        region                  the unique identifier of the units market region (as `str`)
        loss_factor             the units combined transmission and distribution loss factor (as `np.float64`)
        ======================  ==============================================================================
    """

    # Combine loss factors.
    DUDETAILSUMMARY['LOSSFACTOR'] = DUDETAILSUMMARY['TRANSMISSIONLOSSFACTOR'] * \
                                    DUDETAILSUMMARY['DISTRIBUTIONLOSSFACTOR']
    unit_info = DUDETAILSUMMARY.loc[:, ['DUID', 'DISPATCHTYPE', 'CONNECTIONPOINTID', 'REGIONID', 'LOSSFACTOR']]
    unit_info.columns = ['unit', 'dispatch_type', 'connection_point', 'region', 'loss_factor']
    unit_info['dispatch_type'] = unit_info['dispatch_type'].apply(lambda x: dispatch_type_name_map[x])
    return unit_info


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
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG', 'RAISEREG'],
    ...   'PRICEBAND1': [100.0, 50.0, 60.0],
    ...   'PRICEBAND2': [110.0, 60.0, 80.0]})

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'INITIALMW': [50.0, 60.0, 60.0],
    ...   'AGCSTATUS': [0.0, 1.0, 1.0]})

    >>> capacity_limits = pd.DataFrame({
    ...   'unit': ['A', 'B', 'C'],
    ...   'capacity': [50.0, 120.0, 80.0]})

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0
    1    C  RAISEREG        50.0         0.0     100.0           20.0           50.0            70.0          100.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0
    1    C  RAISEREG        60.0        80.0

    If unit C's FCAS MAX AVAILABILITY is changed to zero then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> BIDPEROFFER_D_mod['MAXAVAIL'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0, BIDPEROFFER_D_mod['MAXAVAIL'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's BANDAVAIL1 is changed to zero then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> BIDPEROFFER_D_mod['BANDAVAIL1'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0,
    ...                                            BIDPEROFFER_D_mod['BANDAVAIL1'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's capacity is changed to less than its enablement min then it gets filtered out.

    >>> capacity_limits_mod = capacity_limits.copy()

    >>> capacity_limits_mod['capacity'] = np.where(capacity_limits_mod['unit'] == 'C', 0.0,
    ...                                            capacity_limits_mod['capacity'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits_mod)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

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
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0
    1    C  RAISEREG        50.0         0.0     100.0            0.0           50.0            70.0            0.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0
    1    C  RAISEREG        60.0        80.0

    If unit C's INITIALMW is changed to less than its enablement min then it gets filtered out.

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> DISPATCHLOAD_mod['INITIALMW'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 19.0, DISPATCHLOAD_mod['INITIALMW'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's AGCSTATUS is changed to  0.0 then it gets filtered out.

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> DISPATCHLOAD_mod['AGCSTATUS'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 0.0, DISPATCHLOAD_mod['AGCSTATUS'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = _enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  BANDAVAIL2  MAXAVAIL  ENABLEMENTMIN  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0        10.0       0.0            0.0            0.0             0.0            0.0
    0    B  RAISEREG        50.0        10.0     100.0           20.0           50.0            70.0          100.0

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

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
    fcas_bids = pd.merge(fcas_bids, capacity_limits, 'left', left_on='DUID', right_on='unit')
    fcas_bids = fcas_bids[(fcas_bids['capacity'] >= fcas_bids['ENABLEMENTMIN']) | (fcas_bids['capacity'].isna())]
    fcas_bids = fcas_bids.drop(['unit', 'capacity'], axis=1)

    # Filter out fcas_bids where the enablement max is not greater than zero.
    fcas_bids = fcas_bids[fcas_bids['ENABLEMENTMAX'] >= 0.0]

    # Filter out fcas_bids where the unit is not initially operating between the enablement min and max.
    # Round initial ouput to 5 decimial places because the enablement min and max are given to this number, without
    # this some units are dropped that shouldn't be.
    fcas_bids = pd.merge(fcas_bids, DISPATCHLOAD.loc[:, ['DUID', 'INITIALMW', 'AGCSTATUS']], 'inner', on='DUID')
    fcas_bids['INITIALMW'] = np.where(fcas_bids['INITIALMW'] < 0.0, 0.0, fcas_bids['INITIALMW'])
    fcas_bids = fcas_bids[(fcas_bids['ENABLEMENTMAX'] >= fcas_bids['INITIALMW'].round(5)) &
                          (fcas_bids['ENABLEMENTMIN'] <= fcas_bids['INITIALMW'].round(5))]

    # Filter out fcas_bids where the AGC status is not set to 1.0
    fcas_bids = fcas_bids[~((fcas_bids['AGCSTATUS'] == 0.0) & (fcas_bids['BIDTYPE'].isin(['RAISEREG', 'LOWERREG'])))]
    fcas_bids = fcas_bids.drop(['AGCSTATUS', 'INITIALMW'], axis=1)

    # Filter the fcas price bids use the remaining volume bids.
    fcas_price_bids = pd.merge(fcas_price_bids, fcas_bids.loc[:, ['DUID', 'BIDTYPE']], 'inner', on=['DUID', 'BIDTYPE'])

    # Combine fcas and energy bid back together.
    BIDDAYOFFER_D = pd.concat([energy_price_bids, fcas_price_bids])
    BIDPEROFFER_D = pd.concat([energy_bids, fcas_bids])

    return BIDPEROFFER_D, BIDDAYOFFER_D


def _determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D):
    """Approximates the unit limits used in historical dispatch, returns inputs compatible with the Spot market class.

    The exact method for determining unit limits in historical dispatch is not known. This function first assumes the
    limits are set by the AVAILABILITY, INITIALMW, RAMPUPRATE and RAMPDOWNRATE columns in the MMS table DISPATCHLOAD.
    Then if the historical dispatch amount recorded in TOTALCLEARED is outside these limits the limits are extended.
    This occurs in the following circumstances:

    * For units operating in fast start mode, i.e. dispatch mode not equal to 0.0, if the TOTALCLEARED is outside
      the ramp rate limits then new less restrictive ramp rates are calculated that allow the unit to ramp to the
      TOTALCLEARED amount.

    * For units operating with a SEMIDISPATCHCAP of 1.0 and an offered MAXAVAIL (from the MMS table) amount less than
      the AVAILABILITY, and a TOTALCLEARED amount less than or equal to MAXAVAIL, then MAXAVAIL is used as the upper
      capacity limit instead of TOTALCLEARED.

    * If the unit is incapable of ramping down to its capacity limit then the capacity limit is increased to the ramp
      down limit, to prevent a set of infeasible unit limits.

    From the testing conducted in the tests/historical_testing module these adjustments appear sufficient to ensure
    units can be dispatched to their TOTALCLEARED amount.

    Examples
    --------

    An example where a fast start units initial limits are too restrictive, note the non fast start unit with the same
    paramaters does not have it ramp rates adjusted.

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C', 'D'],
    ...   'INITIALMW': [50.0, 50.0, 50.0, 50.0],
    ...   'AVAILABILITY': [90.0, 90.0, 90.0, 90.0],
    ...   'RAMPDOWNRATE': [120.0, 120.0, 120.0, 120.0],
    ...   'RAMPUPRATE': [120.0, 120.0, 120.0, 120.0],
    ...   'TOTALCLEARED': [80.0, 80.0, 30.0, 30.0],
    ...   'DISPATCHMODE': [1.0, 0.0, 4.0, 0.0],
    ...   'SEMIDISPATCHCAP': [0.0, 0.0, 0.0, 0.0]})

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C', 'D'],
    ...   'BIDTYPE': ['ENERGY', 'ENERGY', 'ENERGY', 'ENERGY'],
    ...   'MAXAVAIL': [100.0, 100.0, 100.0, 100.0]})

    >>> unit_limits = _determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

    >>> print(unit_limits)
      unit  initial_output  capacity  ramp_down_rate  ramp_up_rate
    0    A            50.0      90.0           120.0         360.0
    1    B            50.0      90.0           120.0         120.0
    2    C            50.0      90.0           240.0         120.0
    3    D            50.0      90.0           120.0         120.0

    An example with a unit operating with a SEMIDISPATCHCAP  of 1.0. Only unit A meets all the criteria for having its
    capacity adjusted from the reported AVAILABILITY value.

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C', 'D'],
    ...   'INITIALMW': [50.0, 50.0, 50.0, 50.0],
    ...   'AVAILABILITY': [90.0, 90.0, 90.0, 90.0],
    ...   'RAMPDOWNRATE': [600.0, 600.0, 600.0, 600.0],
    ...   'RAMPUPRATE': [600.0, 600.0, 600.0, 600.0],
    ...   'TOTALCLEARED': [70.0, 90.0, 80.0, 70.0],
    ...   'DISPATCHMODE': [0.0, 0.0, 0.0, 0.0],
    ...   'SEMIDISPATCHCAP': [1.0, 1.0, 1.0, 0.0]})

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C', 'D'],
    ...   'BIDTYPE': ['ENERGY', 'ENERGY', 'ENERGY', 'ENERGY'],
    ...   'MAXAVAIL': [80.0, 80.0, 100.0, 80.0]})

    >>> unit_limits = _determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

    >>> print(unit_limits)
      unit  initial_output  capacity  ramp_down_rate  ramp_up_rate
    0    A            50.0      80.0           600.0         600.0
    1    B            50.0      90.0           600.0         600.0
    2    C            50.0      90.0           600.0         600.0
    3    D            50.0      90.0           600.0         600.0

    An example where the AVAILABILITY is lower than the ramp down limit.

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A'],
    ...   'INITIALMW': [50.0],
    ...   'AVAILABILITY': [30.0],
    ...   'RAMPDOWNRATE': [120.0],
    ...   'RAMPUPRATE': [120.0],
    ...   'TOTALCLEARED': [40.0],
    ...   'DISPATCHMODE': [0.0],
    ...   'SEMIDISPATCHCAP': [0.0]})

    >>> BIDPEROFFER_D = pd.DataFrame({
    ...   'DUID': ['A'],
    ...   'BIDTYPE': ['ENERGY'],
    ...   'MAXAVAIL': [30.0]})

    >>> unit_limits = _determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

    >>> print(unit_limits)
      unit  initial_output  capacity  ramp_down_rate  ramp_up_rate
    0    A            50.0      40.0           120.0         120.0

    Parameters
    ----------
    DISPATCHLOAD : pd.DataFrame

        ===============  ======================================================================================
        Columns:         Description:
        DUID             unique identifier of a dispatch unit (as `str`)
        INITIALMW        the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        AVAILABILITY     the reported maximum output of the unit for dispatch interval, in MW (as `np.float64`)
        RAMPDOWNRATE     the maximum rate at which the unit can decrease output, in MW/h (as `np.float64`)
        RAMPUPRATE       the maximum rate at which the unit can increase output, in MW/h (as `np.float64`)
        TOTALCLEARED     the dispatch target for interval, in MW (as `np.float64`)
        DISPATCHMODE     fast start operating mode, 0.0 for not in fast start mode, 1.0, 2.0, 3.0, 4.0 for in
                         fast start mode, (as `np.float64`)
        SEMIDISPATCHCAP  0.0 for not applicable, 1.0 if the semi scheduled unit output is capped by dispatch
                         target.
        ===============  ======================================================================================

    BIDPEROFFER_D : pd.DataFrame
        Should only be bids of type energy.

        ===============  ======================================================================================
        Columns:         Description:
        DUID             unique identifier of a dispatch unit (as `str`)
        MAXAVAIL         the maximum unit output as specified in the units bid, in MW (as `np.float64`)
        ===============  ======================================================================================

    Returns
    -------
    unit_limits : pd.DataFrame

        ==============  =====================================================================================
        Columns:        Description:
        unit            unique identifier of a dispatch unit (as `str`)
        initial_output  the output of the unit at the start of the dispatch interval, in MW (as `np.float64`)
        capacity        the maximum output of the unit if unconstrained by ramp rate, in MW (as `np.float64`)
        ramp_down_rate  the maximum rate at which the unit can decrease output, in MW/h (as `np.float64`)
        ramp_up_rate    the maximum rate at which the unit can increase output, in MW/h (as `np.float64`)
        ==============  =====================================================================================

    """

    # Override ramp rates for fast start units.
    ic = DISPATCHLOAD  # DISPATCHLOAD provides the initial operating conditions (ic).
    ic['RAMPMAX'] = ic['INITIALMW'] + ic['RAMPUPRATE'] * (5 / 60)
    ic['RAMPUPRATE'] = np.where((ic['TOTALCLEARED'] > ic['RAMPMAX']) & (ic['DISPATCHMODE'] != 0.0),
                                (ic['TOTALCLEARED'] - ic['INITIALMW']) * (60 / 5), ic['RAMPUPRATE'])
    ic['RAMPMIN'] = ic['INITIALMW'] - ic['RAMPDOWNRATE'] * (5 / 60)
    ic['RAMPDOWNRATE'] = np.where((ic['TOTALCLEARED'] < ic['RAMPMIN']) & (ic['DISPATCHMODE'] != 0.0),
                                  (ic['INITIALMW'] - ic['TOTALCLEARED']) * (60 / 5), ic['RAMPDOWNRATE'])

    ic['AVAILABILITY'] = np.where(ic['AVAILABILITY'] < ic['TOTALCLEARED'], ic['TOTALCLEARED'], ic['AVAILABILITY'])

    # Override AVAILABILITY when SEMIDISPATCHCAP is 1.0
    BIDPEROFFER_D = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'ENERGY']
    ic = pd.merge(ic, BIDPEROFFER_D.loc[:, ['DUID', 'MAXAVAIL']], 'inner', on='DUID')
    ic['AVAILABILITY'] = np.where((ic['MAXAVAIL'] < ic['AVAILABILITY']) & (ic['SEMIDISPATCHCAP'] == 1.0) &
                                  (ic['TOTALCLEARED'] <= ic['MAXAVAIL']), ic['MAXAVAIL'],
                                  ic['AVAILABILITY'])

    # Where the availability is lower than the ramp down min set the AVAILABILITY to equal the ramp down min.
    ic['AVAILABILITY'] = np.where(ic['AVAILABILITY'] < ic['RAMPMIN'], ic['RAMPMIN'], ic['AVAILABILITY'])

    # Format for compatibility with the Spot market class.
    ic = ic.loc[:, ['DUID', 'INITIALMW', 'AVAILABILITY', 'RAMPDOWNRATE', 'RAMPUPRATE']]
    ic.columns = ['unit', 'initial_output', 'capacity', 'ramp_down_rate', 'ramp_up_rate']
    return ic
