import pandas as pd
import numpy as np

from nempy.historical import historical_inputs_from_xml as hist_xml


class HistoricalUnits:
    def __init__(self, mms_database, xml_inputs_cache, interval):
        self.mms_database = mms_database
        self.xml_inputs = hist_xml.XMLInputs(cache_folder=xml_inputs_cache, interval=interval)
        self.interval = interval

        self.dispatch_interval = 5  # minutes
        self.dispatch_type_name_map = {'GENERATOR': 'generator', 'LOAD': 'load'}
        self.service_name_mapping = {'ENERGY': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg',
                                     'RAISE6SEC': 'raise_6s',
                                     'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min', 'LOWER6SEC': 'lower_6s',
                                     'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min'}

        self.xml_volume_bids = self.xml_inputs.get_unit_volume_bids()
        self.xml_fast_start_profiles = self.xml_inputs.get_unit_fast_start_parameters()
        self.xml_initial_conditions = self.xml_inputs.get_unit_initial_conditions_dataframe()
        self.xml_ugif_values = self.xml_inputs.get_UGIF_values()

        self.BIDDAYOFFER_D = self.mms_database.BIDDAYOFFER_D.get_data(self.interval)
        self.DUDETAILSUMMARY = self.mms_database.DUDETAILSUMMARY.get_data(self.interval)

    def get_unit_bid_availability(self):
        bid_availability = self.xml_volume_bids
        bid_availability = bid_availability.loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL', 'RAMPDOWNRATE', 'RAMPUPRATE']]
        bid_availability = bid_availability[bid_availability['BIDTYPE'] == 'ENERGY']

        non_schedualed_units = list(self.get_unit_uigf_limits()['unit'])

        initial_cons = self.xml_initial_conditions.loc[:, ['DUID', 'INITIALMW']]

        bid_availability = bid_availability[~bid_availability['DUID'].isin(non_schedualed_units)]
        bid_availability = pd.merge(bid_availability, initial_cons, 'inner', on='DUID')

        # bid_availability['RAMPMIN'] = bid_availability['INITIALMW'] - bid_availability['RAMPDOWNRATE'] / 12
        # bid_availability['MAXAVAIL'] = np.where(bid_availability['RAMPMIN'] > bid_availability['MAXAVAIL'],
        #                                         bid_availability['RAMPMIN'], bid_availability['MAXAVAIL'])

        bid_availability = bid_availability.loc[:, ['DUID', 'MAXAVAIL']]
        bid_availability.columns = ['unit', 'capacity']
        return bid_availability

    def get_unit_uigf_limits(self):
        ugif = self.xml_ugif_values.loc[:, ['DUID', 'UGIF']]
        ugif.columns = ['unit', 'capacity']
        return ugif

    def get_ramp_rates_used_for_energy_dispatch(self):
        ramp_rates = self.xml_volume_bids.loc[:, ['DUID', 'BIDTYPE', 'RAMPDOWNRATE', 'RAMPUPRATE']]
        ramp_rates = ramp_rates[ramp_rates['BIDTYPE'] == 'ENERGY']

        initial_cons = self.xml_initial_conditions.loc[:, ['DUID', 'INITIALMW', 'RAMPDOWNRATE', 'RAMPUPRATE']]

        fast_start_profiles = self.get_fast_start_profiles()
        units_ending_in_mode_two = list(fast_start_profiles[fast_start_profiles['next_mode'] == 2]['unit'].unique())

        ramp_rates = pd.merge(ramp_rates, initial_cons, 'left', on='DUID')
        ramp_rates = ramp_rates[~ramp_rates['DUID'].isin(units_ending_in_mode_two)]

        ramp_rates['RAMPDOWNRATE'] = np.fmin(ramp_rates['RAMPDOWNRATE_x'], ramp_rates['RAMPDOWNRATE_y'])
        ramp_rates['RAMPUPRATE'] = np.fmin(ramp_rates['RAMPUPRATE_x'], ramp_rates['RAMPUPRATE_y'])

        if not fast_start_profiles.empty:
            fast_start_profiles = fast_start_mode_two_targets(fast_start_profiles)
            fast_start_target = fast_start_adjusted_ramp_up_rate(ramp_rates, fast_start_profiles, self.dispatch_interval)
            ramp_rates = pd.merge(ramp_rates, fast_start_target, 'left', left_on='DUID', right_on='unit')
            ramp_rates['INITIALMW'] = np.where(~ramp_rates['fast_start_initial_mw'].isna(),
                                               ramp_rates['fast_start_initial_mw'], ramp_rates['INITIALMW'])
            ramp_rates['RAMPUPRATE'] = np.where(~ramp_rates['new_ramp_up_rate'].isna(),
                                                ramp_rates['new_ramp_up_rate'], ramp_rates['RAMPUPRATE'])

        ramp_rates = ramp_rates.loc[:, ['DUID', 'INITIALMW', 'RAMPUPRATE', 'RAMPDOWNRATE']]
        ramp_rates.columns = ['unit', 'initial_output', 'ramp_up_rate', 'ramp_down_rate']
        return ramp_rates

    def get_fast_start_profiles(self):
        fast_start_profiles = self.xml_fast_start_profiles
        fast_start_profiles = fast_start_profiles.loc[:, ['DUID', 'MinLoadingMW', 'CurrentMode', 'CurrentModeTime',
                                                          'T1', 'T2', 'T3', 'T4']]
        fast_start_profiles = fast_start_profiles.rename(
            columns={'DUID': 'unit', 'MinLoadingMW': 'min_loading', 'CurrentMode': 'current_mode',
                     'CurrentModeTime': 'time_in_current_mode', 'T1': 'mode_one_length', 'T2': 'mode_two_length',
                     'T3': 'mode_three_length', 'T4': 'mode_four_length'})
        fast_start_profiles = fast_start_calc_end_interval_state(fast_start_profiles, self.dispatch_interval)
        return fast_start_profiles

    def get_fast_start_violation(self):
        return self.xml_inputs.get_non_intervention_violations()['fast_start']

    def get_unit_info(self):
        unit_info = format_unit_info(self.DUDETAILSUMMARY, self.dispatch_type_name_map)
        return unit_info.loc[:, ['unit', 'region', 'dispatch_type', 'loss_factor']]

    def get_unit_availability(self):
        bid_availability = self.get_unit_bid_availability()
        ugif_availability = self.get_unit_uigf_limits()
        return pd.concat([bid_availability, ugif_availability])

    def get_processed_bids(self):
        ugif_values = self.xml_inputs.get_UGIF_values()
        BIDDAYOFFER_D = self.mms_database.BIDDAYOFFER_D.get_data(self.interval)
        BIDPEROFFER_D = self.xml_volume_bids.drop(['RAMPDOWNRATE', 'RAMPUPRATE'], axis=1)
        initial_conditions = self.xml_initial_conditions
        unit_info = self.get_unit_info()
        unit_availability = self.get_unit_availability()

        DISPATCHLOAD = self.mms_database.DISPATCHLOAD.get_data(self.interval)
        BIDPEROFFER_D = scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)
        BIDPEROFFER_D = scaling_for_agc_ramp_rates(BIDPEROFFER_D, initial_conditions)
        BIDPEROFFER_D = scaling_for_uigf(BIDPEROFFER_D, ugif_values)
        self.BIDPEROFFER_D, BIDDAYOFFER_D = enforce_preconditions_for_enabling_fcas(
            BIDPEROFFER_D, BIDDAYOFFER_D, initial_conditions, unit_availability)

        volume_bids = format_volume_bids(self.BIDPEROFFER_D, self.service_name_mapping)
        price_bids = format_price_bids(BIDDAYOFFER_D, self.service_name_mapping)
        volume_bids = volume_bids[volume_bids['unit'].isin(list(unit_info['unit']))]
        volume_bids = volume_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                          '6', '7', '8', '9', '10']]
        price_bids = price_bids[price_bids['unit'].isin(list(unit_info['unit']))]
        price_bids = price_bids.loc[:, ['unit', 'service', '1', '2', '3', '4', '5',
                                        '6', '7', '8', '9', '10']]
        return volume_bids, price_bids

    def add_fcas_trapezium_constraints(self):
        self.fcas_trapeziums = format_fcas_trapezium_constraints(self.BIDPEROFFER_D, self.service_name_mapping)

    def get_fcas_max_availability(self):
        return self.fcas_trapeziums.loc[:, ['unit', 'service', 'max_availability']]

    def get_fcas_regulation_trapeziums(self):
        return self.fcas_trapeziums[self.fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]

    def get_scada_ramp_up_rates(self):
        initial_cons = self.xml_initial_conditions.loc[:, ['DUID', 'INITIALMW', 'RAMPUPRATE']]
        initial_cons.columns = ['unit', 'initial_output', 'ramp_rate']
        units_with_scada_ramp_rates = list(
            initial_cons[(~initial_cons['ramp_rate'].isna()) & initial_cons['ramp_rate'] != 0]['unit'])
        initial_cons = initial_cons[initial_cons['unit'].isin(units_with_scada_ramp_rates)]
        return initial_cons

    def get_scada_ramp_down_rates(self):
        initial_cons = self.xml_initial_conditions.loc[:, ['DUID', 'INITIALMW', 'RAMPDOWNRATE']]
        initial_cons.columns = ['unit', 'initial_output', 'ramp_rate']
        units_with_scada_ramp_rates = list(
            initial_cons[(~initial_cons['ramp_rate'].isna()) & initial_cons['ramp_rate'] != 0]['unit'])
        initial_cons = initial_cons[initial_cons['unit'].isin(units_with_scada_ramp_rates)]
        return initial_cons

    def get_raise_reg_units_with_scada_ramp_rates(self):
        reg_units = self.get_fcas_regulation_trapeziums().loc[:, ['unit', 'service']]
        reg_units = pd.merge(self.get_scada_ramp_up_rates(), reg_units, 'inner', on='unit')
        reg_units = reg_units[(reg_units['service'] == 'raise_reg') & (~reg_units['ramp_rate'].isna())]
        reg_units = reg_units.loc[:, ['unit', 'service']]
        return reg_units

    def get_lower_reg_units_with_scada_ramp_rates(self):
        reg_units = self.get_fcas_regulation_trapeziums().loc[:, ['unit', 'service']]
        reg_units = pd.merge(self.get_scada_ramp_down_rates(), reg_units, 'inner', on='unit')
        reg_units = reg_units[(reg_units['service'] == 'lower_reg') & (~reg_units['ramp_rate'].isna())]
        reg_units = reg_units.loc[:, ['unit', 'service']]
        return reg_units

    def get_contingency_services(self):
        return self.fcas_trapeziums[~self.fcas_trapeziums['service'].isin(['raise_reg', 'lower_reg'])]




def format_fcas_trapezium_constraints(BIDPEROFFER_D, service_name_mapping):
    """Extracts and re-formats the fcas trapezium data from the AEMO MSS table BIDDAYOFFER_D.

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

    >>> fcas_trapeziums = format_fcas_trapezium_constraints(BIDPEROFFER_D)

    >>> print(fcas_trapeziums)
      unit    service  ...  high_break_point  enablement_max
    0    A  raise_60s  ...              60.0            80.0
    <BLANKLINE>
    [1 rows x 7 columns]

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

    Returns
    ----------
    fcas_trapeziums : pd.DataFrame

            ================   ======================================================================
            Columns:           Description:
            unit               unique identifier of a dispatch unit (as `str`)
            service            the contingency service being offered (as `str`)
            max_availability   the maximum volume of the contingency service in MW (as `np.float64`)
            enablement_min     the energy dispatch level at which the unit can begin to provide the
                               contingency service, in MW (as `np.float64`)
            low_break_point    the energy dispatch level at which the unit can provide the full
                               contingency service offered, in MW (as `np.float64`)
            high_break_point   the energy dispatch level at which the unit can no longer provide the
                               full contingency service offered, in MW (as `np.float64`)
            enablement_max     the energy dispatch level at which the unit can no longer begin
                               the contingency service, in MW (as `np.float64`)
            ================   ======================================================================
    """
    BIDPEROFFER_D = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] != 'ENERGY']
    trapezium_cons = BIDPEROFFER_D.loc[:, ['DUID', 'BIDTYPE', 'MAXAVAIL', 'ENABLEMENTMIN', 'LOWBREAKPOINT',
                                           'HIGHBREAKPOINT', 'ENABLEMENTMAX']]
    trapezium_cons.columns = ['unit', 'service', 'max_availability', 'enablement_min', 'low_break_point',
                              'high_break_point', 'enablement_max']
    trapezium_cons['service'] = trapezium_cons['service'].apply(lambda x: service_name_mapping[x])
    return trapezium_cons


def format_volume_bids(BIDPEROFFER_D, service_name_mapping):
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

    >>> volume_bids = format_volume_bids(BIDPEROFFER_D)

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


def format_price_bids(BIDDAYOFFER_D, service_name_mapping):
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

    >>> price_bids = format_price_bids(BIDDAYOFFER_D)

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


def format_unit_info(DUDETAILSUMMARY, dispatch_type_name_map):
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

    >>> unit_info = format_unit_info(DUDETAILSUMMARY)

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


def scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD):
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

    >>> BIDPEROFFER_D_out = scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)

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

    >>> BIDPEROFFER_D = scaling_for_agc_enablement_limits(BIDPEROFFER_D, DISPATCHLOAD)

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
    lower_reg = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'LOWERREG']
    raise_reg = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'RAISEREG']
    bids_not_subject_to_scaling = BIDPEROFFER_D[~BIDPEROFFER_D['BIDTYPE'].isin(['RAISEREG', 'LOWERREG'])]

    # Merge in AGC enablement values from dispatch load so they can be compared to offer values.
    lower_reg = pd.merge(lower_reg, DISPATCHLOAD.loc[:, ['DUID', 'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN']],
                         'inner', on='DUID')
    raise_reg = pd.merge(raise_reg, DISPATCHLOAD.loc[:, ['DUID', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN']],
                         'inner', on='DUID')

    # Scale lower reg lower trapezium slope.
    lower_reg['LOWBREAKPOINT'] = np.where(lower_reg['LOWERREGENABLEMENTMIN'] > lower_reg['ENABLEMENTMIN'],
                                          lower_reg['LOWBREAKPOINT'] +
                                          (lower_reg['LOWERREGENABLEMENTMIN'] - lower_reg['ENABLEMENTMIN']),
                                          lower_reg['LOWBREAKPOINT'])
    lower_reg['ENABLEMENTMIN'] = np.where(lower_reg['LOWERREGENABLEMENTMIN'] > lower_reg['ENABLEMENTMIN'],
                                          lower_reg['LOWERREGENABLEMENTMIN'], lower_reg['ENABLEMENTMIN'])
    # Scale lower reg upper trapezium slope.
    lower_reg['HIGHBREAKPOINT'] = np.where(lower_reg['LOWERREGENABLEMENTMAX'] < lower_reg['ENABLEMENTMAX'],
                                           lower_reg['HIGHBREAKPOINT'] -
                                           (lower_reg['ENABLEMENTMAX'] - lower_reg['LOWERREGENABLEMENTMAX']),
                                           lower_reg['HIGHBREAKPOINT'])
    lower_reg['ENABLEMENTMAX'] = np.where(lower_reg['LOWERREGENABLEMENTMAX'] < lower_reg['ENABLEMENTMAX'],
                                          lower_reg['LOWERREGENABLEMENTMAX'], lower_reg['ENABLEMENTMAX'])

    # Scale raise reg lower trapezium slope.
    raise_reg['LOWBREAKPOINT'] = np.where(raise_reg['RAISEREGENABLEMENTMIN'] > raise_reg['ENABLEMENTMIN'],
                                          raise_reg['LOWBREAKPOINT'] +
                                          (raise_reg['RAISEREGENABLEMENTMIN'] - raise_reg['ENABLEMENTMIN']),
                                          raise_reg['LOWBREAKPOINT'])
    raise_reg['ENABLEMENTMIN'] = np.where(raise_reg['RAISEREGENABLEMENTMIN'] > raise_reg['ENABLEMENTMIN'],
                                          raise_reg['RAISEREGENABLEMENTMIN'], raise_reg['ENABLEMENTMIN'])
    # Scale raise reg upper trapezium slope.
    raise_reg['HIGHBREAKPOINT'] = np.where(raise_reg['RAISEREGENABLEMENTMAX'] < raise_reg['ENABLEMENTMAX'],
                                           raise_reg['HIGHBREAKPOINT'] -
                                           (raise_reg['ENABLEMENTMAX'] - raise_reg['RAISEREGENABLEMENTMAX']),
                                           raise_reg['HIGHBREAKPOINT'])
    raise_reg['ENABLEMENTMAX'] = np.where(raise_reg['RAISEREGENABLEMENTMAX'] < raise_reg['ENABLEMENTMAX'],
                                          raise_reg['RAISEREGENABLEMENTMAX'], raise_reg['ENABLEMENTMAX'])

    # Drop un need columns
    raise_reg = raise_reg.drop(['RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN'], axis=1)
    lower_reg = lower_reg.drop(['LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN'], axis=1)

    # Combined bids back together.
    BIDPEROFFER_D = pd.concat([bids_not_subject_to_scaling, lower_reg, raise_reg])

    return BIDPEROFFER_D


def scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD):
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

    >>> BIDPEROFFER_D_out = scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD)

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

    >>> BIDPEROFFER_D_out = scaling_for_agc_ramp_rates(BIDPEROFFER_D, DISPATCHLOAD)

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


def scaling_for_uigf(BIDPEROFFER_D, ugif_values):
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

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'AVAILABILITY': [120.0, 90.0, 80.0]})

    >>> DUDETAILSUMMARY = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'SCHEDULE_TYPE': ['SCHEDULED', 'SCHEDULED', 'SEMI-SCHEDULED']})

    >>> BIDPEROFFER_D_out = scaling_for_uigf(BIDPEROFFER_D, DISPATCHLOAD, DUDETAILSUMMARY)

    >>> print(BIDPEROFFER_D_out.loc[:, ['DUID', 'BIDTYPE', 'HIGHBREAKPOINT', 'ENABLEMENTMAX']])
      DUID     BIDTYPE  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A      ENERGY             0.0            0.0
    1    B    RAISEREG            80.0          100.0
    0    C  LOWER60SEC            60.0           80.0

    In this case we change the availability of unit C so it does not need scaling.

    >>> DISPATCHLOAD = pd.DataFrame({
    ...   'DUID': ['A', 'B', 'C'],
    ...   'AVAILABILITY': [120.0, 90.0, 91.0]})

    >>> BIDPEROFFER_D_out = scaling_for_uigf(BIDPEROFFER_D, DISPATCHLOAD, DUDETAILSUMMARY)

    >>> print(BIDPEROFFER_D_out.loc[:, ['DUID', 'BIDTYPE', 'HIGHBREAKPOINT', 'ENABLEMENTMAX']])
      DUID     BIDTYPE  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A      ENERGY             0.0            0.0
    1    B    RAISEREG            80.0          100.0
    0    C  LOWER60SEC            70.0           90.0

    Parameters
    ----------

    BIDPEROFFER_D : pd.DataFrame

        ==============  ====================================================
        Columns:        Description:
        DUID            unique identifier of a unit (as `str`)
        BIDTYPE         the service being provided (as `str`)
        MAXAVAIL        the offered maximum capacity, in MW (as `np.float64`)
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
        AVAILABILITY     the reported maximum output of the unit for dispatch interval, in MW (as `np.float64`)
        ===============  ======================================================================================

    DUDETAILSUMMARY : pd.DataFrame

        ===============  ======================================================================================
        Columns:         Description:
        DUID             unique identifier of a dispatch unit (as `str`)
        SCHEDULE_TYPE    the schedule type of the plant i.e. SCHEDULED, SEMI-SCHEDULED or NON-SCHEDULED (as `str`)
        ===============  ======================================================================================

    """
    # Split bid based on the scaling that needs to be done.
    semi_scheduled_units = ugif_values['DUID'].unique()
    energy_bids = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'ENERGY']
    fcas_bids = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] != 'ENERGY']
    fcas_semi_scheduled = fcas_bids[fcas_bids['DUID'].isin(semi_scheduled_units)]
    fcas_not_semi_scheduled = fcas_bids[~fcas_bids['DUID'].isin(semi_scheduled_units)]

    fcas_semi_scheduled = pd.merge(fcas_semi_scheduled, ugif_values.loc[:, ['DUID', 'UGIF']],
                                   'inner', on='DUID')

    def get_new_high_break_point(availability, high_break_point, enablement_max):
        if enablement_max > availability:
            high_break_point = high_break_point - (enablement_max - availability)
        return high_break_point

    # Scale high break points.
    fcas_semi_scheduled['HIGHBREAKPOINT'] = \
        fcas_semi_scheduled.apply(lambda x: get_new_high_break_point(x['UGIF'], x['HIGHBREAKPOINT'],
                                                                     x['ENABLEMENTMAX']),
                                  axis=1)

    # Adjust ENABLEMENTMAX.
    fcas_semi_scheduled['ENABLEMENTMAX'] = \
        np.where(fcas_semi_scheduled['ENABLEMENTMAX'] > fcas_semi_scheduled['UGIF'],
                 fcas_semi_scheduled['UGIF'], fcas_semi_scheduled['ENABLEMENTMAX'])

    fcas_semi_scheduled.drop(['UGIF'], axis=1)

    # Combined bids back together.
    BIDPEROFFER_D = pd.concat([energy_bids, fcas_not_semi_scheduled, fcas_semi_scheduled])

    return BIDPEROFFER_D


def enforce_preconditions_for_enabling_fcas(BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits):
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

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    1    C  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [3 rows x 9 columns]

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0
    1    C  RAISEREG        60.0        80.0

    If unit C's FCAS MAX AVAILABILITY is changed to zero then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> BIDPEROFFER_D_mod['MAXAVAIL'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0, BIDPEROFFER_D_mod['MAXAVAIL'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [2 rows x 9 columns]

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's BANDAVAIL1 is changed to zero then it gets filtered out.

    >>> BIDPEROFFER_D_mod = BIDPEROFFER_D.copy()

    >>> BIDPEROFFER_D_mod['BANDAVAIL1'] = np.where(BIDPEROFFER_D_mod['DUID'] == 'C', 0.0,
    ...                                            BIDPEROFFER_D_mod['BANDAVAIL1'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [2 rows x 9 columns]

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's capacity is changed to less than its enablement min then it gets filtered out.

    >>> capacity_limits_mod = capacity_limits.copy()

    >>> capacity_limits_mod['capacity'] = np.where(capacity_limits_mod['unit'] == 'C', 0.0,
    ...                                            capacity_limits_mod['capacity'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD, capacity_limits_mod)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [2 rows x 9 columns]

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

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D_mod, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [2 rows x 9 columns]

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's INITIALMW is changed to less than its enablement min then it gets filtered out.

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> DISPATCHLOAD_mod['INITIALMW'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 19.0, DISPATCHLOAD_mod['INITIALMW'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [2 rows x 9 columns]

    >>> print(BIDDAYOFFER_D_out)
      DUID   BIDTYPE  PRICEBAND1  PRICEBAND2
    0    A    ENERGY       100.0       110.0
    0    B  RAISEREG        50.0        60.0

    If unit C's AGCSTATUS is changed to  0.0 then it gets filtered out.

    >>> DISPATCHLOAD_mod = DISPATCHLOAD.copy()

    >>> DISPATCHLOAD_mod['AGCSTATUS'] = np.where(DISPATCHLOAD_mod['DUID'] == 'C', 0.0, DISPATCHLOAD_mod['AGCSTATUS'])

    >>> BIDPEROFFER_D_out, BIDDAYOFFER_D_out = enforce_preconditions_for_enabling_fcas(
    ...   BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD_mod, capacity_limits)

    All criteria are meet so no units are filtered out.

    >>> print(BIDPEROFFER_D_out)
      DUID   BIDTYPE  BANDAVAIL1  ...  LOWBREAKPOINT  HIGHBREAKPOINT  ENABLEMENTMAX
    0    A    ENERGY       100.0  ...            0.0             0.0            0.0
    0    B  RAISEREG        50.0  ...           50.0            70.0          100.0
    <BLANKLINE>
    [2 rows x 9 columns]

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


def determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D):
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

    >>> unit_limits = determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

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

    >>> unit_limits = determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

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

    >>> unit_limits = determine_unit_limits(DISPATCHLOAD, BIDPEROFFER_D)

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


def fast_start_mode_two_targets(fast_start_profile):
    units_in_mode_two = \
        fast_start_profile[(fast_start_profile['current_mode'] == 2)]
    units_in_mode_two['fast_start_initial_mw'] = (((units_in_mode_two['time_in_current_mode'])
                                                          / units_in_mode_two['mode_two_length']) *
                                                         units_in_mode_two['min_loading'])
    return units_in_mode_two


def fast_start_calc_end_interval_state(fast_start_profile, dispatch_interval):
    fast_start_profile = fast_start_profile[fast_start_profile['current_mode'] != 0]
    fast_start_profile['time_in_current_mode_at_end'] = fast_start_profile['time_in_current_mode'] + dispatch_interval

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

    fast_start_profile['next_mode'] = np.where(fast_start_profile['time_in_current_mode_at_end'] >
                                                  fast_start_profile['current_mode_length'],
                                                  fast_start_profile['current_mode'] + 1,
                                                  fast_start_profile['current_mode'])

    fast_start_profile['time_in_end_mode'] = np.where(fast_start_profile['next_mode'] != fast_start_profile['current_mode'],
                                                      fast_start_profile['time_in_current_mode_at_end'] -
                                                      fast_start_profile['current_mode_length'],
                                                      fast_start_profile['time_in_current_mode_at_end'])

    fast_start_profile['time_after_mode_two'] = np.where((fast_start_profile['current_mode'] == 2) &
                                                         (fast_start_profile['next_mode'] == 3),
                                                         fast_start_profile['time_in_end_mode'],
                                                         np.NAN)

    for i in range(1, 10):

        fast_start_profile['previous_mode'] = fast_start_profile['next_mode']

        fast_start_profile['current_mode_length'] = fast_start_profile.apply(lambda x: clac_mode_length(x), axis=1)

        fast_start_profile['next_mode'] = np.where(fast_start_profile['time_in_end_mode'] >
                                                      fast_start_profile['current_mode_length'],
                                                      fast_start_profile['previous_mode'] + 1,
                                                      fast_start_profile['previous_mode'])

        fast_start_profile['time_in_end_mode'] = np.where(fast_start_profile['next_mode'] !=
                                                          fast_start_profile['previous_mode'],
                                                          fast_start_profile['time_in_end_mode'] -
                                                          fast_start_profile['current_mode_length'],
                                                          fast_start_profile['time_in_end_mode'])

        fast_start_profile['time_after_mode_two'] = np.where((fast_start_profile['current_mode'] == 2) &
                                                             (fast_start_profile['next_mode'] == 3),
                                                             fast_start_profile['time_in_end_mode'],
                                                             fast_start_profile['time_after_mode_two'])

    return fast_start_profile.loc[:, ['unit', 'min_loading', 'current_mode', 'next_mode', 'time_in_current_mode',
                                      'time_in_end_mode', 'mode_one_length', 'mode_two_length', 'mode_three_length',
                                      'mode_four_length', 'time_after_mode_two']]


def fast_start_adjusted_ramp_up_rate(ramp_rates, fast_start_end_condition, dispatch_interval):
    fast_start_end_condition = fast_start_end_condition[(fast_start_end_condition['current_mode'] == 2) |
                                                        (fast_start_end_condition['next_mode'] > 2)]
    fast_start_end_condition = pd.merge(ramp_rates, fast_start_end_condition, left_on='DUID', right_on='unit')
    fast_start_end_condition['ramp_max'] = fast_start_end_condition['RAMPUPRATE'] / 12.0
    fast_start_end_condition['ramp_max'] = (fast_start_end_condition['time_after_mode_two'] / dispatch_interval) * \
                                           fast_start_end_condition['ramp_max'] + fast_start_end_condition[
                                               'min_loading']
    fast_start_end_condition['new_ramp_up_rate'] = (fast_start_end_condition['ramp_max'] -
                                                    fast_start_end_condition['fast_start_initial_mw']) * 12
    return fast_start_end_condition.loc[:, ['unit', 'fast_start_initial_mw', 'new_ramp_up_rate']]
