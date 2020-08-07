import xmltodict
import pandas as pd
import requests
import zipfile
import io
import os
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, time


class XMLInputs:
    def __init__(self, cache_folder, interval):
        self.xml = None
        self.interval = interval
        self.cache_folder = cache_folder
        Path(cache_folder).mkdir(parents=False, exist_ok=True)
        if not self.interval_inputs_in_cache():
            self.download_xml_from_nemweb()
            if not self.interval_inputs_in_cache():
                raise ValueError('File not downloaded.')
        self.load_xml()

    def interval_inputs_in_cache(self):
        return os.path.exists(self.get_file_path())

    def get_file_path(self):
        return Path(self.cache_folder) / self.get_file_name()

    def get_file_name(self):
        year, month, day = self.get_market_year_month_day_as_str()
        interval_number = self.get_interval_number_as_str()
        base_name = "NEMSPDOutputs_{year}{month}{day}{interval_number}00.loaded"
        name = base_name.format(year=year, month=month, day=day, interval_number=interval_number)
        path_name = Path(self.cache_folder) / name
        if os.path.exists(path_name):
            return name
        else:
            return name.replace('.loaded', '_OCD.loaded')

    def download_xml_from_nemweb(self):
        year, month, day = self.get_market_year_month_day_as_str()
        base_url = "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/{year}/NEMDE_{year}_{month}/NEMDE_Market_Data/NEMDE_Files/NemSpdOutputs_{year}{month}{day}_loaded.zip"
        url = base_url.format(year=year, month=month, day=day)
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(self.cache_folder)

    def get_market_year_month_day(self):
        date_time = self.get_interval_datetime_object()
        hour = date_time.hour
        minute = date_time.minute
        start_market_day = time(hour=4, minute=5)
        interval_time = time(hour=hour, minute=minute)
        if interval_time < start_market_day:
            date_time_for_date = date_time - timedelta(days=1)
        else:
            date_time_for_date = date_time
        year = date_time_for_date.year
        month = date_time_for_date.month
        day = date_time_for_date.day
        return year, month, day

    def get_interval_number_as_str(self):
        return str(self.get_interval_number()).zfill(3)

    def get_market_year_month_day_as_str(self):
        year, month, day = self.get_market_year_month_day()
        year_str = str(year)
        month_str = str(month).zfill(2)
        day_str = str(day).zfill(2)
        return year_str, month_str, day_str

    def get_interval_number(self):
        year, month, day = self.get_market_year_month_day()
        start_market_day_datetime = datetime(year=year, month=month, day=day, hour=4, minute=5)
        time_since_market_day_started = self.get_interval_datetime_object() - start_market_day_datetime
        intervals_elapsed = time_since_market_day_started / timedelta(minutes=5)
        interval_number = int(intervals_elapsed) + 1
        return interval_number

    def get_interval_datetime_object(self):
        return datetime.strptime(self.interval, '%Y/%m/%d %H:%M:%S')

    def load_xml(self):

        # try:
        #
        #     with open(self.get_file_path()) as file:
        #         inputs = xmltodict.parse(file.read())
        #         self.xml = inputs
        #
        # except:
        #
        #     print("Bad {}".format(self.interval))

        x=1

    def get_unit_initial_conditions_dataframe(self):
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['TraderCollection']['Trader']
        initial_conditions = dict(DUID=[], INITIALMW=[], RAMPUPRATE=[], RAMPDOWNRATE=[], AGCSTATUS=[])
        if self.is_intervention_period():
            INITIALMW_name = 'WhatIfInitialMW'
        else:
            INITIALMW_name = 'InitialMW'
        name_map = dict(INITIALMW=INITIALMW_name, RAMPUPRATE='SCADARampUpRate', RAMPDOWNRATE='SCADARampDnRate',
                        AGCSTATUS='AGCStatus')
        for trader in traders:
            initial_conditions['DUID'].append(trader['@TraderID'])
            initial_cons = trader['TraderInitialConditionCollection']['TraderInitialCondition']
            for our_name, aemo_name in name_map.items():
                for con in initial_cons:
                    if con['@InitialConditionID'] == aemo_name:
                        value = float(con['@Value'])
                        break
                    else:
                        value = np.NAN
                initial_conditions[our_name].append(value)
        initial_conditions = pd.DataFrame(initial_conditions)
        return initial_conditions

    def get_unit_fast_start_parameters(self):
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['TraderCollection']['Trader']
        initial_conditions = dict(DUID=[], MinLoadingMW=[], CurrentMode=[], CurrentModeTime=[], T1=[], T2=[],
                                  T3=[], T4=[], SEMIDISPATCH=[])
        cols = dict(MinLoadingMW='@MinLoadingMW', CurrentMode='@CurrentMode', SEMIDISPATCH='@SemiDispatch',
                    CurrentModeTime='@CurrentModeTime', T1='@T1', T2='@T2', T3='@T3', T4='@T4')
        for trader in traders:
            row = False
            if '@CurrentMode' in trader:
                for key, name in cols.items():
                    row = True
                    value = int(trader[name])
                    if name == '@CurrentMode' and '@WhatIfCurrentMode' in trader:
                        value = int(trader['@WhatIfCurrentMode'])
                    if name == '@CurrentModeTime' and '@WhatIfCurrentModeTime' in trader:
                        value = int(trader['@WhatIfCurrentModeTime'])
                    initial_conditions[key].append(value)
            if row:
                initial_conditions['DUID'].append(trader['@TraderID'])

        initial_conditions = pd.DataFrame(initial_conditions)
        return initial_conditions

    def get_unit_volume_bids(self):
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['TraderPeriodCollection']['TraderPeriod']
        trades_by_unit_and_type = dict(DUID=[], BIDTYPE=[], MAXAVAIL=[], ENABLEMENTMIN=[], ENABLEMENTMAX=[],
                                       LOWBREAKPOINT=[], HIGHBREAKPOINT=[], BANDAVAIL1=[], BANDAVAIL2=[],
                                       BANDAVAIL3=[], BANDAVAIL4=[], BANDAVAIL5=[], BANDAVAIL6=[], BANDAVAIL7=[],
                                       BANDAVAIL8=[], BANDAVAIL9=[], BANDAVAIL10=[], RAMPDOWNRATE=[], RAMPUPRATE=[])
        name_map = dict(BIDTYPE='@TradeType', MAXAVAIL='@MaxAvail', ENABLEMENTMIN='@EnablementMin',
                        ENABLEMENTMAX='@EnablementMax',  LOWBREAKPOINT='@LowBreakpoint',
                        HIGHBREAKPOINT='@HighBreakpoint', BANDAVAIL1='@BandAvail1', BANDAVAIL2='@BandAvail2',
                        BANDAVAIL3='@BandAvail3', BANDAVAIL4='@BandAvail4', BANDAVAIL5='@BandAvail5',
                        BANDAVAIL6='@BandAvail6', BANDAVAIL7='@BandAvail7', BANDAVAIL8='@BandAvail8',
                        BANDAVAIL9='@BandAvail9', BANDAVAIL10='@BandAvail10', RAMPDOWNRATE='@RampDnRate',
                        RAMPUPRATE='@RampUpRate')
        for trader in traders:
            if type(trader['TradeCollection']['Trade']) != list:
                trades = trader['TradeCollection']
                for _, trade in trades.items():
                    trades_by_unit_and_type['DUID'].append(trader['@TraderID'])
                    for our_name, aemo_name in name_map.items():
                        if aemo_name in trade:
                            if aemo_name == '@TradeType':
                                value = trade[aemo_name]
                            else:
                                value = float(trade[aemo_name])
                        else:
                            value = 0.0
                        trades_by_unit_and_type[our_name].append(value)
            else:
                for trade in trader['TradeCollection']['Trade']:
                    trades_by_unit_and_type['DUID'].append(trader['@TraderID'])
                    for our_name, aemo_name in name_map.items():
                        if aemo_name in trade:
                            if aemo_name == '@TradeType':
                                value = trade[aemo_name]
                            else:
                                value = float(trade[aemo_name])
                        else:
                            value = 0.0
                        trades_by_unit_and_type[our_name].append(value)
        trades_by_unit_and_type = pd.DataFrame(trades_by_unit_and_type)
        bid_type_map = dict(ENOF='ENERGY', LDOF='ENERGY', L5RE='LOWERREG', R5RE='RAISEREG', R5MI='RAISE5MIN',
                            L5MI='LOWER5MIN', R60S='RAISE60SEC', L60S='LOWER60SEC', R6SE='RAISE6SEC', L6SE='LOWER6SEC')
        trades_by_unit_and_type["BIDTYPE"] = trades_by_unit_and_type["BIDTYPE"].apply(lambda x: bid_type_map[x])
        return trades_by_unit_and_type

    def get_UGIF_values(self):
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['TraderPeriodCollection']['TraderPeriod']
        trades_by_unit_and_type = dict(DUID=[], UGIF=[])
        for trader in traders:
            if '@UIGF' in trader:
                trades_by_unit_and_type['DUID'].append(trader['@TraderID'])
                trades_by_unit_and_type['UGIF'].append(float(trader['@UIGF']))
        trades_by_unit_and_type = pd.DataFrame(trades_by_unit_and_type)
        return trades_by_unit_and_type

    def get_non_intervention_violations(self):
        outputs = self.xml['NEMSPDCaseFile']['NemSpdOutputs']
        name_map = dict(regional_demand='@TotalAreaGenViolation',
                        interocnnector='@TotalInterconnectorViolation',
                        generic_constraint='@TotalGenericViolation',
                        ramp_rate='@TotalRampRateViolation',
                        unit_capacity='@TotalUnitMWCapacityViolation',
                        energy_constraint='@TotalEnergyConstrViolation',
                        energy_offer='@TotalEnergyOfferViolation',
                        fcas_profile='@TotalASProfileViolation',
                        fast_start='@TotalFastStartViolation',
                        mnsp_ramp_rate='@TotalMNSPRampRateViolation',
                        msnp_offer='@TotalMNSPOfferViolation',
                        mnsp_capacity='@TotalMNSPCapacityViolation',
                        ugif='@TotalUIGFViolation')
        violations = {}
        if type(outputs['PeriodSolution']) == list:
            for solution in outputs['PeriodSolution']:
                if solution['@Intervention'] == '0':
                    for name, aemo_name in name_map.items():
                        violations[name] = float(solution[aemo_name])
        else:
            for name, aemo_name in name_map.items():
                violations[name] = float(outputs['PeriodSolution'][aemo_name])
        return violations

    def get_constraint_violation_prices(self):
        inputs = self.xml['NEMSPDCaseFile']['NemSpdInputs']
        name_map = dict(regional_demand='@EnergyDeficitPrice',
                        interocnnector='@InterconnectorPrice',
                        generic_constraint='@GenericConstraintPrice',
                        ramp_rate='@RampRatePrice',
                        unit_capacity='@CapacityPrice',
                        energy_offer='@OfferPrice',
                        fcas_profile='@ASProfilePrice',
                        fcas_max_avail='@ASMaxAvailPrice',
                        fcas_enablement_min='@ASEnablementMinPrice',
                        fcas_enablement_max='@ASEnablementMaxPrice',
                        fast_start='@FastStartPrice',
                        mnsp_ramp_rate='@MNSPRampRatePrice',
                        msnp_offer='@MNSPOfferPrice',
                        mnsp_capacity='@MNSPCapacityPrice',
                        ugif='@UIGFSurplusPrice',
                        voll='@VoLL')
        violations = {}
        for name, aemo_name in name_map.items():
            violations[name] = float(inputs['Case'][aemo_name])
        return violations

    def is_intervention_period(self):
        return type(self.xml['NEMSPDCaseFile']['NemSpdOutputs']['PeriodSolution']) == list

    def get_constraint_rhs(self):
        """

        Examples
        --------

        >>> xml = XMLInputs('../../tests/test_files/historical_xml_files', '2019/01/27 13:45:00')

        >>> xml.get_constraint_rhs()
                         set           rhs
        0               #BANN1_E     32.000000
        1              #BNGSF2_E      3.000000
        2            #CHILDSF1_E      0.000000
        3            #CROWLWF1_E     48.000000
        4             #CSPVPS1_E     29.000000
        ..                   ...           ...
        736          V_OWF_NRB_0  10000.001000
        737  V_OWF_TGTSNRBHTN_30  10030.000000
        738        V_S_NIL_ROCOF   1203.600037
        739          V_T_NIL_BL1    125.000000
        740        V_T_NIL_FCSPS  19985.000000
        <BLANKLINE>
        [741 rows x 2 columns]

        Returns
        -------

        """
        constraints = self.xml['NEMSPDCaseFile']['NemSpdOutputs']['ConstraintSolution']
        rhs_values = dict(set=[], rhs=[])
        for con in constraints:
            if con['@Intervention'] == '0':
                rhs_values['set'].append(con['@ConstraintID'])
                rhs_values['rhs'].append(float(con['@RHS']))
        return pd.DataFrame(rhs_values)

    def get_constraint_type(self):
        """

        Examples
        --------

        >>> xml = XMLInputs('../../tests/test_files/historical_xml_files', '2019/01/27 13:45:00')

        >>> xml.get_constraint_type()
                             set type
        0               #BANN1_E   LE
        1              #BNGSF2_E   LE
        2            #CHILDSF1_E   LE
        3            #CROWLWF1_E   LE
        4             #CSPVPS1_E   LE
        ..                   ...  ...
        736          V_OWF_NRB_0   LE
        737  V_OWF_TGTSNRBHTN_30   LE
        738        V_S_NIL_ROCOF   LE
        739          V_T_NIL_BL1   LE
        740        V_T_NIL_FCSPS   LE
        <BLANKLINE>
        [741 rows x 2 columns]

        Returns
        -------

        """
        constraints = self.xml['NEMSPDCaseFile']['NemSpdInputs']['GenericConstraintCollection']['GenericConstraint']
        rhs_values = dict(set=[], type=[], cost=[])
        for con in constraints:
            rhs_values['set'].append(con['@ConstraintID'])
            rhs_values['type'].append(con['@Type'])
            rhs_values['cost'].append(float(con['@ViolationPrice']))
        return pd.DataFrame(rhs_values)

    def get_constraint_region_lhs(self):
        """

        Examples
        --------

        >>> xml = XMLInputs('../../tests/test_files/historical_xml_files', '2019/01/27 13:45:00')

        >>> xml.get_constraint_region_lhs()
                           set region service  coefficient
        0        F_I+LREG_0120   NSW1    L5RE          1.0
        1        F_I+LREG_0120   QLD1    L5RE          1.0
        2        F_I+LREG_0120    SA1    L5RE          1.0
        3        F_I+LREG_0120   TAS1    L5RE          1.0
        4        F_I+LREG_0120   VIC1    L5RE          1.0
        ..                 ...    ...     ...          ...
        478   F_T+NIL_WF_TG_R5   TAS1    R5RE          1.0
        479   F_T+NIL_WF_TG_R6   TAS1    R6SE          1.0
        480  F_T+NIL_WF_TG_R60   TAS1    R60S          1.0
        481      F_T+RREG_0050   TAS1    R5RE          1.0
        482    F_T_NIL_MINP_R6   TAS1    R6SE          1.0
        <BLANKLINE>
        [483 rows x 4 columns]

        Returns
        -------

        """
        constraints = self.xml['NEMSPDCaseFile']['NemSpdInputs']['GenericConstraintCollection']['GenericConstraint']
        lhs_values = dict(set=[], region=[], service=[], coefficient=[])
        for con in constraints:
            lhs = con['LHSFactorCollection']
            if lhs is not None and 'RegionFactor' in lhs:
                if type(lhs['RegionFactor']) == list:
                    for term in lhs['RegionFactor']:
                        lhs_values['set'].append(con['@ConstraintID'])
                        lhs_values['region'].append(term['@RegionID'])
                        lhs_values['service'].append(term['@TradeType'])
                        lhs_values['coefficient'].append(float(term['@Factor']))
                else:
                    term = lhs['RegionFactor']
                    lhs_values['set'].append(con['@ConstraintID'])
                    lhs_values['region'].append(term['@RegionID'])
                    lhs_values['service'].append(term['@TradeType'])
                    lhs_values['coefficient'].append(float(term['@Factor']))
        return pd.DataFrame(lhs_values)

    def get_constraint_unit_lhs(self):
        """

        Examples
        --------

        >>> xml = XMLInputs('../../tests/test_files/historical_xml_files', '2019/01/27 13:45:00')

        >>> xml.get_constraint_unit_lhs()
                              set      unit service  coefficient
        0                #BANN1_E     BANN1    ENOF          1.0
        1               #BNGSF2_E    BNGSF2    ENOF          1.0
        2             #CHILDSF1_E  CHILDSF1    ENOF          1.0
        3             #CROWLWF1_E  CROWLWF1    ENOF          1.0
        4              #CSPVPS1_E   CSPVPS1    ENOF          1.0
        ...                   ...       ...     ...          ...
        6013    V_GANWR_SF_BAT_50   GANNSF1    ENOF          1.0
        6014      V_MTGBRAND_33WT  MTGELWF1    ENOF          1.0
        6015     V_OAKHILL_TFB_42  OAKLAND1    ENOF          1.0
        6016          V_OWF_NRB_0  OAKLAND1    ENOF          1.0
        6017  V_OWF_TGTSNRBHTN_30  OAKLAND1    ENOF          1.0
        <BLANKLINE>
        [6018 rows x 4 columns]

        Returns
        -------

        """
        constraints = self.xml['NEMSPDCaseFile']['NemSpdInputs']['GenericConstraintCollection']['GenericConstraint']
        lhs_values = dict(set=[], unit=[], service=[], coefficient=[])
        for con in constraints:
            lhs = con['LHSFactorCollection']
            if lhs is not None and 'TraderFactor' in lhs:
                if type(lhs['TraderFactor']) == list:
                    for term in lhs['TraderFactor']:
                        lhs_values['set'].append(con['@ConstraintID'])
                        lhs_values['unit'].append(term['@TraderID'])
                        lhs_values['service'].append(term['@TradeType'])
                        lhs_values['coefficient'].append(float(term['@Factor']))
                else:
                    term = lhs['TraderFactor']
                    lhs_values['set'].append(con['@ConstraintID'])
                    lhs_values['unit'].append(term['@TraderID'])
                    lhs_values['service'].append(term['@TradeType'])
                    lhs_values['coefficient'].append(float(term['@Factor']))
        return pd.DataFrame(lhs_values)

    def get_constraint_interconnector_lhs(self):
        """

        Examples
        --------

        >>> xml = XMLInputs('../../tests/test_files/historical_xml_files', '2019/01/27 13:45:00')

        >>> xml.get_constraint_interconnector_lhs()
                             set interconnector  coefficient
        0               DATASNAP      N-Q-MNSP1          1.0
        1        DATASNAP_DFS_LS      N-Q-MNSP1          1.0
        2      DATASNAP_DFS_NCAN      N-Q-MNSP1          1.0
        3    DATASNAP_DFS_NCWEST      N-Q-MNSP1          1.0
        4      DATASNAP_DFS_NNTH      N-Q-MNSP1          1.0
        ..                   ...            ...          ...
        619      V^^S_NIL_TBSE_1           V-SA          1.0
        620      V^^S_NIL_TBSE_2           V-SA          1.0
        621        V_S_NIL_ROCOF           V-SA          1.0
        622          V_T_NIL_BL1      T-V-MNSP1         -1.0
        623        V_T_NIL_FCSPS      T-V-MNSP1         -1.0
        <BLANKLINE>
        [624 rows x 3 columns]

        Returns
        -------

        """
        constraints = self.xml['NEMSPDCaseFile']['NemSpdInputs']['GenericConstraintCollection']['GenericConstraint']
        lhs_values = dict(set=[], interconnector=[], coefficient=[])
        for con in constraints:
            lhs = con['LHSFactorCollection']
            if lhs is not None and 'InterconnectorFactor' in lhs:
                if type(lhs['InterconnectorFactor']) == list:
                    for term in lhs['InterconnectorFactor']:
                        lhs_values['set'].append(con['@ConstraintID'])
                        lhs_values['interconnector'].append(term['@InterconnectorID'])
                        lhs_values['coefficient'].append(float(term['@Factor']))
                else:
                    term = lhs['InterconnectorFactor']
                    lhs_values['set'].append(con['@ConstraintID'])
                    lhs_values['interconnector'].append(term['@InterconnectorID'])
                    lhs_values['coefficient'].append(float(term['@Factor']))
        return pd.DataFrame(lhs_values)

    def get_bass_link_bid_availability(self):
        """

        Examples
        --------

        >>> xml = XMLInputs('../../tests/test_files/historical_xml_files', '2019/01/27 13:45:00')

        >>> xml.get_bass_link_bid_availability()

        Returns
        -------

        """
        bass_link_bids = self.xml['NEMSPDCaseFile']['NemSpdInputs']['InterconnectorPeriodCollection']['']




