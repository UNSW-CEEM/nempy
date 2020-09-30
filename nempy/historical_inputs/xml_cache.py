import xmltodict
import pandas as pd
import requests
import zipfile
import io
import os
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, time
from time import time as t

pd.set_option('display.width', None)


class XMLCacheManager:
    """Class for accessing data stored in AEMO's NEMDE output files.

    Examples
    --------

    A XMLCacheManager instance is created by providing the path to directory containing the cache of XML files.

    >>> manager = XMLCacheManager('test_nemde_cache')

    Parameters
    ----------
    cache_folder : str
    """
    def __init__(self, cache_folder):
        self.cache_folder = cache_folder
        self.interval = None
        self.xml = None
        Path(cache_folder).mkdir(parents=False, exist_ok=True)

    def populate(self, start_year, start_month, end_year, end_month, verbose=True):
        start = datetime(year=start_year, month=start_month, day=1) - timedelta(days=1)
        if end_month == 12:
            end_month = 0
            end_year += 1
        end = datetime(year=end_year, month=end_month + 1, day=1)
        download_date = start
        while download_date <= end:
            if verbose:
                print('Downloading NEMDE XML file for year={}, month={}, day={}'.format(download_date.year,
                                                                                        download_date.month,
                                                                                        download_date.day))
            download_date_str = download_date.isoformat().replace('T', ' ').replace('-', '/')
            self.load_interval(download_date_str)
            download_date += timedelta(days=1)

    def load_interval(self, interval):
        """Load the data for particular 5 min dispatch interval into memory.

        If the file intervals data is not on disk then an attempt to download it from AEMO's NEMweb portal is made.

        Examples
        --------

        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        Parameters
        ----------
        interval : str
            In the format '%Y/%m/%d %H:%M:%S'

        Raises
        ------
        MissingDataError
            If the data for an interval is not in the cache and cannot be downloaded from NEMWeb.
        """
        self.interval = interval
        if not self.interval_inputs_in_cache():
            self._download_xml_from_nemweb()
            if not self.interval_inputs_in_cache():
                raise MissingDataError('File not downloaded, check internet connection and that NEMWeb contains data for interval {}.'.format(self.interval))
        with open(self.get_file_path()) as file:
            read = file.read()
            self.xml = xmltodict.parse(read)

    def interval_inputs_in_cache(self):
        """Check if the cache contains the data for the loaded interval, primarily for debugging.

        Examples
        --------

        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.interval_inputs_in_cache()
        True

        Returns
        -------
        bool
        """
        return os.path.exists(self.get_file_path())

    def get_file_path(self):
        """Get the file path to the currently loaded interval.

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_file_path()
        PosixPath('test_nemde_cache/NEMSPDOutputs_2018123124000.loaded')
        """
        return Path(self.cache_folder) / self.get_file_name()

    def get_file_name(self):
        """Get the filename of the currently loaded interval.

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_file_name()
        'NEMSPDOutputs_2018123124000.loaded'
        """
        year, month, day = self._get_market_year_month_day_as_str()
        interval_number = self._get_interval_number_as_str()
        base_name = "NEMSPDOutputs_{year}{month}{day}{interval_number}00.loaded"
        name = base_name.format(year=year, month=month, day=day, interval_number=interval_number)
        path_name = Path(self.cache_folder) / name
        name_OCD = name.replace('.loaded', '_OCD.loaded')
        path_name_OCD = Path(self.cache_folder) / name_OCD
        if os.path.exists(path_name):
            return name
        elif path_name_OCD:
            return name_OCD
        else:
            return name

    def _download_xml_from_nemweb(self):
        year, month, day = self._get_market_year_month_day_as_str()
        base_url = "https://www.nemweb.com.au/Data_Archive/Wholesale_Electricity/NEMDE/{year}/NEMDE_{year}_{month}/NEMDE_Market_Data/NEMDE_Files/NemSpdOutputs_{year}{month}{day}_loaded.zip"
        url = base_url.format(year=year, month=month, day=day)
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(self.cache_folder)

    def _get_market_year_month_day(self):
        date_time = self._get_interval_datetime_object()
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

    def _get_interval_number_as_str(self):
        return str(self._get_interval_number()).zfill(3)

    def _get_market_year_month_day_as_str(self):
        year, month, day = self._get_market_year_month_day()
        year_str = str(year)
        month_str = str(month).zfill(2)
        day_str = str(day).zfill(2)
        return year_str, month_str, day_str

    def _get_interval_number(self):
        year, month, day = self._get_market_year_month_day()
        start_market_day_datetime = datetime(year=year, month=month, day=day, hour=4, minute=5)
        time_since_market_day_started = self._get_interval_datetime_object() - start_market_day_datetime
        intervals_elapsed = time_since_market_day_started / timedelta(minutes=5)
        interval_number = int(intervals_elapsed) + 1
        return interval_number

    def _get_interval_datetime_object(self):
        return datetime.strptime(self.interval, '%Y/%m/%d %H:%M:%S')

    def get_unit_initial_conditions(self):
        """Get the initial conditions of units at the start of the dispatch interval.

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_unit_initial_conditions()
                 DUID   INITIALMW  RAMPUPRATE  RAMPDOWNRATE  AGCSTATUS
        0      AGLHAL    0.000000         NaN           NaN        0.0
        1      AGLSOM    0.000000         NaN           NaN        0.0
        2     ANGAST1    0.000000         NaN           NaN        0.0
        3       APD01    0.000000         NaN           NaN        0.0
        4       ARWF1   54.500000         NaN           NaN        0.0
        ..        ...         ...         ...           ...        ...
        283  YARWUN_1  140.360001         NaN           NaN        0.0
        284     YWPS1  366.665833  177.750006    177.750006        1.0
        285     YWPS2  374.686066  190.125003    190.125003        1.0
        286     YWPS3    0.000000  300.374994    300.374994        0.0
        287     YWPS4  368.139252  182.249994    182.249994        1.0
        <BLANKLINE>
        [288 rows x 5 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            INITIALMW         the output or consumption of the unit \n
                              at the start of the interval, in MW, \n
                              (as `np.int64`),
            RAMPUPRATE        ramp up rate of unit as repoted by the \n
                              scada system at the start if the \n
                              interval, in MW/h, (as `np.int64`)
            RAMPDOWNRATE      ramp down rate of unit as repoted by the \n
                              scada system at the start if the \n
                              interval, in MW/h, (as `np.int64`)
            AGCSTATUS         flag to indicate whether the unit is \n
                              connected to the AGC system at the \n
                              start of the interval, 0.0 if not and \n
                              1.0 if it is, (as `np.int64`)
            ================  ========================================

        """
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
        """Get the unit fast start dispatch inflexibility parameter values.

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_unit_fast_start_parameters()
                DUID  MinLoadingMW  CurrentMode  CurrentModeTime  T1  T2  T3  T4
        0     AGLHAL             2            0                0  10   3  10   2
        1     AGLSOM            16            0                0  20   2  35   2
        2   BARCALDN            12            0                0  14   4   1   4
        3   BARRON-1             5            4                1  11   3   1   1
        4   BARRON-2             5            4                1  11   3   1   1
        ..       ...           ...          ...              ...  ..  ..  ..  ..
        69     VPGS5            48            0                0   5   3  15   0
        70     VPGS6            48            0                0   5   3  15   0
        71   W/HOE#1           160            0                0   3   0   0   0
        72   W/HOE#2           160            0                0   3   0   0   0
        73    YABULU            83            0                0   5   6  42   6
        <BLANKLINE>
        [74 rows x 8 columns]

        Returns
        --------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            MinLoadingMW      :download:`see AEMO doc <../../docs/pdfs/Fast_Start_Unit_Inflexibility_Profile_Model_October_2014.pdf>`, \n
                              in MW, (as `np.int64`)
            CurrentMode       The dispatch mode if the unit at the \n
                              start of the interval, for mode \n
                              definitions :download:`see AEMO doc <../../docs/pdfs/Fast_Start_Unit_Inflexibility_Profile_Model_October_2014.pdf>`,\n
                              (as `np.int64`)
            CurrentModeTime   The time already spent in the current \n
                              mode, in minutes, (as `np.int64`)
            T1                The total length of mode 1, in minutes \n
                              (as `np.int64`)
            T2                The total length of mode 2, in minutes \n
                              (as `np.int64`)
            T3                The total length of mode 1, in minutes, \n
                              (as `np.int64`)
            T4                The total length of mode 4, in minutes, \n
                              (as `np.int64`)
            ================  ========================================


        """
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['TraderCollection']['Trader']
        initial_conditions = dict(DUID=[], MinLoadingMW=[], CurrentMode=[], CurrentModeTime=[], T1=[], T2=[],
                                  T3=[], T4=[])
        cols = dict(MinLoadingMW='@MinLoadingMW', CurrentMode='@CurrentMode',
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
        """Get the unit volume bids

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_unit_volume_bids()
                 DUID     BIDTYPE  MAXAVAIL  ENABLEMENTMIN  ENABLEMENTMAX  LOWBREAKPOINT  HIGHBREAKPOINT  BANDAVAIL1  BANDAVAIL2  BANDAVAIL3  BANDAVAIL4  BANDAVAIL5  BANDAVAIL6  BANDAVAIL7  BANDAVAIL8  BANDAVAIL9  BANDAVAIL10  RAMPDOWNRATE  RAMPUPRATE
        0      AGLHAL      ENERGY     173.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0        60.0         0.0         0.0        160.0         720.0       720.0
        1      AGLSOM      ENERGY     160.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0        170.0         480.0       480.0
        2     ANGAST1      ENERGY      43.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0        50.0         0.0         0.0         0.0         50.0         840.0       840.0
        3       APD01   LOWER5MIN       0.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0        300.0           0.0         0.0
        4       APD01  LOWER60SEC       0.0            0.0            0.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0        300.0           0.0         0.0
        ...       ...         ...       ...            ...            ...            ...             ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...           ...         ...
        1021    YWPS4   LOWER6SEC      25.0          250.0          385.0          275.0           385.0        15.0        10.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0          0.0           0.0         0.0
        1022    YWPS4   RAISE5MIN       0.0          250.0          390.0          250.0           380.0         0.0         0.0         0.0         0.0         5.0         0.0         0.0         5.0         0.0         10.0           0.0         0.0
        1023    YWPS4    RAISEREG      15.0          250.0          385.0          250.0           370.0         0.0         0.0         0.0         0.0         0.0         0.0         5.0        10.0         0.0          5.0           0.0         0.0
        1024    YWPS4  RAISE60SEC      10.0          220.0          400.0          220.0           390.0         0.0         0.0         0.0         0.0         0.0         5.0         5.0         0.0         0.0         10.0           0.0         0.0
        1025    YWPS4   RAISE6SEC      15.0          220.0          405.0          220.0           390.0         0.0         0.0         0.0        10.0         5.0         0.0         0.0         0.0         0.0         10.0           0.0         0.0
        <BLANKLINE>
        [1026 rows x 19 columns]

        Returns
        --------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            BIDTYPE           the service the bid applies to, \n
                              (as `str`)
            MAXAVAIL          the bid in unit availablity, in MW,
                              (as `str`)
            ENABLEMENTMIN     :download:`see AMEO docs  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`, \n
                              in MW, (as `np.float64`)
            ENABLEMENTMAX     :download:`see AMEO docs  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`, \n
                              in MW, (as `np.float64`)
            LOWBREAKPOINT     :download:`see AMEO docs <../../docs/pdfs/FCAS Model in NEMDE.pdf>`, \n
                              in MW, (as `np.float64`)
            HIGHBREAKPOINT    :download:`see AMEO docs  <../../docs/pdfs/FCAS Model in NEMDE.pdf>`, \n
                              in MW, (as `np.float64`)
            BANDAVAIL1        the volume bid in the first bid band,
                              in MW, (as `np.float64`)
                 :
            BANDAVAIL10       the volume bid in the tenth bid band,
                              in MW, (as `np.float64`)
            RAMPDOWNRATE      the bid in ramp down rate, in MW/h,
                              (as `np.int64`)
            RAMPUPRATE        the bid in ramp up rate, in MW/h,
                              (as `np.int64`)
            ================  ========================================


        """
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

    def get_UIGF_values(self):
        """Get the unit unconstrained intermittent generation forecast.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_UIGF_values()
                DUID    UIGF
        0      ARWF1  56.755
        1   BALDHWF1   9.160
        2      BANN1   0.000
        3     BLUFF1   4.833
        4     BNGSF1   0.000
        ..       ...     ...
        57     WGWF1  25.445
        58   WHITSF1   0.000
        59  WOODLWN1   0.075
        60     WRSF1   0.000
        61     WRWF1  15.760
        <BLANKLINE>
        [62 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            UGIF              the units generation forecast for end \n
                              of the inteval, in MW, (as `np.float64`)
            ================  ========================================


        """
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['TraderPeriodCollection']['TraderPeriod']
        trades_by_unit_and_type = dict(DUID=[], UIGF=[])
        for trader in traders:
            if '@UIGF' in trader:
                trades_by_unit_and_type['DUID'].append(trader['@TraderID'])
                trades_by_unit_and_type['UIGF'].append(float(trader['@UIGF']))
        trades_by_unit_and_type = pd.DataFrame(trades_by_unit_and_type)
        return trades_by_unit_and_type

    def get_violations(self):
        """Get the total volume violation of different constraint sets.

        For more information on the constraint sets :download:`see AMEO docs  <../../docs/pdfs/Schedule of Constraint Violation Penalty factors.pdf>`

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_violations()
        {'regional_demand': 0.0, 'interocnnector': 0.0, 'generic_constraint': 0.0, 'ramp_rate': 0.0, 'unit_capacity': 0.36, 'energy_constraint': 0.0, 'energy_offer': 0.0, 'fcas_profile': 0.0, 'fast_start': 0.0, 'mnsp_ramp_rate': 0.0, 'msnp_offer': 0.0, 'mnsp_capacity': 0.0, 'ugif': 0.0}

        Returns
        -------
        dict

        """
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
        """Get the price of violating different constraint sets.

        For more information on the constraint sets :download:`see AMEO docs  <../../docs/pdfs/Schedule of Constraint Violation Penalty factors.pdf>`

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_constraint_violation_prices()
        {'regional_demand': 2175000.0, 'interocnnector': 16675000.0, 'generic_constraint': 435000.0, 'ramp_rate': 16747500.0, 'unit_capacity': 5365000.0, 'energy_offer': 16457500.0, 'fcas_profile': 2247500.0, 'fcas_max_avail': 2247500.0, 'fcas_enablement_min': 1015000.0, 'fcas_enablement_max': 1015000.0, 'fast_start': 16385000.0, 'mnsp_ramp_rate': 16747500.0, 'msnp_offer': 16457500.0, 'mnsp_capacity': 5292500.0, 'uigf': 5582500.0, 'voll': 14500.0}

        Returns
        -------
        dict
        """
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
                        uigf='@UIGFSurplusPrice',
                        voll='@VoLL')
        violations = {}
        for name, aemo_name in name_map.items():
            violations[name] = float(inputs['Case'][aemo_name])
        return violations

    def is_intervention_period(self):
        """Check if the interval currently loaded was subject to an intervention.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.is_intervention_period()
        False

        Returns
        -------
        bool

        """
        return type(self.xml['NEMSPDCaseFile']['NemSpdOutputs']['PeriodSolution']) == list

    def get_constraint_rhs(self):
        """Get generic constraints rhs values.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_constraint_rhs()
                             set           rhs
        0               #BANN1_E     32.000000
        1              #BNGSF2_E      3.000000
        2            #CROWLWF1_E     43.000000
        3             #CSPVPS1_E     29.000000
        4             #DAYDSF1_E      0.000000
        ..                   ...           ...
        704          V_OWF_NRB_0  10000.001000
        705  V_OWF_TGTSNRBHTN_30  10030.000000
        706        V_S_NIL_ROCOF    812.280029
        707          V_T_NIL_BL1    478.000000
        708        V_T_NIL_FCSPS    425.154024
        <BLANKLINE>
        [709 rows x 2 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            set               the unique identifier of the generic \n
                              constraint, (as `str`)
            rhs               the rhs value of the constraint, \n
                              (as `np.float64`)
            ================  ========================================

        """
        constraints = self.xml['NEMSPDCaseFile']['NemSpdOutputs']['ConstraintSolution']
        rhs_values = dict(set=[], rhs=[])
        for con in constraints:
            if con['@Intervention'] == '0':
                rhs_values['set'].append(con['@ConstraintID'])
                rhs_values['rhs'].append(float(con['@RHS']))
        return pd.DataFrame(rhs_values)

    def get_constraint_type(self):
        """Get generic constraints type.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_constraint_type()
                             set type       cost
        0               #BANN1_E   LE  5220000.0
        1              #BNGSF2_E   LE  5220000.0
        2            #CROWLWF1_E   LE  5220000.0
        3             #CSPVPS1_E   LE  5220000.0
        4             #DAYDSF1_E   LE  5220000.0
        ..                   ...  ...        ...
        704          V_OWF_NRB_0   LE  5220000.0
        705  V_OWF_TGTSNRBHTN_30   LE  5220000.0
        706        V_S_NIL_ROCOF   LE   507500.0
        707          V_T_NIL_BL1   LE  5220000.0
        708        V_T_NIL_FCSPS   LE   435000.0
        <BLANKLINE>
        [709 rows x 3 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            set               the unique identifier of the generic \n
                              constraint, (as `str`)
            type              the type of constraint, i.e '=', '<=' or \n
                              '<=', (as `str`)
            cost              the cost of violating the constraint, \n
                              (as `np.float64`)
            ================  ========================================
        """

        constraints = self.xml['NEMSPDCaseFile']['NemSpdInputs']['GenericConstraintCollection']['GenericConstraint']
        rhs_values = dict(set=[], type=[], cost=[])
        for con in constraints:
            rhs_values['set'].append(con['@ConstraintID'])
            rhs_values['type'].append(con['@Type'])
            rhs_values['cost'].append(float(con['@ViolationPrice']))
        return pd.DataFrame(rhs_values)

    def get_constraint_region_lhs(self):
        """Get generic constraints lhs term regional coefficients.

        This is a compact way of describing constraints that apply to all units in a region. If a constraint set appears
        here and also in the unit specific lhs table then the coefficents used in the constraint is the sum of the two
        coefficients, this can be used to exclude particular units from otherwise region wide constraints.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_constraint_region_lhs()
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
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            set               the unique identifier of the generic \n
                              constraint, (as `str`)
            region            the regions the constraint applies in, \n
                              (as `str`)
            service           the services the constraint applies too, \n
                              (as `str`)
            coefficient       the coefficient of the terms on the lhs, \n
                              (as `np.float64`)
            ================  ========================================
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
        """Get generic constraints lhs term unit coefficients.

        If a constraint set appears here and also in the region lhs table then the coefficents used in the
        constraint is the sum of the two coefficients, this can be used to exclude particular units from otherwise
        region wide constraints.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_constraint_unit_lhs()
                              set      unit service  coefficient
        0                #BANN1_E     BANN1    ENOF          1.0
        1               #BNGSF2_E    BNGSF2    ENOF          1.0
        2             #CROWLWF1_E  CROWLWF1    ENOF          1.0
        3              #CSPVPS1_E   CSPVPS1    ENOF          1.0
        4              #DAYDSF1_E   DAYDSF1    ENOF          1.0
        ...                   ...       ...     ...          ...
        5864      V_ARWF_FSTTRP_5     ARWF1    ENOF          1.0
        5865      V_MTGBRAND_33WT  MTGELWF1    ENOF          1.0
        5866     V_OAKHILL_TFB_42  OAKLAND1    ENOF          1.0
        5867          V_OWF_NRB_0  OAKLAND1    ENOF          1.0
        5868  V_OWF_TGTSNRBHTN_30  OAKLAND1    ENOF          1.0
        <BLANKLINE>
        [5869 rows x 4 columns]

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            set               the unique identifier of the generic \n
                              constraint, (as `str`)
            unit              the units the constraint applies in, \n
                              (as `str`)
            service           the services the constraint applies too, \n
                              (as `str`)
            coefficient       the coefficient of the terms on the lhs, \n
                              (as `np.float64`)
            ================  ========================================
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
        """Get generic constraints lhs term interconnector coefficients.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_constraint_interconnector_lhs()
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

            ================  ========================================
            Columns:          Description:
            set               the unique identifier of the generic \n
                              constraint, (as `str`)
            interconnector    the interconnector the constraint applies in, \n
                              (as `str`)
            coefficient       the coefficient of the terms on the lhs, \n
                              (as `np.float64`)
            ================  ========================================
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

    def get_market_interconnector_link_bid_availability(self):
        """Get the bid availability of market interconnectors.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.get_market_interconnector_link_bid_availability()
          interconnector to_region  availability
        0      T-V-MNSP1      TAS1         478.0
        1      T-V-MNSP1      VIC1         478.0

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            interconnector    the interconnector the constraint applies in, \n
                              (as `str`)
            to_region         the direction the bid availability applies to, \n
                              (as `str`)
            availability      the availability as bid in by the \n
                              interconnector, (as `str`)
            ================  ========================================
        """
        inters = self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['InterconnectorPeriodCollection']['InterconnectorPeriod']
        bid_availability = dict(interconnector=[], to_region=[], availability=[])
        for inter in inters:
            if inter['@MNSP'] == '1':
                for offer in inter['MNSPOfferCollection']['MNSPOffer']:
                    bid_availability['interconnector'].append(inter['@InterconnectorID'])
                    bid_availability['to_region'].append(offer['@RegionID'])
                    bid_availability['availability'].append(float(offer['@MaxAvail']))
        bid_availability = pd.DataFrame(bid_availability)
        return bid_availability

    def find_intervals_with_violations(self, limit, start_year, start_month, end_year, end_month):
        """Find the set of dispatch intervals where the non-intervention dispatch runs had constraint violations.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2019/01/01 00:00:00')

        >>> manager.find_intervals_with_violations(limit=3, start_year=2019, start_month=1, end_year=2019, end_month=1)
        {'2019/01/01 00:00:00': ['unit_capacity'], '2019/01/01 00:05:00': ['unit_capacity'], '2019/01/01 00:10:00': ['unit_capacity']}

        Parameters
        ----------
        limit : int
            number of intervals to find, finds first intervals in chronolgical order
        start_year : int
            year to start search
        start_month : int
            month to start search
        end_year : int
            year to end search
        end_month : int
            month to end search

        Returns
        -------
        dict

        """
        if end_month == 12:
            start = datetime(year=start_year, month=start_month, day=1)
            end = datetime(year=end_year + 1, month=1, day=1)
        else:
            start = datetime(year=start_year, month=start_month, day=1)
            end = datetime(year=end_year, month=end_month + 1, day=1)

        check_time = start
        intervals = {}
        while check_time <= end and len(intervals) < limit:
            time_as_str = check_time.isoformat().replace('T', ' ').replace('-', '/')
            try:
                self.load_interval(time_as_str)
                violations = self.get_violations()
                for violation_type, violation_value in violations.items():
                    if violation_value > 0.0:
                        if time_as_str not in intervals:
                            intervals[time_as_str] = []
                        intervals[time_as_str].append(violation_type)
            except MissingDataError:
                pass
            check_time += timedelta(minutes=5)
        return intervals


class MissingDataError(Exception):
    """Raise for unable to downloaded data from NEMWeb."""
