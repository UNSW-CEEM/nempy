import xmltodict
import pandas as pd
import requests
import zipfile
import io
import os
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, time
from time import sleep

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
        """Download data to the cache from the AEMO website. Data downloaded is inclusive of the start and end month."""

        if end_month == 12:
            end_month = 1
            end_year += 1
        else:
            end_month += 1

        self.populate_by_day(start_year=start_year, start_month=start_month, start_day=1,
                             end_year=end_year, end_month=end_month, end_day=1, verbose=verbose)

    def populate_by_day(self, start_year, start_month, end_year, end_month, start_day, end_day, verbose=True):
        """Download data to the cache from the AEMO website. Data downloaded is inclusive of the start and end date.
        """

        start = datetime(year=start_year, month=start_month, day=start_day) - timedelta(days=1)
        end = datetime(year=end_year, month=end_month, day=end_day)
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

        >>> manager.load_interval('2024/07/10 12:05:00')

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
                raise MissingDataError(
                    'File not downloaded, check internet connection and that NEMWeb contains data for interval {}.'.format(
                        self.interval))
        with open(self.get_file_path()) as file:
            read = file.read()
            self.xml = xmltodict.parse(read)

    def interval_inputs_in_cache(self):
        """Check if the cache contains the data for the loaded interval, primarily for debugging.

        Examples
        --------

        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_file_path() # doctest: +SKIP
        PosixPath('test_nemde_cache/NEMSPDOutputs_2018123124000.loaded')

        So the doctest runs on all Operating systems lets also look at the parts of the path.

        >>> manager.get_file_path().parts
        ('test_nemde_cache', 'NEMSPDOutputs_2024071009700.loaded')
        """
        return Path(self.cache_folder) / self.get_file_name()

    def get_file_name(self):
        """Get the filename of the currently loaded interval.

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_file_name()
        'NEMSPDOutputs_2024071009700.loaded'
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
        try:
            r = requests.get(url)
            z = zipfile.ZipFile(io.BytesIO(r.content))
            z.extractall(self.cache_folder)
        except zipfile.BadZipFile:
            sleep(200)
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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_unit_initial_conditions()
                DUID TRADERTYPE  INITIALMW  RAMPUPRATE  RAMPDOWNRATE  AGCSTATUS
        0    ADPBA1G  GENERATOR    0.00000   93.119938     93.119938        1.0
        1    ADPBA1L       LOAD    1.40400   93.119938     93.119938        1.0
        2     ADPPV1  GENERATOR   10.90800  298.499937    298.499937        0.0
        3     AGLHAL  GENERATOR    0.00000         NaN           NaN        0.0
        4     AGLSOM  GENERATOR   60.00000         NaN           NaN        0.0
        ..       ...        ...        ...         ...           ...        ...
        492  YENDWF1  GENERATOR    6.75000         NaN           NaN        0.0
        493    YWPS1  GENERATOR    0.00000  180.000000    180.000000        0.0
        494    YWPS2  GENERATOR  358.89621  176.624994    176.624994        1.0
        495    YWPS3  GENERATOR  371.52658  181.124997    181.124997        1.0
        496    YWPS4  GENERATOR  337.93546  180.000000    180.000000        1.0
        <BLANKLINE>
        [497 rows x 6 columns]

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
        initial_conditions = dict(DUID=[], TRADERTYPE=[], INITIALMW=[], RAMPUPRATE=[], RAMPDOWNRATE=[], AGCSTATUS=[])
        if self.is_intervention_period():
            INITIALMW_name = 'WhatIfInitialMW'
        else:
            INITIALMW_name = 'InitialMW'
        name_map = dict(INITIALMW=INITIALMW_name, RAMPUPRATE='SCADARampUpRate', RAMPDOWNRATE='SCADARampDnRate',
                        AGCSTATUS='AGCStatus')
        for trader in traders:
            initial_conditions['DUID'].append(trader['@TraderID'])
            initial_conditions['TRADERTYPE'].append(trader['@TraderType'])
            initial_cons = trader['TraderInitialConditionCollection']['TraderInitialCondition']
            for our_name, aemo_name in name_map.items():
                for con in initial_cons:
                    if con['@InitialConditionID'] == aemo_name:
                        value = float(con['@Value'])
                        break
                    else:
                        value = np.nan
                initial_conditions[our_name].append(value)
        initial_conditions = pd.DataFrame(initial_conditions)
        return initial_conditions

    def get_unit_fast_start_parameters(self):
        """Get the unit fast start dispatch inflexibility parameter values.

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_unit_fast_start_parameters()
                DUID  MinLoadingMW  CurrentMode  CurrentModeTime  T1  T2  T3  T4
        0     AGLHAL             2            0                0  10   3  10   2
        1     AGLSOM            12            4                2  20   2  35   2
        2   BARRON-1             5            4                1  12   3  10   1
        3   BARRON-2             5            0                0  12   3  10   1
        4   BBTHREE1            17            0                0   8   4   1   1
        ..       ...           ...          ...              ...  ..  ..  ..  ..
        68     VPGS4            50            0                0   5   8  15   0
        69     VPGS5            50            0                0   5   3  15   0
        70     VPGS6            50            0                0   5   8  15   0
        71   W/HOE#1            40            0                0   4   2  15   0
        72   W/HOE#2            40            0                0   4   1  15   0
        <BLANKLINE>
        [73 rows x 8 columns]

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        # >>> manager.load_interval('2024/08/01 02:15:00')

        >>> manager.get_unit_volume_bids()
                 DUID     BIDTYPE DIRECTION  MAXAVAIL  ENABLEMENTMIN  ENABLEMENTMAX  LOWBREAKPOINT  HIGHBREAKPOINT  BANDAVAIL1  BANDAVAIL2  BANDAVAIL3  BANDAVAIL4  BANDAVAIL5  BANDAVAIL6  BANDAVAIL7  BANDAVAIL8  BANDAVAIL9  BANDAVAIL10  RAMPDOWNRATE  RAMPUPRATE
        0     ADPBA1G      ENERGY      None       6.0            6.0            6.0            6.0             6.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         6.0         0.0          0.0         120.0       120.0
        1     ADPBA1G    LOWERREG      None       6.0            0.0            6.0            6.0             6.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         6.0         0.0          0.0           0.0         0.0
        2     ADPBA1G   RAISE5MIN      None       3.0            0.0            6.0            0.0             3.0         3.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0          0.0           0.0         0.0
        3     ADPBA1G    RAISEREG      None       6.0            0.0            6.0            0.0             0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         6.0         0.0          0.0           0.0         0.0
        4     ADPBA1G  RAISE60SEC      None       3.0            0.0            6.0            0.0             3.0         3.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0          0.0           0.0         0.0
        ...       ...         ...       ...       ...            ...            ...            ...             ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...           ...         ...
        1725    YWPS4   LOWER6SEC      None       0.0          250.0          385.0          275.0           385.0        15.0        10.0         0.0         0.0         0.0         0.0         0.0         0.0         0.0          0.0           0.0         0.0
        1726    YWPS4   RAISE5MIN      None       0.0          250.0          390.0          250.0           380.0         0.0         0.0         0.0         0.0         5.0         0.0         0.0         5.0         0.0         10.0          10.0        10.0
        1727    YWPS4    RAISEREG      None       0.0          250.0          385.0          250.0           370.0         0.0         0.0         0.0         0.0         0.0         0.0         5.0        10.0         0.0          5.0           5.0         5.0
        1728    YWPS4  RAISE60SEC      None       0.0          220.0          400.0          220.0           390.0         0.0         0.0         0.0         0.0         0.0         5.0         5.0         0.0         0.0         10.0          10.0        10.0
        1729    YWPS4   RAISE6SEC      None       0.0          220.0          405.0          220.0           390.0         0.0         0.0         0.0         5.0         5.0         5.0         5.0         0.0         0.0          5.0           5.0         5.0
        <BLANKLINE>
        [1730 rows x 20 columns]

        Returns
        --------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            DIRECTION         "LOAD" or "GENERATOR", (as `str`) \n
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
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['TraderPeriodCollection'][
            'TraderPeriod']
        trades_by_unit_and_type = dict(DUID=[], BIDTYPE=[], DIRECTION=[], MAXAVAIL=[], ENABLEMENTMIN=[], ENABLEMENTMAX=[],
                                       LOWBREAKPOINT=[], HIGHBREAKPOINT=[], BANDAVAIL1=[], BANDAVAIL2=[],
                                       BANDAVAIL3=[], BANDAVAIL4=[], BANDAVAIL5=[], BANDAVAIL6=[], BANDAVAIL7=[],
                                       BANDAVAIL8=[], BANDAVAIL9=[], BANDAVAIL10=[], RAMPDOWNRATE=[], RAMPUPRATE=[])
        name_map = dict(BIDTYPE='@TradeType', DIRECTION='@Direction', MAXAVAIL='@MaxAvail', ENABLEMENTMIN='@EnablementMin',
                        ENABLEMENTMAX='@EnablementMax', LOWBREAKPOINT='@LowBreakpoint',
                        HIGHBREAKPOINT='@HighBreakpoint', BANDAVAIL1='@BandAvail1', BANDAVAIL2='@BandAvail2',
                        BANDAVAIL3='@BandAvail3', BANDAVAIL4='@BandAvail4', BANDAVAIL5='@BandAvail5',
                        BANDAVAIL6='@BandAvail6', BANDAVAIL7='@BandAvail7', BANDAVAIL8='@BandAvail8',
                        BANDAVAIL9='@BandAvail9', BANDAVAIL10='@BandAvail10', RAMPDOWNRATE='@RampDnRate',
                        RAMPUPRATE='@RampUpRate')

        def append_values(trade, trades_by_unit_and_type):
            trades_by_unit_and_type['DUID'].append(trader['@TraderID'])
            for our_name, aemo_name in name_map.items():
                if aemo_name in trade:
                    if aemo_name in ['@TradeType', '@Direction']:
                        value = trade[aemo_name]
                    else:
                        value = float(trade[aemo_name])
                elif aemo_name == '@Direction':
                    value = 'missing'
                trades_by_unit_and_type[our_name].append(value)
            return trades_by_unit_and_type

        for trader in traders:
            if type(trader['TradeCollection']['Trade']) != list:
                trades = trader['TradeCollection']
                for _, trade in trades.items():
                    trades_by_unit_and_type = append_values(trade, trades_by_unit_and_type)
            else:
                for trade in trader['TradeCollection']['Trade']:
                    trades_by_unit_and_type = append_values(trade, trades_by_unit_and_type)

        trades_by_unit_and_type = pd.DataFrame(trades_by_unit_and_type)
        bid_type_map = dict(ENOF='ENERGY', LDOF='ENERGY', DROF='ENERGY', BDOF='ENERGY', L5RE='LOWERREG', R5RE='RAISEREG',
                            R5MI='RAISE5MIN', L5MI='LOWER5MIN', R60S='RAISE60SEC', L60S='LOWER60SEC', R6SE='RAISE6SEC',
                            L6SE='LOWER6SEC', R1SE='RAISE1SEC', L1SE='LOWER1SEC')
        trades_by_unit_and_type["BIDTYPE"] = trades_by_unit_and_type["BIDTYPE"].apply(lambda x: bid_type_map[x])
        direction_type_map = dict(GEN='GENERATOR', LOAD='LOAD', missing=None)
        trades_by_unit_and_type["DIRECTION"] = (
            trades_by_unit_and_type["DIRECTION"].apply(lambda x: direction_type_map[x]))
        return trades_by_unit_and_type

    def get_unit_price_bids(self):
        """Get the unit volume bids

        Examples
        --------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_unit_price_bids()
                 DUID     BIDTYPE  DIRECTION  PRICEBAND1  PRICEBAND2  PRICEBAND3  PRICEBAND4  PRICEBAND5  PRICEBAND6  PRICEBAND7  PRICEBAND8  PRICEBAND9  PRICEBAND10
        0     ADPBA1G      ENERGY  GENERATOR     -966.92        0.00       53.28       94.72      165.76      270.34      369.01      984.68     3945.63      9866.53
        1     ADPBA1G    LOWERREG  GENERATOR        5.00        8.00       12.00       18.00       24.00       47.00       98.00      268.00      498.00     12000.00
        2     ADPBA1G   RAISE5MIN  GENERATOR        0.00        1.00        2.00        3.00        4.00        5.00        6.00      100.00     1000.00     15000.00
        3     ADPBA1G    RAISEREG  GENERATOR        5.00        8.00       12.00       18.00       24.00       47.00       98.00      268.00      498.00     12000.00
        4     ADPBA1G  RAISE60SEC  GENERATOR        0.00        1.00        2.00        3.00        4.00        5.00        6.00      100.00     1000.00     15000.00
        ...       ...         ...        ...         ...         ...         ...         ...         ...         ...         ...         ...         ...          ...
        1725    YWPS4   LOWER6SEC  GENERATOR        0.03        0.05        0.16        0.30        1.90       25.04       30.04       99.00     4600.00      9899.00
        1726    YWPS4   RAISE5MIN  GENERATOR        0.36        0.71        1.41        4.33       19.88       28.88       46.88       97.88      558.88     12400.40
        1727    YWPS4    RAISEREG  GENERATOR        0.05        2.70        9.99       19.99       49.00       95.50      240.00      450.50      950.50     11900.00
        1728    YWPS4  RAISE60SEC  GENERATOR        0.36        0.84        1.41        4.78       19.88       28.88       46.88       97.88      558.88     11999.00
        1729    YWPS4   RAISE6SEC  GENERATOR        0.36        0.84        1.41        4.78       19.88       28.88       46.88       97.88      558.88     12299.00
        <BLANKLINE>
        [1730 rows x 13 columns]


        Returns
        --------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            BIDTYPE           the service the bid applies to, \n
                              (as `str`)
            DIRECTION         "LOAD" or "GENERATOR"
            PRICEBAND1        the volume bid in the first bid band,
                              in MW, (as `np.float64`)
                 :
            PRICEBAND10       the volume bid in the tenth bid band,
                              in MW, (as `np.float64`)
            ================  ========================================


        """
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['TraderCollection']['Trader']
        trades_by_unit_and_type = dict(DUID=[], BIDTYPE=[], DIRECTION=[], PRICEBAND1=[], PRICEBAND2=[], PRICEBAND3=[],
                                       PRICEBAND4=[], PRICEBAND5=[], PRICEBAND6=[], PRICEBAND7=[], PRICEBAND8=[],
                                       PRICEBAND9=[], PRICEBAND10=[])
        name_map = dict(BIDTYPE='@TradeType', DIRECTION='@Direction', PRICEBAND1='@PriceBand1',
                        PRICEBAND2='@PriceBand2', PRICEBAND3='@PriceBand3', PRICEBAND4='@PriceBand4',
                        PRICEBAND5='@PriceBand5', PRICEBAND6='@PriceBand6', PRICEBAND7='@PriceBand7',
                        PRICEBAND8='@PriceBand8', PRICEBAND9='@PriceBand9', PRICEBAND10='@PriceBand10')

        def append_values(trade, trades_by_unit_and_type):
            trades_by_unit_and_type['DUID'].append(trader['@TraderID'])
            for our_name, aemo_name in name_map.items():
                value = None
                if aemo_name in trade:
                    if aemo_name in ['@TradeType', '@Direction']:
                        value = trade[aemo_name]
                    else:
                        value = float(trade[aemo_name])
                elif '@Direction' not in trade:
                    if trader['@TraderType'] in ["GENERATOR", "NORMALLY_ON_LOAD", "WDR", "BIDIRECTIONAL"]:
                        value = "GEN"
                    elif trader['@TraderType'] in ["LOAD"]:
                        value = "LOAD"
                trades_by_unit_and_type[our_name].append(value)
            return trades_by_unit_and_type

        for trader in traders:
            if type(trader['TradePriceStructureCollection']['TradePriceStructure']['TradeTypePriceStructureCollection']
                    ['TradeTypePriceStructure']) != list:
                trades = trader['TradePriceStructureCollection']['TradePriceStructure'][
                    'TradeTypePriceStructureCollection']
                for _, trade in trades.items():
                    trades_by_unit_and_type = append_values(trade, trades_by_unit_and_type)
            else:
                for trade in \
                        trader['TradePriceStructureCollection']['TradePriceStructure'][
                            'TradeTypePriceStructureCollection'][
                            'TradeTypePriceStructure']:
                    trades_by_unit_and_type = append_values(trade, trades_by_unit_and_type)

        trades_by_unit_and_type = pd.DataFrame(trades_by_unit_and_type)
        bid_type_map = dict(ENOF='ENERGY', LDOF='ENERGY', DROF='ENERGY', BDOF='ENERGY', L5RE='LOWERREG',
                            R5RE='RAISEREG',
                            R5MI='RAISE5MIN', L5MI='LOWER5MIN', R60S='RAISE60SEC', L60S='LOWER60SEC', R6SE='RAISE6SEC',
                            L6SE='LOWER6SEC', R1SE='RAISE1SEC', L1SE='LOWER1SEC')
        trades_by_unit_and_type["BIDTYPE"] = trades_by_unit_and_type["BIDTYPE"].apply(lambda x: bid_type_map[x])
        direction_type_map = dict(GEN='GENERATOR', LOAD='LOAD')
        trades_by_unit_and_type["DIRECTION"] = (
            trades_by_unit_and_type["DIRECTION"].apply(lambda x: direction_type_map[x]))
        return trades_by_unit_and_type

    def get_UIGF_values(self):
        """Get the unit unconstrained intermittent generation forecast.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_UIGF_values()
                 DUID      UIGF
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
            DUID              unique identifier of a dispatch unit, \n
                              (as `str`)
            UGIF              the units generation forecast for end \n
                              of the inteval, in MW, (as `np.float64`)
            ================  ========================================


        """
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['TraderPeriodCollection'][
            'TraderPeriod']
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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_violations()
        {'regional_demand': 0.0, 'interocnnector': 0.0, 'generic_constraint': 0.0, 'ramp_rate': 0.416, 'unit_capacity': 0.3, 'energy_constraint': 0.0, 'energy_offer': 0.0, 'fcas_profile': 0.0, 'fast_start': 0.0, 'mnsp_ramp_rate': 0.0, 'msnp_offer': 0.0, 'mnsp_capacity': 0.0, 'ugif': 0.0}

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
            for name, aemo_name in name_map.items():
                for solution in outputs['PeriodSolution']:
                    if solution['@Intervention'] == '0':
                        if aemo_name in solution:
                            violations[name] = float(solution[aemo_name])
                for solution in outputs['CaseSolution']:
                    if solution['@Intervention'] == '0':
                        if name not in violations:
                            violations[name] = float(solution[aemo_name])
        else:
            for name, aemo_name in name_map.items():
                if aemo_name in outputs['PeriodSolution']:
                    violations[name] = float(outputs['PeriodSolution'][aemo_name])
                else:
                    violations[name] = float(outputs['CaseSolution'][aemo_name])

        return violations

    def get_constraint_violation_prices(self):
        """Get the price of violating different constraint sets.

        For more information on the constraint sets :download:`see AMEO docs  <../../docs/pdfs/Schedule of Constraint Violation Penalty factors.pdf>`

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_constraint_violation_prices()
        {'regional_demand': 2625000.0, 'interocnnector': 20125000.0, 'generic_constraint': 525000.0, 'ramp_rate': 20212500.0, 'unit_capacity': 6475000.0, 'energy_offer': 19862500.0, 'fcas_profile': 2712500.0, 'fcas_max_avail': 2712500.0, 'fcas_enablement_min': 1225000.0, 'fcas_enablement_max': 1225000.0, 'fast_start': 19775000.0, 'mnsp_ramp_rate': 20212500.0, 'msnp_offer': 19862500.0, 'mnsp_capacity': 6387500.0, 'uigf': 6737500.0, 'voll': 17500.0, 'tiebreak': 1e-06}

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
                        voll='@VoLL',
                        tiebreak='@TieBreakPrice')
        violations = {}
        for name, aemo_name in name_map.items():
            violations[name] = float(inputs['Case'][aemo_name])
        return violations

    def is_intervention_period(self):
        """Check if the interval currently loaded was subject to an intervention.

        Examples
        -------
        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_constraint_rhs()
                           set           rhs
        0          #BANGOWF2_E     82.800000
        1          #BBATRYL1_E     50.000000
        2          #BBATTERY_E     50.000000
        3          #BBTHREE3_E     25.000000
        4           #BOWWPV1_E      6.100000
        ...                ...           ...
        1107       V_T_NIL_BL1 -10125.000000
        1108     V_T_NIL_FCSPS    493.111848
        1109    V_WDR_NO_SCADA     95.000000
        1110  V_WEMENSF_FLT_20     20.000000
        1111   V_YATPSF_FLT_20     20.000000
        <BLANKLINE>
        [1112 rows x 2 columns]

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_constraint_type()
                           set type       cost
        0          #BANGOWF2_E   LE  6300000.0
        1          #BBATRYL1_E   LE  6300000.0
        2          #BBATTERY_E   LE  6300000.0
        3          #BBTHREE3_E   LE  6300000.0
        4           #BOWWPV1_E   LE  6300000.0
        ...                ...  ...        ...
        1172       V_T_NIL_BL1   GE  6300000.0
        1173     V_T_NIL_FCSPS   LE   525000.0
        1174    V_WDR_NO_SCADA   LE  6300000.0
        1175  V_WEMENSF_FLT_20   LE   612500.0
        1176   V_YATPSF_FLT_20   LE   612500.0
        <BLANKLINE>
        [1177 rows x 3 columns]

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_constraint_region_lhs()
                            set region service  coefficient
        0        D_I+BIP_ML2_L1   NSW1    L1SE          1.0
        1        D_I+BIP_ML2_L1   QLD1    L1SE          1.0
        2        D_I+BIP_ML2_L1    SA1    L1SE          1.0
        3        D_I+BIP_ML2_L1   TAS1    L1SE          1.0
        4        D_I+BIP_ML2_L1   VIC1    L1SE          1.0
        ..                  ...    ...     ...          ...
        498  F_TASCAP_RREG_0220   NSW1    R5RE          1.0
        499  F_TASCAP_RREG_0220   QLD1    R5RE          1.0
        500  F_TASCAP_RREG_0220    SA1    R5RE          1.0
        501  F_TASCAP_RREG_0220   VIC1    R5RE          1.0
        502     F_T_NIL_MINP_R6   TAS1    R6SE          1.0
        <BLANKLINE>
        [503 rows x 4 columns]

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_constraint_unit_lhs()
                            set      unit service  coefficient
        0           #BANGOWF2_E  BANGOWF2    ENOF          1.0
        1           #BBATRYL1_E  BBATRYL1    LDOF          1.0
        2           #BBATTERY_E  BBATTERY    ENOF          1.0
        3           #BBTHREE3_E  BBTHREE3    ENOF          1.0
        4            #BOWWPV1_E   BOWWPV1    ENOF          1.0
        ...                 ...       ...     ...          ...
        17032    V_WDR_NO_SCADA  DRXVDX01    DROF          1.0
        17033    V_WDR_NO_SCADA  DRXVQP01    DROF          1.0
        17034    V_WDR_NO_SCADA  DRXVQX01    DROF          1.0
        17035  V_WEMENSF_FLT_20  WEMENSF1    ENOF          1.0
        17036   V_YATPSF_FLT_20    YATSF1    ENOF          1.0
        <BLANKLINE>
        [17037 rows x 4 columns]

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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_constraint_interconnector_lhs()
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

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_market_interconnector_link_bid_availability()
          interconnector to_region  availability
        0      T-V-MNSP1      TAS1         478.0
        1      T-V-MNSP1      VIC1         594.0

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
        inters = \
            self.xml['NEMSPDCaseFile']['NemSpdInputs']['PeriodCollection']['Period']['InterconnectorPeriodCollection'][
                'InterconnectorPeriod']
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

        >>> manager.load_interval('2024/07/10 12:05:00')

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

    def get_service_prices(self):
        """Get the energy market and FCAS prices by region.

        Examples
        --------

        >>> manager = XMLCacheManager('test_nemde_cache')

        >>> manager.load_interval('2024/07/10 12:05:00')

        >>> manager.get_service_prices()
           region     service      price
        0    NSW1      ENERGY   53.99972
        1    NSW1   RAISE5MIN       0.25
        2    NSW1  RAISE60SEC       0.25
        3    NSW1  LOWER60SEC          3
        4    NSW1   RAISE6SEC       0.38
        5    NSW1   LOWER6SEC          1
        6    NSW1   RAISE1SEC       0.94
        7    NSW1   LOWER1SEC       0.01
        8    QLD1      ENERGY      -10.4
        9    QLD1   RAISE5MIN       0.25
        10   QLD1  RAISE60SEC       0.25
        11   QLD1  LOWER60SEC          3
        12   QLD1   RAISE6SEC       0.38
        13   QLD1   LOWER6SEC          1
        14   QLD1   RAISE1SEC       0.94
        15   QLD1   LOWER1SEC       0.01
        16    SA1      ENERGY        -30
        17    SA1   RAISE5MIN       0.25
        18    SA1  RAISE60SEC       0.25
        19    SA1  LOWER60SEC          3
        20    SA1   RAISE6SEC       0.38
        21    SA1   LOWER6SEC          1
        22    SA1   RAISE1SEC       0.94
        23    SA1   LOWER1SEC       0.01
        24   TAS1      ENERGY      260.2
        25   TAS1   RAISE5MIN       0.38
        26   TAS1  RAISE60SEC       0.38
        27   TAS1  LOWER60SEC       0.38
        28   TAS1   RAISE6SEC       0.38
        29   TAS1   LOWER6SEC       0.38
        30   TAS1   RAISE1SEC       0.94
        31   TAS1   LOWER1SEC          0
        32   VIC1      ENERGY  202.07105
        33   VIC1   RAISE5MIN       0.25
        34   VIC1  RAISE60SEC       0.25
        35   VIC1  LOWER60SEC          3
        36   VIC1   RAISE6SEC       0.38
        37   VIC1   LOWER6SEC          1
        38   VIC1   RAISE1SEC       0.94
        39   VIC1   LOWER1SEC       0.01

        Returns
        -------
        pd.DataFrame

            ================  ========================================
            Columns:          Description:
            region            the region (as `str`)
            service           the services (as `str`), i.e. energy, \n
                              lower_1s, lower_5min, etc
            price             the price of the service (as `np.float64`)
            ================  ========================================
        """
        service_type_map = \
            {'@EnergyPrice': 'ENERGY', '@LRegPrice': 'LOWERREG', '@RRegPrice': 'RAISEREG', '@R5Price': 'RAISE5MIN',
             '@RL5Price': 'LOWER5MIN', '@R60Price': 'RAISE60SEC', '@L60Price': 'LOWER60SEC', '@R6Price': 'RAISE6SEC',
             '@L6Price': 'LOWER6SEC', '@R1Price': 'RAISE1SEC', '@L1Price': 'LOWER1SEC'}
        prices = dict(region=[], service=[], price=[])
        regions = self.xml['NEMSPDCaseFile']['NemSpdOutputs']['RegionSolution']
        for region in regions:
            for xml_service, mms_service in service_type_map.items():
                if xml_service in region:
                    prices['region'].append(region['@RegionID'])
                    prices['service'].append(mms_service)
                    prices['price'].append(region[xml_service])
        return pd.DataFrame(prices)


class MissingDataError(Exception):
    """Raise for unable to downloaded data from NEMWeb."""
