import xmltodict
import pandas as pd
import requests
import zipfile
import io
import os
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta, time


class xml_inputs:

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
        with open(self.get_file_path()) as file:
            inputs = xmltodict.parse(file.read())
        self.xml = inputs

    def get_unit_initial_conditions_dataframe(self):
        traders = self.xml['NEMSPDCaseFile']['NemSpdInputs']['TraderCollection']['Trader']
        initial_conditions = dict(DUID=[], INITIALMW=[], RAMPUPRATE=[], RAMPDOWNRATE=[])
        if self.is_intervention_period():
            INITIALMW_name = 'WhatIfInitialMW'
        else:
            INITIALMW_name = 'InitialMW'
        name_map = dict(INITIALMW=INITIALMW_name, RAMPUPRATE='SCADARampUpRate', RAMPDOWNRATE='SCADARampDnRate')
        for trader in traders:
            initial_conditions['DUID'].append(trader['@TraderID'])
            initial_cons = trader['TraderInitialConditionCollection']['TraderInitialCondition']
            for our_name, aemo_name in name_map.items():
                for con in initial_cons:
                    if con['@InitialConditionID'] == aemo_name:
                        value = float(con['@Value'])
                        break
                    else:
                        value = value = np.NAN
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
        name_map = dict(TOTAL_UGIF_VIOLATION='@TotalUIGFViolation',
                        TOTAL_UNIT_CAPACITY_VIOLATION='@TotalUnitMWCapacityViolation',
                        TOTAL_UNIT_ENERGY_OFFER_VIOLATION='@TotalEnergyOfferViolation',
                        TOTAL_RAMP_RATE_VIOLATION='@TotalRampRateViolation',
                        TOTAL_FAST_START_VIOLATION='@TotalFastStartViolation')
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

    def is_intervention_period(self):
        return type(self.xml['NEMSPDCaseFile']['NemSpdOutputs']['PeriodSolution']) == list




