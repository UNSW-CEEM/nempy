import requests
import zipfile
import io
import pandas as pd
import sqlite3
from nempy import check
from datetime import datetime, timedelta
from time import time


def download_to_df(table_name, year, month):
    data_url = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip'
    r = requests.get(data_url.format(table=table_name, year=year, month=str(month).zfill(2)))
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    file_name = zf.namelist()[0]
    data = pd.read_csv(zf.open(file_name), skiprows=1)
    return data


class MMSTable:
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        with con:
            cur = con.cursor()
            cur.execute("""DROP TABLE IF EXISTS {};""".format(table_name))
            base_create_query = """CREATE TABLE {}({}, PRIMARY KEY ({}));"""
            columns = ','.join(['{} TEXT'.format(col) for col in self.table_columns[table_name]])
            primary_keys = ','.join(['{}'.format(col) for col in self.table_primary_keys[table_name]])
            create_query = base_create_query.format(table_name, columns, primary_keys)
            cur.execute(create_query)
            con.commit()


class SingleDataSource(MMSTable):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    @check.table_exists()
    def add_data(self, table_name, year, month, con):
        data = download_to_df(table_name, year, month)
        data = data.loc[:, self.table_columns[table_name]]
        with con:
            data.to_sql(table_name, con=con, if_exists='replace', index=False)
            con.commit()


class MultiDataSource(MMSTable):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    @check.table_exists()
    def add_data(self, table_name, year, month, con):
        data = download_to_df(table_name, year, month)
        data = data.loc[:, self.table_columns[table_name]]
        with con:
            data.to_sql(table_name, con=con, if_exists='append', index=False)
            con.commit()


class InputsBySettlementDate(MultiDataSource):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    def get_data(self, date_time, con):
        query = "Select * from {table} where SETTLEMENTDATE == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=con)


class InputsByIntervalDateTime(MultiDataSource):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    def get_data(self, date_time, con):
        query = "Select * from {table} where INTERVAL_DATETIME == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=con)


class InputsByDay(MultiDataSource):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    def get_data(self, date_time, con):
        date_time = datetime.strptime(date_time, '%Y/%m/%d %H:%M:%S')
        date_time = date_time - timedelta(hours=4)
        date_time = date_time.replace(hour=0, minute=0)
        date_time = date_time - timedelta(seconds=1)
        date_time = datetime.isoformat(date_time).replace('-', '/').replace('T', ' ')
        query = "Select * from {table} where INTERVAL_DATETIME == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=con)


class InputsStartAndEnd(MultiDataSource):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    def get_data(self, date_time, con):
        query = "Select * from {table} where START_DATE <= '{datetime}' and END_DATE > '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=con)


class DBManager:
    def __init__(self, db):
        self.con = sqlite3.connect(db)
        self.BIDPEROFFER_D = InputsByIntervalDateTime(
            table_name='BIDPEROFFER_D', table_columns=['INTERVAL_DATETIME', 'DUID', 'BIDTYPE', 'BANDAVAIL1',
                                                       'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4', 'BANDAVAIL5',
                                                       'BANDAVAIL6','BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9',
                                                       'BANDAVAIL10', 'MAXAVAIL', 'ENABLEMENTMIN', 'ENABLEMENTMAX',
                                                       'LOWBREAKPOINT', 'HIGHBREAKPOINT'],
            table_primary_keys=['INTERVAL_DATETIME', 'DUID', 'BIDTYPE'])
        self.BIDDAYOFFER_D = InputsByIntervalDateTime(
            table_name='BIDDAYOFFER_D', table_columns=['SETTLEMENTDATE', 'DUID', 'BIDTYPE', 'PRICEBAND1', 'PRICEBAND2',
                                                       'PRICEBAND3', 'PRICEBAND4', 'PRICEBAND5', 'PRICEBAND6',
                                                       'PRICEBAND7', 'PRICEBAND8', 'PRICEBAND9', 'PRICEBAND10', 'T1',
                                                       'T2', 'T3', 'T4'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID', 'BIDTYPE'])
        self.DISPATCHREGIONSUM = InputsBySettlementDate(
            table_name='DISPATCHREGIONSUM', table_columns=['SETTLEMENTDATE', 'DUID', 'TOTALDEMAND',
                                                           'DEMANDFORECAST', 'INITIALSUPPLY'],
            table_primary_keys=['SETTLEMENTDATE', 'REGIONID'])
        self.DISPATCHLOAD = InputsBySettlementDate(
            table_name='DISPATCHLOAD', table_columns=['SETTLEMENTDATE', 'DUID', 'DISPATCHMODE', 'AGCSTATUS',
                                                      'INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE', 'RAMPUPRATE',
                                                      'AVAILABILITY', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN',
                                                      'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID'])
        self.DUDETAILSUMMARY = InputsBySettlementDate(
            table_name='DUDETAILSUMMARY', table_columns=['SETTLEMENTDATE', 'DUID', 'DISPATCHMODE', 'AGCSTATUS',
                                                      'INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE', 'RAMPUPRATE',
                                                      'AVAILABILITY', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN',
                                                      'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID'])
        self.DISPATCHCONSTRAINT = InputsBySettlementDate(
            table_name='DISPATCHCONSTRAINT', table_columns=['SETTLEMENTDATE', 'CONSTRAINTID', 'RHS',
                                                           'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO'],
            table_primary_keys=['SETTLEMENTDATE', 'CONSTRAINTID'])
        self.GENCONDATA = InputsByIntervalDateTime(
            table_name='GENCONDATA', table_columns=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'CONSTRAINTTYPE'
                                                    'GENERICCONSTRAINTWEIGHT'],
            table_primary_keys=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'])
        self.SPDREGIONCONSTRAINT = InputsByIntervalDateTime(
            table_name='SPDREGIONCONSTRAINT', table_columns=['REGIONID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID'
                                                             'BIDTYPE', 'FACTOR'],
            table_primary_keys=['REGIONID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'BIDTYPE'])
        self.SPDCONNECTIONPOINTCONSTRAINT = InputsByIntervalDateTime(
            table_name='SPDCONNECTIONPOINTCONSTRAINT', table_columns=['CONNECTIONPOINTID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                      'GENCONID', 'BIDTYPE', 'FACTOR'],
            table_primary_keys=['CONNECTIONPOINTID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'BIDTYPE'])
        self.SPDINTERCONNECTORCONSTRAINT = InputsByIntervalDateTime(
            table_name='SPDINTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                     'GENCONID', 'BIDTYPE', 'FACTOR'],
            table_primary_keys=['INTERCONNECTORID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'])
        self.INTERCONNECTOR = InputsByIntervalDateTime(
            table_name='INTERCONNECTOR', table_columns=['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO'],
            table_primary_keys=['INTERCONNECTORID'])
        self.INTERCONNECTORCONSTRAINT = InputsByIntervalDateTime(
            table_name='INTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                  'FROMREGIONLOSSSHARE', 'LOSSCONSTANT',
                                                                  'LOSSFLOWCOEFFICIENT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'])
        self.LOSSMODEL = InputsByIntervalDateTime(
            table_name='LOSSMODEL', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'LOSSSEGMENT',
                                                   'MWBREAKPOINT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'])
        self.LOSSFACTORMODEL = InputsByIntervalDateTime(
            table_name='LOSSFACTORMODEL', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'REGIONID',
                                                         'DEMANDCOEFFICIENT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'])
        self.DISPATCHINTERCONNECTORRES = InputsBySettlementDate(
            table_name='DISPATCHINTERCONNECTORRES', table_columns=['INTERCONNECTORID', 'SETTLEMENTDATE'],
            table_primary_keys=['INTERCONNECTORID', 'SETTLEMENTDATE'])


    @check.table_exists()
    def get_historical_inputs_old(self, table_name, applicable_for):
        settlement_date_query = "Select * from {table} where SETTLEMENTDATE == '{datetime}'"
        dispatch_interval_data_query = "Select * from {table} where INTERVAL_DATETIME == '{datetime}'"
        start_and_end_time_query = "Select * from {table} where START_DATE <= '{datetime}' and END_DATE > '{datetime}'"
        effective_date = """Create temporary table temp as
                                Select * from {table} where EFFECTIVEDATE <= '{datetime}';
                                
                            Create temporary table temp2 as
                                Select {id}, EFFECTIVEDATE, max(VERSIONNO) as VERSIONNO 
                                  from temp 
                              group by {id}, EFFECTIVEDATE';
                            
                            Create temporary table temp3 as
                                Select {id}, VERSIONNO, max(EFFECTIVEDATE) as EFFECTIVEDATE 
                                  from temp2 
                              group by {id}';
                              
                              Select * from {table} inner join temp3 on {id}, VERSIONNO, EFFECTIVEDATE
                         """

        query_to_execute = queries_by_table[table_name].format(table=table_name, datetime=applicable_for,
                                                               id='INTERCONNECTORID')
        return pd.read_sql_query(query_to_execute, con=self.con)



