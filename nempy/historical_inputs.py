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
        self.tables = {}
        self.table_types = {
            'BIDPEROFFER_D': InputsByIntervalDateTime,
            'BIDDAYOFFER_D': InputsByDay,
            'DISPATCHREGIONSUM': InputsBySettlementDate,
            'DISPATCHLOAD': InputsBySettlementDate,
            'DISPATCHCONSTRAINT': InputsBySettlementDate,
            'GENCONDATA': ['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'],
            'DUDETAILSUMMARY': InputsStartAndEnd,
            'LOSSMODEL': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'LOSSSEGMENT'],
            'LOSSFACTORMODEL': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'REGIONID'],
            'SPDREGIONCONSTRAINT': ['REGIONID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'BIDTYPE'],
            'SPDCONNECTIONPOINTCONSTRAINT': ['CONNECTIONPOINTID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'BIDTYPE'],
            'SPDINTERCONNECTORCONSTRAINT': ['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID'],
            'INTERCONNECTOR': ['INTERCONNECTORID'],
            'INTERCONNECTORCONSTRAINT': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'MAXMWIN', 'MAXMWOUT',
                                         'LOSSCONSTANT', 'LOSSFLOWCOEFFICIENT', 'FROMREGIONLOSSSHARE', 'ICTYPE']
        }

    def create_table(self, table_name, table_columns, table_primary_keys):
        self.tables[table_name] = self.table_types[table_name](table_name, table_columns, table_primary_keys, self.con)

    def get_historical_inputs(self, table_name, datetime):
        self.tables[table_name].get_data(table_name, datetime, self.con)

    def add_data(self, table_name, year, month):
        self.tables[table_name].add_data(table_name, year, month, self.con)

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
        queries_by_table = {
            'BIDPEROFFER_D': dispatch_interval_data_query,
            'BIDDAYOFFER_D': settlement_date_query,
            'DISPATCHREGIONSUM': settlement_date_query,
            'DISPATCHLOAD': settlement_date_query,
            'DISPATCHCONSTRAINT': settlement_date_query,
            'GENCONDATA': ['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'],
            'DUDETAILSUMMARY': ['DUID', 'START_DATE'],
            'LOSSMODEL': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'LOSSSEGMENT'],
            'LOSSFACTORMODEL': effective_date,
            'SPDREGIONCONSTRAINT': ['REGIONID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'BIDTYPE'],
            'SPDCONNECTIONPOINTCONSTRAINT': ['CONNECTIONPOINTID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID', 'BIDTYPE'],
            'SPDINTERCONNECTORCONSTRAINT': ['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID'],
            'INTERCONNECTOR': ['INTERCONNECTORID'],
            'INTERCONNECTORCONSTRAINT': ['EFFECTIVEDATE', 'VERSIONNO', 'INTERCONNECTORID', 'MAXMWIN', 'MAXMWOUT',
                                         'LOSSCONSTANT', 'LOSSFLOWCOEFFICIENT', 'FROMREGIONLOSSSHARE', 'ICTYPE']
        }

        query_to_execute = queries_by_table[table_name].format(table=table_name, datetime=applicable_for,
                                                               id='INTERCONNECTORID')
        return pd.read_sql_query(query_to_execute, con=self.con)



hi = DBManager('historical_inputs.db')
hi.create_table('LOSSFACTORMODEL')
hi.add_data('LOSSFACTORMODEL', 2019, 1)
out = hi.get_historical_inputs('LOSSFACTORMODEL', '2019/01/19 00:00:00')
#
# t0 = time()
# historical_inputs = DBManager('historical_inputs.db')
# print(time()-t0)
# t0 = time()
# historical_inputs.create_table('DISPATCHCONSTRAINT')
# print(time()-t0)
# t0 = time()
# for i in range(1, 2):
#     historical_inputs.add_data('DISPATCHCONSTRAINT', 2019, i)
# print(time()-t0)
# t0 = time()
# for i in range(0, 2):
#     out = historical_inputs.get_historical_inputs('DISPATCHCONSTRAINT', '2019/{}/19 00:00:00'.format(str(i).zfill(2)))
# print((time()-t0)/12)
# t0 = time()
