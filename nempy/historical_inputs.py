import requests
import zipfile
import io
import pandas as pd
import sqlite3
from time import time


class DBManager:
    def __init__(self, db):
        self.con = sqlite3.connect(db)
        self.base_url = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip'
        self.table_columns = {
            'DISPATCHLOAD': ['SETTLEMENTDATE', 'DUID', 'AVAILABILITY', 'DISPATCHMODE', 'AGCSTATUS', 'INITIALMW',
                             'RAMPDOWNRATE', 'RAMPUPRATE'],
            'DISPATCHCONSTRAINT': ['SETTLEMENTDATE', 'CONSTRAINTID', 'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO',
                                   'RHS'],
            'GENCONDATA': ['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'CONSTRAINTTYPE', 'CONSTRAINTVALUE',
                           'GENERICCONSTRAINTWEIGHT'],
            'DUDETAILSUMMARY': ['DUID', 'START_DATE', 'END_DATE', 'DISPATCHTYPE', 'CONNECTIONPOINTID', 'REGIONID',
                                'LASTCHANGED', 'TRANSMISSIONLOSSFACTOR', 'STARTTYPE', 'DISTRIBUTIONLOSSFACTOR'],

        }
        self.table_primary_keys = {
            'DISPATCHLOAD': ['SETTLEMENTDATE', 'DUID'],
            'DISPATCHCONSTRAINT': ['SETTLEMENTDATE', 'CONSTRAINTID']
        }
        self.column_data_types = {
            'SETTLEMENTDATE': 'TEXT',
            'DUID': 'TEXT',
            'AVAILABILITY': 'REAL',
            'CONSTRAINTID': 'TEXT',
            'RHS': 'REAL'
        }

    def download_to_df(self, table_name, year, month):
        r = requests.get(self.base_url.format(table=table_name, year=year, month=str(month).zfill(2)))
        zf = zipfile.ZipFile(io.BytesIO(r.content))
        file_name = zf.namelist()[0]
        data = pd.read_csv(zf.open(file_name), skiprows=1)
        return data

    def create_table(self, table_name):
        with self.con:
            cur = self.con.cursor()
            cur.execute("""DROP TABLE IF EXISTS {};""".format(table_name))
            base_create_query = """CREATE TABLE {}({}, PRIMARY KEY ({}));"""
            columns = ','.join(
                ['{} {}'.format(col, self.column_data_types[col]) for col in self.table_columns[table_name]])
            primary_keys = ','.join(['{}'.format(col) for col in self.table_primary_keys[table_name]])
            create_query = base_create_query.format(table_name, columns, primary_keys)
            cur.execute(create_query)
            self.con.commit()

    def add_data(self, table_name, year, month):
        data = self.download_to_df(table_name, year, month)
        data = data[data['INTERVENTION'] == 0]
        data = data.loc[:, self.table_columns[table_name]]
        with self.con:
            data.to_sql(table_name, con=self.con, if_exists='append', index=False)
            self.con.commit()

    def get_dispatch_interval_data(self, table_name, date_time):
        interval_data_query = "Select * from {} where SETTLEMENTDATE=='{}'".format(table_name, date_time)
        return pd.read_sql_query(interval_data_query, con=self.con)


t0 = time()
historical_inputs = DBManager('historical_inputs.db')
print(time()-t0)
t0 = time()
historical_inputs.create_table('DISPATCHCONSTRAINT')
print(time()-t0)
t0 = time()
for i in range(1, 13):
    historical_inputs.add_data('DISPATCHCONSTRAINT', 2019, i)
print(time()-t0)
t0 = time()
for i in range(0, 13):
    out = historical_inputs.get_dispatch_interval_data('DISPATCHCONSTRAINT', '2019/{}/19 00:00:00'.format(str(i).zfill(2)))
print((time()-t0)/12)
t0 = time()
