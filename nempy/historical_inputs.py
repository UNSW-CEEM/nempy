import requests
import zipfile
import io
import pandas as pd
import sqlite3
from nempy import check
from datetime import datetime, timedelta
from time import time


def download_to_df(url, table_name, year, month):
    """Downloads a zipped csv file and converts it to a pandas DataFrame, returns the DataFrame.

    Examples
    --------
    This will only work if you are connected to the internet.

    >>> url = ('http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' +
    ...        'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip')

    >>> table_name = 'DISPATCHREGIONSUM'

    >>> df = download_to_df(url, table_name='DISPATCHREGIONSUM', year=2020, month=1)

    >>> print(df)
           I       DISPATCH  ... SEMISCHEDULE_CLEAREDMW  SEMISCHEDULE_COMPLIANCEMW
    0      D       DISPATCH  ...              549.30600                    0.00000
    1      D       DISPATCH  ...              102.00700                    0.00000
    2      D       DISPATCH  ...              387.40700                    0.00000
    3      D       DISPATCH  ...              145.43200                    0.00000
    4      D       DISPATCH  ...              136.85200                    0.00000
    ...   ..            ...  ...                    ...                        ...
    45381  D       DISPATCH  ...              142.71600                    0.00000
    45382  D       DISPATCH  ...              310.28903                    0.36103
    45383  D       DISPATCH  ...               83.94100                    0.00000
    45384  D       DISPATCH  ...              196.69610                    0.69010
    45385  C  END OF REPORT  ...                    NaN                        NaN
    <BLANKLINE>
    [45386 rows x 109 columns]

    Parameters
    ----------
    url : str
        A url of the format 'PUBLIC_DVD_{table}_{year}{month}010000.zip', typically this will be a location on AEMO's
        nemweb portal where data is stored in monthly archives.

    table_name : str
        The name of the table you want to download from nemweb.

    year : int
        The year the table is from.

    month : int
        The month the table is form.

    Returns
    -------
    pd.DataFrame

    Raises
    ------
    MissingData
        If internet connection is down, nemweb is down or data requested is not on nemweb.

    """
    # Insert the table_name, year and month into the url.
    url = url.format(table=table_name, year=year, month=str(month).zfill(2))
    # Download the file.
    r = requests.get(url)
    if r.status_code != 200:
        raise MissingData(('Requested data for table: {}, year: {}, month: {} not downloaded.' +
                           '\nPlease check your internet connection. Also check' 
                           '\nhttp://nemweb.com.au/#mms-data-model, to see if your requested' +
                           '\ndata is uploaded.').format(table_name, year, month))
    # Convert the contents of the response into a zipfile object.
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    # Get the name of the file inside the zip object, assuming only one file is zipped inside.
    file_name = zf.namelist()[0]
    # Read the file into a DataFrame.
    data = pd.read_csv(zf.open(file_name), skiprows=1)
    return data


class MissingData(Exception):
    """Raise for nemweb not returning status 200 for file request."""


class MMSTable:
    """Manages Market Management System (MMS) tables stored in an sqlite database.

    This class creates the table in the data base when the object is instantiated. Methods for adding adding and
    retrieving data are added by sub classing.
    """
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        """Creates a table in sqlite database that the connection is provided for.

        Examples
        --------
        This class is designed to be used after subclassing, however this is how it would be used on it own.

        >>> connection = sqlite3.connect('the_database.db')

        >>> table = MMSTable(table_name='a_table', table_columns=['col_1', 'col_2'], table_primary_keys=['col_1'],
        ...                  con=connection)

        Parameters
        ----------
        table_name : str
            Name of the table.
        table_columns : list(str)
            List of table column names.
        table_primary_keys : list(str)
            Table columns to use as primary keys.
        con : sqlite3.Connection
            Connection to an existing database.
        """
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        # url that sub classes will use to pull MMS tables from nemweb.
        self.url = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' + \
                   'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip'
        with con:
            cur = con.cursor()
            cur.execute("""DROP TABLE IF EXISTS {};""".format(table_name))
            base_create_query = """CREATE TABLE {}({}, PRIMARY KEY ({}));"""
            columns = ','.join(['{} TEXT'.format(col) for col in self.table_columns])
            primary_keys = ','.join(['{}'.format(col) for col in self.table_primary_keys])
            create_query = base_create_query.format(table_name, columns, primary_keys)
            cur.execute(create_query)
            con.commit()


class SingleDataSource(MMSTable):
    """Manages downloading data from nemweb for tables where all relevant data is stored in lasted data file."""
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def set_data(self, year, month, con):
        """"Download data for the given table and time, replace any existing data.

        Examples
        --------
        This class is designed to be used after subclassing, however this is how it would be used on it own. This
        example will only work with an internet connection.

        >>> connection = sqlite3.connect('the_database.db')

        >>> table = SingleDataSource(table_name='DUDETAILSUMMARY',
        ...                          table_columns=['DUID', 'START_DATE', 'CONNECTIONPOINTID', 'REGIONID'],
        ...                          table_primary_keys=['START_DATE', 'DUID'], con=connection)

        >>> table.set_data(year=2020, month=1, con=connection)

        Now the database should contain data for this table that is up to date as the end of Janurary.

        >>> query = "Select * from DUDETAILSUMMARY order by START_DATE DESC limit 1;"

        >>> print(pd.read_sql_query(query, con=connection))
                SETTLEMENTDATE REGIONID  TOTALDEMAND  DEMANDFORECAST  INITIALSUPPLY
        0  2020/01/02 00:00:00     NSW1       6962.0       -34.85254      7021.4668

        However if we subsequently set data from a previous date then any existing data will be replaced

        >>> table.set_data(year=2020, month=1, con=connection)

        >>> print(pd.read_sql_query(query, con=connection))
        SETTLEMENTDATE REGIONID  TOTALDEMAND  DEMANDFORECAST  INITIALSUPPLY
        0  2020/01/02 00:00:00     NSW1       6962.0       -34.85254      7021.4668

        Parameters
        ----------
        year : int
            The year to download data for.
        month : int
            The month to download data for.
        con : sqlite3.Connection
            Connection to a database in which the given table already exists.

        Return
        ------
        None
        """
        data = download_to_df(self.url, self.table_name, year, month)
        data = data.loc[:, self.table_columns]
        with con:
            data.to_sql(self.table_name, con=con, if_exists='replace', index=False)
            con.commit()


class MultiDataSource(MMSTable):
    def __init__(self, table_name, table_columns, table_primary_keys):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys)

    @check.table_exists()
    def add_data(self, table_name, year, month, con):
        data = download_to_df(self.url, table_name, year, month)
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
            table_name='DISPATCHREGIONSUM', table_columns=['SETTLEMENTDATE', 'REGIONID', 'TOTALDEMAND',
                                                           'DEMANDFORECAST', 'INITIALSUPPLY'],
            table_primary_keys=['SETTLEMENTDATE', 'REGIONID'])
        self.DISPATCHLOAD = InputsBySettlementDate(
            table_name='DISPATCHLOAD', table_columns=['SETTLEMENTDATE', 'DUID', 'DISPATCHMODE', 'AGCSTATUS',
                                                      'INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE', 'RAMPUPRATE',
                                                      'AVAILABILITY', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN',
                                                      'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID'])
        self.DUDETAILSUMMARY = InputsBySettlementDate(
            table_name='DUDETAILSUMMARY', table_columns=['DUID', 'START_DATE', 'END_DATE', 'DISPATCHTYPE',
                                                         'CONNECTIONPOINTID', 'REGIONID', 'STATIONID',
                                                         'LASTCHANGED', 'TRANSMISSIONLOSSFACTOR',
                                                         'DISTRIBUTIONLOSSFACTOR'],
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



