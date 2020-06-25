import requests
import zipfile
import io
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import os
import numpy as np

from nempy.help_functions import helper_functions as hf


def _download_to_df(url, table_name, year, month):
    """Downloads a zipped csv file and converts it to a pandas DataFrame, returns the DataFrame.

    Examples
    --------
    This will only work if you are connected to the internet.

    >>> url = ('http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' +
    ...        'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip')

    >>> table_name = 'DISPATCHREGIONSUM'

    >>> df = _download_to_df(url, table_name='DISPATCHREGIONSUM', year=2020, month=1)

    >>> print(df)
           I  DISPATCH  ... SEMISCHEDULE_CLEAREDMW  SEMISCHEDULE_COMPLIANCEMW
    0      D  DISPATCH  ...              549.30600                    0.00000
    1      D  DISPATCH  ...              102.00700                    0.00000
    2      D  DISPATCH  ...              387.40700                    0.00000
    3      D  DISPATCH  ...              145.43200                    0.00000
    4      D  DISPATCH  ...              136.85200                    0.00000
    ...   ..       ...  ...                    ...                        ...
    45380  D  DISPATCH  ...              757.47600                    0.00000
    45381  D  DISPATCH  ...              142.71600                    0.00000
    45382  D  DISPATCH  ...              310.28903                    0.36103
    45383  D  DISPATCH  ...               83.94100                    0.00000
    45384  D  DISPATCH  ...              196.69610                    0.69010
    <BLANKLINE>
    [45385 rows x 109 columns]

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
        raise _MissingData(("""Requested data for table: {}, year: {}, month: {} 
                              not downloaded. Please check your internet connection. Also check
                              http://nemweb.com.au/#mms-data-model, to see if your requested
                              data is uploaded.""").format(table_name, year, month))
    # Convert the contents of the response into a zipfile object.
    zf = zipfile.ZipFile(io.BytesIO(r.content))
    # Get the name of the file inside the zip object, assuming only one file is zipped inside.
    file_name = zf.namelist()[0]
    # Read the file into a DataFrame.
    data = pd.read_csv(zf.open(file_name), skiprows=1)
    # Discard last row of DataFrame
    data = data[:-1]
    return data


class _MissingData(Exception):
    """Raise for nemweb not returning status 200 for file request."""


class _MMSTable:
    """Manages Market Management System (MMS) tables stored in an sqlite database.

    This class creates the table in the data base when the object is instantiated. Methods for adding adding and
    retrieving data are added by sub classing.
    """

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        """Creates a table in sqlite database that the connection is provided for.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = _MMSTable(table_name='a_table', table_columns=['col_1', 'col_2'], table_primary_keys=['col_1'],
        ...                  con=con)

        Clean up by deleting database created.

        >>> con.close()
        >>> os.remove('historical.db')

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
        self.con = con
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        # url that sub classes will use to pull MMS tables from nemweb.
        self.url = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' + \
                   'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip'
        self.columns_types = {
            'INTERVAL_DATETIME': 'TEXT', 'DUID': 'TEXT', 'BIDTYPE': 'TEXT', 'BANDAVAIL1': 'REAL', 'BANDAVAIL2': 'REAL',
            'BANDAVAIL3': 'REAL', 'BANDAVAIL4': 'REAL', 'BANDAVAIL5': 'REAL', 'BANDAVAIL6': 'REAL',
            'BANDAVAIL7': 'REAL', 'BANDAVAIL8': 'REAL', 'BANDAVAIL9': 'REAL', 'BANDAVAIL10': 'REAL', 'MAXAVAIL': 'REAL',
            'ENABLEMENTMIN': 'REAL', 'ENABLEMENTMAX': 'REAL', 'LOWBREAKPOINT': 'REAL', 'HIGHBREAKPOINT': 'REAL',
            'SETTLEMENTDATE': 'TEXT', 'PRICEBAND1': 'REAL', 'PRICEBAND2': 'REAL', 'PRICEBAND3': 'REAL',
            'PRICEBAND4': 'REAL', 'PRICEBAND5': 'REAL', 'PRICEBAND6': 'REAL', 'PRICEBAND7': 'REAL',
            'PRICEBAND8': 'REAL', 'PRICEBAND9': 'REAL', 'PRICEBAND10': 'REAL', 'T1': 'REAL', 'T2': 'REAL',
            'T3': 'REAL', 'T4': 'REAL', 'REGIONID': 'TEXT', 'TOTALDEMAND': 'REAL', 'DEMANDFORECAST': 'REAL',
            'INITIALSUPPLY': 'REAL', 'DISPATCHMODE': 'TEXT', 'AGCSTATUS': 'TEXT', 'INITIALMW': 'REAL',
            'TOTALCLEARED': 'REAL', 'RAMPDOWNRATE': 'REAL', 'RAMPUPRATE': 'REAL', 'AVAILABILITY': 'REAL',
            'RAISEREGENABLEMENTMAX': 'REAL', 'RAISEREGENABLEMENTMIN': 'REAL', 'LOWERREGENABLEMENTMAX': 'REAL',
            'LOWERREGENABLEMENTMIN': 'REAL', 'START_DATE': 'TEXT', 'END_DATE': 'TEXT', 'DISPATCHTYPE': 'TEXT',
            'CONNECTIONPOINTID': 'TEXT', 'TRANSMISSIONLOSSFACTOR': 'REAL', 'DISTRIBUTIONLOSSFACTOR': 'REAL',
            'CONSTRAINTID': 'TEXT', 'RHS': 'REAL', 'GENCONID_EFFECTIVEDATE': 'TEXT', 'GENCONID_VERSIONNO': 'TEXT',
            'GENCONID': 'TEXT', 'EFFECTIVEDATE': 'TEXT', 'VERSIONNO': 'TEXT', 'CONSTRAINTTYPE': 'TEXT',
            'GENERICCONSTRAINTWEIGHT': 'REAL', 'FACTOR': 'REAL', 'FROMREGIONLOSSSHARE': 'REAL', 'LOSSCONSTANT': 'REAL',
            'LOSSFLOWCOEFFICIENT': 'REAL', 'IMPORTLIMIT': 'REAL', 'EXPORTLIMIT': 'REAL', 'LOSSSEGMENT': 'TEXT',
            'MWBREAKPOINT': 'REAL', 'DEMANDCOEFFICIENT': 'REAL', 'INTERCONNECTORID': 'TEXT', 'REGIONFROM': 'TEXT',
            'REGIONTO': 'TEXT', 'MWFLOW': 'REAL', 'MWLOSSES': 'REAL', 'MINIMUMLOAD': 'REAL', 'MAXCAPACITY': 'REAL',
            'SEMIDISPATCHCAP': 'REAL', 'RRP': 'REAL', 'SCHEDULE_TYPE': 'TEXT', 'LOWER5MIN': 'REAL',
            'LOWER60SEC': 'REAL', 'LOWER6SEC': 'REAL', 'RAISE5MIN': 'REAL', 'RAISE60SEC': 'REAL', 'RAISE6SEC': 'REAL',
            'LOWERREG': 'REAL', 'RAISEREG': 'REAL', 'RAISEREGAVAILABILITY': 'REAL',
            'RAISE6SECACTUALAVAILABILITY': 'REAL', 'RAISE60SECACTUALAVAILABILITY': 'REAL',
            'RAISE5MINACTUALAVAILABILITY': 'REAL', 'RAISEREGACTUALAVAILABILITY': 'REAL',
            'LOWER6SECACTUALAVAILABILITY': 'REAL', 'LOWER60SECACTUALAVAILABILITY': 'REAL',
            'LOWER5MINACTUALAVAILABILITY': 'REAL', 'LOWERREGACTUALAVAILABILITY': 'REAL', 'LHS': 'REAL',
            'VIOLATIONDEGREE': 'REAL', 'MARGINALVALUE': 'REAL', 'RAISE6SECRRP': 'REAL',
            'RAISE60SECRRP': 'REAL', 'RAISE5MINRRP': 'REAL', 'RAISEREGRRP': 'REAL', 'LOWER6SECRRP': 'REAL',
            'LOWER60SECRRP': 'REAL', 'LOWER5MINRRP': 'REAL', 'LOWERREGRRP': 'REAL', 'FROM_REGION_TLF': 'REAL',
            'TO_REGION_TLF': 'REAL', 'ICTYPE': 'TEXT', 'LINKID': 'TEXT', 'FROMREGION': 'TEXT', 'TOREGION': 'TEXT'
        }

    def create_table_in_sqlite_db(self):
        """Creates a table in the sqlite database that the object has a connection to.

        Note
        ----
        This method and its documentation is inherited from the _MMSTable class.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = _MMSTable(table_name='EXAMPLE', table_columns=['DUID', 'BIDTYPE'], table_primary_keys=['DUID'],
        ...                  con=con)

        Create the corresponding table in the sqlite database, note this step many not be needed if you have connected
        to an existing database.

        >>> table.create_table_in_sqlite_db()

        Now a table exists in the database, but its empty.

        >>> print(pd.read_sql("Select * from example", con=con))
        Empty DataFrame
        Columns: [DUID, BIDTYPE]
        Index: []

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        """
        with self.con:
            cur = self.con.cursor()
            cur.execute("""DROP TABLE IF EXISTS {};""".format(self.table_name))
            base_create_query = """CREATE TABLE {}({}, PRIMARY KEY ({}));"""
            columns = ','.join(['{} {}'.format(col, self.columns_types[col]) for col in self.table_columns])
            primary_keys = ','.join(['{}'.format(col) for col in self.table_primary_keys])
            create_query = base_create_query.format(self.table_name, columns, primary_keys)
            cur.execute(create_query)
            self.con.commit()


class _SingleDataSource(_MMSTable):
    """Manages downloading data from nemweb for tables where all relevant data is stored in lasted data file."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def set_data(self, year, month):
        """"Download data for the given table and time, replace any existing data.

        Note
        ----
        This method and its documentation is inherited from the _SingleDataSource class.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = _SingleDataSource(table_name='DUDETAILSUMMARY',
        ...                          table_columns=['DUID', 'START_DATE', 'CONNECTIONPOINTID', 'REGIONID'],
        ...                          table_primary_keys=['START_DATE', 'DUID'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Downloading data from http://nemweb.com.au/#mms-data-model into the table.

        >>> table.set_data(year=2020, month=1)

        Now the database should contain data for this table that is up to date as the end of Janurary.

        >>> query = "Select * from DUDETAILSUMMARY order by START_DATE DESC limit 1;"

        >>> print(pd.read_sql_query(query, con=con))
              DUID           START_DATE CONNECTIONPOINTID REGIONID
        0  URANQ11  2020/02/04 00:00:00            NURQ1U     NSW1

        However if we subsequently set data from a previous date then any existing data will be replaced. Note the
        change in the most recent record in the data set below.

        >>> table.set_data(year=2019, month=1)

        >>> print(pd.read_sql_query(query, con=con))
               DUID           START_DATE CONNECTIONPOINTID REGIONID
        0  WEMENSF1  2019/03/04 00:00:00            VWES2W     VIC1

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        year : int
            The year to download data for.
        month : int
            The month to download data for.

        Return
        ------
        None
        """
        data = _download_to_df(self.url, self.table_name, year, month)
        data = data.loc[:, self.table_columns]
        with self.con:
            data.to_sql(self.table_name, con=self.con, if_exists='replace', index=False)
            self.con.commit()


class _MultiDataSource(_MMSTable):
    """Manages downloading data from nemweb for tables where data main be stored across multiple monthly files."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def add_data(self, year, month):
        """"Download data for the given table and time, appends to any existing data.

        Note
        ----
        This method and its documentation is inherited from the _MultiDataSource class.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = _MultiDataSource(table_name='DISPATCHLOAD',
        ...                          table_columns=['SETTLEMENTDATE', 'DUID',  'RAMPDOWNRATE', 'RAMPUPRATE'],
        ...                          table_primary_keys=['SETTLEMENTDATE', 'DUID'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Downloading data from http://nemweb.com.au/#mms-data-model into the table.

        >>> table.add_data(year=2020, month=1)

        Now the database should contain data for this table that is up to date as the end of Janurary.

        >>> query = "Select * from DISPATCHLOAD order by SETTLEMENTDATE DESC limit 1;"

        >>> print(pd.read_sql_query(query, con=con))
                SETTLEMENTDATE   DUID  RAMPDOWNRATE  RAMPUPRATE
        0  2020/02/01 00:00:00  YWPS4         180.0       180.0

        If we subsequently add data from an earlier month the old data remains in the table, in addition to the new
        data.

        >>> table.add_data(year=2019, month=1)

        >>> print(pd.read_sql_query(query, con=con))
                SETTLEMENTDATE   DUID  RAMPDOWNRATE  RAMPUPRATE
        0  2020/02/01 00:00:00  YWPS4         180.0       180.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        year : int
            The year to download data for.
        month : int
            The month to download data for.

        Return
        ------
        None
        """
        data = _download_to_df(self.url, self.table_name, year, month)
        if 'INTERVENTION' in data.columns:
            data = data[data['INTERVENTION'] == 0]
        data = data.loc[:, self.table_columns]
        with self.con:
            data.to_sql(self.table_name, con=self.con, if_exists='append', index=False)
            self.con.commit()


class _AllHistDataSource(_MMSTable):
    """Manages downloading data from nemweb for tables where relevant data could be stored in any previous monthly file.
    """

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def set_data(self, year, month):
        """"Download data for the given table and time, replace any existing data.

        Note
        ----
        This method and its documentation is inherited from the _SingleDataSource class.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = _SingleDataSource(table_name='DUDETAILSUMMARY',
        ...                          table_columns=['DUID', 'START_DATE', 'CONNECTIONPOINTID', 'REGIONID'],
        ...                          table_primary_keys=['START_DATE', 'DUID'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Downloading data from http://nemweb.com.au/#mms-data-model into the table.

        >>> table.set_data(year=2020, month=1)

        Now the database should contain data for this table that is up to date as the end of Janurary.

        >>> query = "Select * from DUDETAILSUMMARY order by START_DATE DESC limit 1;"

        >>> print(pd.read_sql_query(query, con=con))
              DUID           START_DATE CONNECTIONPOINTID REGIONID
        0  URANQ11  2020/02/04 00:00:00            NURQ1U     NSW1

        However if we subsequently set data from a previous date then any existing data will be replaced. Note the
        change in the most recent record in the data set below.

        >>> table.set_data(year=2019, month=1)

        >>> print(pd.read_sql_query(query, con=con))
               DUID           START_DATE CONNECTIONPOINTID REGIONID
        0  WEMENSF1  2019/03/04 00:00:00            VWES2W     VIC1

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        year : int
            The year to download data for.
        month : int
            The month to download data for.

        Return
        ------
        None
        """
        for y in range(year, 2009, -1):
            for m in range(12, 0, -1):
                if y == year and m > month:
                    continue
                try:
                    data = _download_to_df(self.url, self.table_name, y, m)
                    if not set(self.table_columns) < set(data.columns):
                        continue
                    data = data.loc[:, self.table_columns]
                    with self.con:
                        if y == year and m == month:
                            data.to_sql(self.table_name, con=self.con, if_exists='replace', index=False)
                            cumulative_data = data.loc[:, self.table_primary_keys]
                        else:
                            # Filter data to only include rows unique to the new data and not in data
                            # previously downloaded.
                            data = pd.merge(data, cumulative_data, 'outer', on=self.table_primary_keys, indicator=True)
                            data = data[data['_merge'] == 'left_only'].drop('_merge', axis=1)
                            # Insert data.
                            data.to_sql(self.table_name, con=self.con, if_exists='append', index=False)
                            cumulative_data = pd.concat([cumulative_data, data.loc[:, self.table_primary_keys]])
                        self.con.commit()
                except _MissingData:
                    pass


class InputsBySettlementDate(_MultiDataSource):
    """Manages retrieving dispatch inputs by SETTLEMENTDATE."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time e.g. 2019/01/01 11:55:00"

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsBySettlementDate(table_name='EXAMPLE', table_columns=['SETTLEMENTDATE', 'INITIALMW'],
        ...                                table_primary_keys=['SETTLEMENTDATE'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the add_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'SETTLEMENTDATE': ['2019/01/01 11:55:00', '2019/01/01 12:00:00'],
        ...   'INITIALMW': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by SETTLEMENTDATE.

        >>> print(table.get_data(date_time='2019/01/01 12:00:00'))
                SETTLEMENTDATE  INITIALMW
        0  2019/01/01 12:00:00        2.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame

        """
        query = "Select * from {table} where SETTLEMENTDATE == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=self.con)


class InputsByIntervalDateTime(_MultiDataSource):
    """Manages retrieving dispatch inputs by INTERVAL_DATETIME."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time e.g. 2019/01/01 11:55:00"

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsByIntervalDateTime(table_name='EXAMPLE', table_columns=['INTERVAL_DATETIME', 'INITIALMW'],
        ...                                  table_primary_keys=['INTERVAL_DATETIME'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the add_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'INTERVAL_DATETIME': ['2019/01/01 11:55:00', '2019/01/01 12:00:00'],
        ...   'INITIALMW': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by INTERVAL_DATETIME.

        >>> print(table.get_data(date_time='2019/01/01 12:00:00'))
             INTERVAL_DATETIME  INITIALMW
        0  2019/01/01 12:00:00        2.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame

        """
        query = "Select * from {table} where INTERVAL_DATETIME == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=self.con)


class InputsByDay(_MultiDataSource):
    """Manages retrieving dispatch inputs by SETTLEMENTDATE, where inputs are stored on a daily basis."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time e.g. 2019/01/01 11:55:00, where inputs are stored on daily basis.

        Note that a market day begins with the first 5 min interval as 04:05:00, there for if and input date_time of
        2019/01/01 04:05:00 is given inputs where the SETTLEMENDATE is 2019/01/01 00:00:00 will be retrieved and if
        a date_time of 2019/01/01 04:00:00 or earlier is given then inputs where the SETTLEMENDATE is
        2018/12/31 00:00:00 will be retrieved.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsByDay(table_name='EXAMPLE', table_columns=['SETTLEMENTDATE', 'INITIALMW'],
        ...                     table_primary_keys=['SETTLEMENTDATE'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the add_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'SETTLEMENTDATE': ['2019/01/01 00:00:00', '2019/01/02 00:00:00'],
        ...   'INITIALMW': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by SETTLEMENTDATE and the results from the appropriate market
        day starting at 04:05:00 are retrieved. In the results below note when the output changes

        >>> print(table.get_data(date_time='2019/01/01 12:00:00'))
                SETTLEMENTDATE  INITIALMW
        0  2019/01/01 00:00:00        1.0

        >>> print(table.get_data(date_time='2019/01/02 04:00:00'))
                SETTLEMENTDATE  INITIALMW
        0  2019/01/01 00:00:00        1.0

        >>> print(table.get_data(date_time='2019/01/02 04:05:00'))
                SETTLEMENTDATE  INITIALMW
        0  2019/01/02 00:00:00        2.0

        >>> print(table.get_data(date_time='2019/01/02 12:00:00'))
                SETTLEMENTDATE  INITIALMW
        0  2019/01/02 00:00:00        2.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame
        """

        # Convert to datetime object
        date_time = datetime.strptime(date_time, '%Y/%m/%d %H:%M:%S')
        # Change date_time provided so any time less than 04:05:00 will have the previous days date.
        date_time = date_time - timedelta(hours=4, seconds=1)
        # Convert back to string.
        date_time = datetime.isoformat(date_time).replace('-', '/').replace('T', ' ')
        # Remove the time component.
        date_time = date_time[:10]
        date_padding = ' 00:00:00'
        date_time = date_time + date_padding
        query = "Select * from {table} where SETTLEMENTDATE == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=self.con)


class InputsStartAndEnd(_SingleDataSource):
    """Manages retrieving dispatch inputs by START_DATE and END_DATE."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time by START_DATE and END_DATE.

        Records with a START_DATE before or equal to the date_times and an END_DATE after the date_time will be
        returned.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsStartAndEnd(table_name='EXAMPLE', table_columns=['START_DATE', 'END_DATE', 'INITIALMW'],
        ...                           table_primary_keys=['START_DATE'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the add_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'START_DATE': ['2019/01/01 00:00:00', '2019/01/02 00:00:00'],
        ...   'END_DATE': ['2019/01/02 00:00:00', '2019/01/03 00:00:00'],
        ...   'INITIALMW': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by START_DATE and END_DATE.

        >>> print(table.get_data(date_time='2019/01/01 00:00:00'))
                    START_DATE             END_DATE  INITIALMW
        0  2019/01/01 00:00:00  2019/01/02 00:00:00        1.0

        >>> print(table.get_data(date_time='2019/01/01 12:00:00'))
                    START_DATE             END_DATE  INITIALMW
        0  2019/01/01 00:00:00  2019/01/02 00:00:00        1.0

        >>> print(table.get_data(date_time='2019/01/02 00:00:00'))
                    START_DATE             END_DATE  INITIALMW
        0  2019/01/02 00:00:00  2019/01/03 00:00:00        2.0

        >>> print(table.get_data(date_time='2019/01/02 00:12:00'))
                    START_DATE             END_DATE  INITIALMW
        0  2019/01/02 00:00:00  2019/01/03 00:00:00        2.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame
        """

        query = "Select * from {table} where START_DATE <= '{datetime}' and END_DATE > '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=self.con)


class InputsByMatchDispatchConstraints(_AllHistDataSource):
    """Manages retrieving dispatch inputs by matching against the DISPATCHCONSTRAINTS table"""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time by matching against the DISPATCHCONSTRAINT table.

        First the DISPATCHCONSTRAINT table is filtered by SETTLEMENTDATE and then the contents of the classes table
        is matched against that.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsByMatchDispatchConstraints(table_name='EXAMPLE',
        ...                           table_columns=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'RHS'],
        ...                           table_primary_keys=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the set_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'GENCONID': ['X', 'X', 'Y', 'Y'],
        ...   'EFFECTIVEDATE': ['2019/01/02 00:00:00', '2019/01/03 00:00:00', '2019/01/01 00:00:00',
        ...                     '2019/01/03 00:00:00'],
        ...   'VERSIONNO': [1, 2, 2, 3],
        ...   'RHS': [1.0, 2.0, 2.0, 3.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        >>> data = pd.DataFrame({
        ...   'SETTLEMENTDATE' : ['2019/01/02 00:00:00', '2019/01/02 00:00:00', '2019/01/03 00:00:00',
        ...                       '2019/01/03 00:00:00'],
        ...   'CONSTRAINTID': ['X', 'Y', 'X', 'Y'],
        ...   'GENCONID_EFFECTIVEDATE': ['2019/01/02 00:00:00', '2019/01/01 00:00:00', '2019/01/03 00:00:00',
        ...                              '2019/01/03 00:00:00'],
        ...   'GENCONID_VERSIONNO': [1, 2, 2, 3]})

        >>> data.to_sql('DISPATCHCONSTRAINT', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by the contents of DISPATCHCONSTRAINT.

        >>> print(table.get_data(date_time='2019/01/02 00:00:00'))
          GENCONID        EFFECTIVEDATE VERSIONNO  RHS
        0        X  2019/01/02 00:00:00         1  1.0
        1        Y  2019/01/01 00:00:00         2  2.0

        >>> print(table.get_data(date_time='2019/01/03 00:00:00'))
          GENCONID        EFFECTIVEDATE VERSIONNO  RHS
        0        X  2019/01/03 00:00:00         2  2.0
        1        Y  2019/01/03 00:00:00         3  3.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame
        """
        columns = ','.join(['{}'.format(col) for col in self.table_columns])
        query = """Select {columns} from (
                        {table} 
                    inner join 
                        (Select * from DISPATCHCONSTRAINT where SETTLEMENTDATE == '{datetime}')
                    on GENCONID == CONSTRAINTID
                    and EFFECTIVEDATE == GENCONID_EFFECTIVEDATE
                    and VERSIONNO == GENCONID_VERSIONNO);"""
        query = query.format(columns=columns, table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=self.con)


class InputsByEffectiveDateVersionNoAndDispatchInterconnector(_SingleDataSource):
    """Manages retrieving dispatch inputs by EFFECTTIVEDATE and VERSIONNO."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time by EFFECTTIVEDATE and VERSIONNO.

        For each unique record (by the remaining primary keys, not including EFFECTTIVEDATE and VERSIONNO) the record
        with the most recent EFFECTIVEDATE

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsByEffectiveDateVersionNoAndDispatchInterconnector(table_name='EXAMPLE',
        ...                           table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'INITIALMW'],
        ...                           table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the set_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'INTERCONNECTORID': ['X', 'X', 'Y', 'Y'],
        ...   'EFFECTIVEDATE': ['2019/01/02 00:00:00', '2019/01/03 00:00:00', '2019/01/01 00:00:00',
        ...                     '2019/01/03 00:00:00'],
        ...   'VERSIONNO': [1, 2, 2, 3],
        ...   'INITIALMW': [1.0, 2.0, 2.0, 3.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        We also need to add data to DISPATCHINTERCONNECTORRES because the results of the get_data method are filtered
        against this table

        >>> data = pd.DataFrame({
        ...   'INTERCONNECTORID': ['X', 'X', 'Y'],
        ...   'SETTLEMENTDATE': ['2019/01/02 00:00:00', '2019/01/03 00:00:00', '2019/01/02 00:00:00']})

        >>> data.to_sql('DISPATCHINTERCONNECTORRES', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by the contents of DISPATCHCONSTRAINT.

        >>> print(table.get_data(date_time='2019/01/02 00:00:00'))
          INTERCONNECTORID        EFFECTIVEDATE VERSIONNO  INITIALMW
        0                X  2019/01/02 00:00:00         1        1.0
        1                Y  2019/01/01 00:00:00         2        2.0

        In the next interval interconnector Y is not present in DISPATCHINTERCONNECTORRES.

        >>> print(table.get_data(date_time='2019/01/03 00:00:00'))
          INTERCONNECTORID        EFFECTIVEDATE VERSIONNO  INITIALMW
        0                X  2019/01/03 00:00:00         2        2.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame
        """
        id_columns = ','.join([col for col in self.table_primary_keys if col not in ['EFFECTIVEDATE', 'VERSIONNO']])
        return_columns = ','.join(self.table_columns)
        with self.con:
            cur = self.con.cursor()
            cur.execute("DROP TABLE IF EXISTS temp;")
            cur.execute("DROP TABLE IF EXISTS temp2;")
            cur.execute("DROP TABLE IF EXISTS temp3;")
            cur.execute("DROP TABLE IF EXISTS temp4;")
            # Store just the unique sets of ids that came into effect before the the datetime in a temporary table.
            query = """CREATE TEMPORARY TABLE temp AS 
                              SELECT * 
                                FROM {table} 
                               WHERE EFFECTIVEDATE <= '{datetime}';"""
            cur.execute(query.format(table=self.table_name, datetime=date_time))
            # For each unique set of ids and effective dates get the latest versionno and sore in temporary table.
            query = """CREATE TEMPORARY TABLE temp2 AS
                              SELECT {id}, EFFECTIVEDATE, MAX(VERSIONNO) AS VERSIONNO
                                FROM temp
                               GROUP BY {id}, EFFECTIVEDATE;"""
            cur.execute(query.format(id=id_columns))
            # For each unique set of ids get the record with the most recent effective date.
            query = """CREATE TEMPORARY TABLE temp3 as
                              SELECT {id}, VERSIONNO, max(EFFECTIVEDATE) as EFFECTIVEDATE
                                FROM temp2
                               GROUP BY {id};"""
            cur.execute(query.format(id=id_columns))
            # Inner join the original table to the set of most recent effective dates and version no.
            query = """CREATE TEMPORARY TABLE temp4 AS
                              SELECT * 
                                FROM {table} 
                                     INNER JOIN temp3 
                                     USING ({id}, VERSIONNO, EFFECTIVEDATE);"""
            cur.execute(query.format(table=self.table_name, id=id_columns))
        # Inner join the most recent data with the interconnectors used in the actual interval of interest.
        query = """SELECT {cols} 
                     FROM temp4 
                          INNER JOIN (SELECT * 
                                        FROM DISPATCHINTERCONNECTORRES 
                                       WHERE SETTLEMENTDATE == '{datetime}') 
                          USING (INTERCONNECTORID);"""
        query = query.format(datetime=date_time, id=id_columns, cols=return_columns)
        data = pd.read_sql_query(query, con=self.con)
        return data


class InputsByEffectiveDateVersionNo(_SingleDataSource):
    """Manages retrieving dispatch inputs by EFFECTTIVEDATE and VERSIONNO."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time by EFFECTTIVEDATE and VERSIONNO.

        For each unique record (by the remaining primary keys, not including EFFECTTIVEDATE and VERSIONNO) the record
        with the most recent EFFECTIVEDATE

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsByEffectiveDateVersionNo(table_name='EXAMPLE',
        ...                           table_columns=['DUID', 'EFFECTIVEDATE', 'VERSIONNO', 'INITIALMW'],
        ...                           table_primary_keys=['DUID', 'EFFECTIVEDATE', 'VERSIONNO'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the set_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'DUID': ['X', 'X', 'Y', 'Y'],
        ...   'EFFECTIVEDATE': ['2019/01/02 00:00:00', '2019/01/03 00:00:00', '2019/01/01 00:00:00',
        ...                     '2019/01/03 00:00:00'],
        ...   'VERSIONNO': [1, 2, 2, 3],
        ...   'INITIALMW': [1.0, 2.0, 2.0, 3.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by most recent effective date and highest version no.

        >>> print(table.get_data(date_time='2019/01/02 00:00:00'))
          DUID        EFFECTIVEDATE VERSIONNO  INITIALMW
        0    X  2019/01/02 00:00:00         1        1.0
        1    Y  2019/01/01 00:00:00         2        2.0

        In the next interval interconnector Y is not present in DISPATCHINTERCONNECTORRES.

        >>> print(table.get_data(date_time='2019/01/03 00:00:00'))
          DUID        EFFECTIVEDATE VERSIONNO  INITIALMW
        0    X  2019/01/03 00:00:00         2        2.0
        1    Y  2019/01/03 00:00:00         3        3.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Parameters
        ----------
        date_time : str
            Should be of format '%Y/%m/%d %H:%M:%S', and always a round 5 min interval e.g. 2019/01/01 11:55:00.

        Returns
        -------
        pd.DataFrame
        """
        id_columns = ','.join([col for col in self.table_primary_keys if col not in ['EFFECTIVEDATE', 'VERSIONNO']])
        return_columns = ','.join(self.table_columns)
        with self.con:
            cur = self.con.cursor()
            cur.execute("DROP TABLE IF EXISTS temp;")
            cur.execute("DROP TABLE IF EXISTS temp2;")
            cur.execute("DROP TABLE IF EXISTS temp3;")
            cur.execute("DROP TABLE IF EXISTS temp4;")
            # Store just the unique sets of ids that came into effect before the the datetime in a temporary table.
            query = """CREATE TEMPORARY TABLE temp AS 
                              SELECT * 
                                FROM {table} 
                               WHERE EFFECTIVEDATE <= '{datetime}';"""
            cur.execute(query.format(table=self.table_name, datetime=date_time))
            # For each unique set of ids and effective dates get the latest versionno and sore in temporary table.
            query = """CREATE TEMPORARY TABLE temp2 AS
                              SELECT {id}, EFFECTIVEDATE, MAX(VERSIONNO) AS VERSIONNO
                                FROM temp
                               GROUP BY {id}, EFFECTIVEDATE;"""
            cur.execute(query.format(id=id_columns))
            # For each unique set of ids get the record with the most recent effective date.
            query = """CREATE TEMPORARY TABLE temp3 as
                              SELECT {id}, VERSIONNO, max(EFFECTIVEDATE) as EFFECTIVEDATE
                                FROM temp2
                               GROUP BY {id};"""
            cur.execute(query.format(id=id_columns))
            # Inner join the original table to the set of most recent effective dates and version no.
            query = """CREATE TEMPORARY TABLE temp4 AS
                              SELECT * 
                                FROM {table} 
                                     INNER JOIN temp3 
                                     USING ({id}, VERSIONNO, EFFECTIVEDATE);"""
            cur.execute(query.format(table=self.table_name, id=id_columns))
        # Inner join the most recent data with the interconnectors used in the actual interval of interest.
        query = """SELECT {cols} FROM temp4 ;"""
        query = query.format(cols=return_columns)
        data = pd.read_sql_query(query, con=self.con)
        return data


class InputsNoFilter(_SingleDataSource):
    """Manages retrieving dispatch inputs where no filter is require."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self):
        """Retrieves all data in the table.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the table object.

        >>> table = InputsNoFilter(table_name='EXAMPLE', table_columns=['DUID', 'INITIALMW'],
        ...                        table_primary_keys=['DUID'], con=con)

        Create the table in the database.

        >>> table.create_table_in_sqlite_db()

        Normally you would use the set_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'DUID': ['X', 'Y'],
        ...   'INITIALMW': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data all data in the table is returned.

        >>> print(table.get_data())
          DUID  INITIALMW
        0    X        1.0
        1    Y        2.0

        Clean up by closing the database and deleting if its no longer needed.

        >>> con.close()
        >>> os.remove('historical.db')

        Returns
        -------
        pd.DataFrame
        """

        return pd.read_sql_query("Select * from {table}".format(table=self.table_name), con=self.con)


class DBManager:
    """Constructs and manages a sqlite database for accessing historical inputs for NEM spot market dispatch.

    Constructs a database if none exists, otherwise connects to an existing database. Specific datasets can be added
    to the database from AEMO nemweb portal and inputs can be retrieved on a 5 min dispatch interval basis.

    Examples
    --------
    Create the database or connect to an existing one.

    >>> con = sqlite3.connect('historical.db')

    Create the database manager.

    >>> historical = DBManager(con)

    Create a set of default table in the database.

    >>> historical.create_tables()

    Add data from AEMO nemweb data portal. In this case we are adding data from the table BIDDAYOFFER_D which contains
    unit's volume bids on 5 min basis, the data comes in monthly chunks.

    >>> historical.BIDDAYOFFER_D.add_data(year=2020, month=1)

    >>> historical.BIDDAYOFFER_D.add_data(year=2020, month=2)

    This table has an add_data method indicating that data provided by AEMO comes in monthly files that do not overlap.
    If you need data for multiple months then multiple add_data calls can be made.

    Data for a specific 5 min dispatch interval can then be retrieved.

    >>> print(historical.BIDDAYOFFER_D.get_data('2020/01/10 12:35:00').head())
            SETTLEMENTDATE     DUID     BIDTYPE  ...    T3   T4  MINIMUMLOAD
    0  2020/01/10 00:00:00   AGLHAL      ENERGY  ...  10.0  2.0          2.0
    1  2020/01/10 00:00:00   AGLSOM      ENERGY  ...  35.0  2.0         16.0
    2  2020/01/10 00:00:00  ANGAST1      ENERGY  ...   0.0  0.0         46.0
    3  2020/01/10 00:00:00    APD01   LOWER5MIN  ...   0.0  0.0          0.0
    4  2020/01/10 00:00:00    APD01  LOWER60SEC  ...   0.0  0.0          0.0
    <BLANKLINE>
    [5 rows x 18 columns]

    Some tables will have a set_data method instead of an add_data method, indicating that the most recent data file
    provided by AEMO contains all historical data for this table. In this case if multiple calls to the set_data method
    are made the new data replaces the old.

    >>> historical.DUDETAILSUMMARY.set_data(year=2020, month=2)

    Data for a specific 5 min dispatch interval can then be retrieved.

    >>> print(historical.DUDETAILSUMMARY.get_data('2020/01/10 12:35:00').head())
           DUID           START_DATE  ... DISTRIBUTIONLOSSFACTOR  SCHEDULE_TYPE
    0    AGLHAL  2019/07/01 00:00:00  ...                 1.0000      SCHEDULED
    1   AGLNOW1  2019/07/01 00:00:00  ...                 1.0000  NON-SCHEDULED
    2  AGLSITA1  2019/07/01 00:00:00  ...                 1.0000  NON-SCHEDULED
    3    AGLSOM  2019/07/01 00:00:00  ...                 0.9891      SCHEDULED
    4   ANGAST1  2019/07/01 00:00:00  ...                 0.9890      SCHEDULED
    <BLANKLINE>
    [5 rows x 9 columns]

    Parameters
    ----------
    con : sqlite3.connection


    Attributes
    ----------
    BIDPEROFFER_D : InputsByIntervalDateTime
        Unit volume bids by 5 min dispatch intervals.
    BIDDAYOFFER_D : InputsByDay
        Unit price bids by market day.
    DISPATCHREGIONSUM : InputsBySettlementDate
        Regional demand terms by 5 min dispatch intervals.
    DISPATCHLOAD : InputsBySettlementDate
        Unit operating conditions by 5 min dispatch intervals.
    DUDETAILSUMMARY : InputsStartAndEnd
        Unit information by the start and end times of when the information is applicable.
    DISPATCHCONSTRAINT : InputsBySettlementDate
        The generic constraints that were used in each 5 min interval dispatch.
    GENCONDATA : InputsByMatchDispatchConstraints
        The generic constraints information, their applicability to a particular dispatch interval is determined by
        reference to DISPATCHCONSTRAINT.
    SPDREGIONCONSTRAINT : InputsByMatchDispatchConstraints
        The regional lhs terms in generic constraints, their applicability to a particular dispatch interval is
        determined by reference to DISPATCHCONSTRAINT.
    SPDCONNECTIONPOINTCONSTRAINT : InputsByMatchDispatchConstraints
        The connection point lhs terms in generic constraints, their applicability to a particular dispatch interval is
        determined by reference to DISPATCHCONSTRAINT.
    SPDINTERCONNECTORCONSTRAINT : InputsByMatchDispatchConstraints
        The interconnector lhs terms in generic constraints, their applicability to a particular dispatch interval is
        determined by reference to DISPATCHCONSTRAINT.
    INTERCONNECTOR : InputsNoFilter
        The the regions that each interconnector links.
    INTERCONNECTORCONSTRAINT : InputsByEffectiveDateAndVersionNo
        Interconnector properties FROMREGIONLOSSSHARE, LOSSCONSTANT, LOSSFLOWCOEFFICIENT, MAXMWIN, MAXMWOUT by
        EFFECTIVEDATE and VERSIONNO.
    LOSSMODEL : InputsByEffectiveDateAndVersionNo
        Break points used in linearly interpolating interconnector loss funtctions by EFFECTIVEDATE and VERSIONNO.
    LOSSFACTORMODEL : InputsByEffectiveDateAndVersionNo
        Coefficients of demand terms in interconnector loss functions.
    DISPATCHINTERCONNECTORRES : InputsBySettlementDate
        Record of which interconnector were used in a particular dispatch interval.

    """

    def __init__(self, connection):
        self.con = connection
        self.BIDPEROFFER_D = InputsByIntervalDateTime(
            table_name='BIDPEROFFER_D', table_columns=['INTERVAL_DATETIME', 'DUID', 'BIDTYPE', 'BANDAVAIL1',
                                                       'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4', 'BANDAVAIL5',
                                                       'BANDAVAIL6', 'BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9',
                                                       'BANDAVAIL10', 'MAXAVAIL', 'ENABLEMENTMIN', 'ENABLEMENTMAX',
                                                       'LOWBREAKPOINT', 'HIGHBREAKPOINT'],
            table_primary_keys=['INTERVAL_DATETIME', 'DUID', 'BIDTYPE'], con=self.con)
        self.BIDDAYOFFER_D = InputsByDay(
            table_name='BIDDAYOFFER_D', table_columns=['SETTLEMENTDATE', 'DUID', 'BIDTYPE', 'PRICEBAND1', 'PRICEBAND2',
                                                       'PRICEBAND3', 'PRICEBAND4', 'PRICEBAND5', 'PRICEBAND6',
                                                       'PRICEBAND7', 'PRICEBAND8', 'PRICEBAND9', 'PRICEBAND10', 'T1',
                                                       'T2', 'T3', 'T4', 'MINIMUMLOAD'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID', 'BIDTYPE'], con=self.con)
        self.DISPATCHREGIONSUM = InputsBySettlementDate(
            table_name='DISPATCHREGIONSUM', table_columns=['SETTLEMENTDATE', 'REGIONID', 'TOTALDEMAND',
                                                           'DEMANDFORECAST', 'INITIALSUPPLY'],
            table_primary_keys=['SETTLEMENTDATE', 'REGIONID'], con=self.con)
        self.DISPATCHLOAD = InputsBySettlementDate(
            table_name='DISPATCHLOAD', table_columns=['SETTLEMENTDATE', 'DUID', 'DISPATCHMODE', 'AGCSTATUS',
                                                      'INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE', 'RAMPUPRATE',
                                                      'AVAILABILITY', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN',
                                                      'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN',
                                                      'SEMIDISPATCHCAP', 'LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC',
                                                      'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC', 'LOWERREG', 'RAISEREG',
                                                      'RAISEREGAVAILABILITY', 'RAISE6SECACTUALAVAILABILITY',
                                                      'RAISE60SECACTUALAVAILABILITY', 'RAISE5MINACTUALAVAILABILITY',
                                                      'RAISEREGACTUALAVAILABILITY', 'LOWER6SECACTUALAVAILABILITY',
                                                      'LOWER60SECACTUALAVAILABILITY', 'LOWER5MINACTUALAVAILABILITY',
                                                      'LOWERREGACTUALAVAILABILITY'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID'], con=self.con)
        self.DISPATCHPRICE = InputsBySettlementDate(
            table_name='DISPATCHPRICE', table_columns=['SETTLEMENTDATE', 'REGIONID', 'RRP', 'RAISE6SECRRP',
                                                       'RAISE60SECRRP', 'RAISE5MINRRP', 'RAISEREGRRP',
                                                       'LOWER6SECRRP', 'LOWER60SECRRP', 'LOWER5MINRRP',
                                                       'LOWERREGRRP'],
            table_primary_keys=['SETTLEMENTDATE', 'REGIONID'], con=self.con)
        self.DUDETAILSUMMARY = InputsStartAndEnd(
            table_name='DUDETAILSUMMARY', table_columns=['DUID', 'START_DATE', 'END_DATE', 'DISPATCHTYPE',
                                                         'CONNECTIONPOINTID', 'REGIONID', 'TRANSMISSIONLOSSFACTOR',
                                                         'DISTRIBUTIONLOSSFACTOR', 'SCHEDULE_TYPE'],
            table_primary_keys=['START_DATE', 'DUID'], con=self.con)
        self.DUDETAIL = InputsByEffectiveDateVersionNo(
            table_name='DUDETAIL', table_columns=['DUID', 'EFFECTIVEDATE', 'VERSIONNO', 'MAXCAPACITY'],
            table_primary_keys=['DUID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.DISPATCHCONSTRAINT = InputsBySettlementDate(
            table_name='DISPATCHCONSTRAINT', table_columns=['SETTLEMENTDATE', 'CONSTRAINTID', 'RHS',
                                                            'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO',
                                                            'LHS', 'VIOLATIONDEGREE', 'MARGINALVALUE'],
            table_primary_keys=['SETTLEMENTDATE', 'CONSTRAINTID'], con=self.con)
        self.GENCONDATA = InputsByMatchDispatchConstraints(
            table_name='GENCONDATA', table_columns=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'CONSTRAINTTYPE',
                                                    'GENERICCONSTRAINTWEIGHT'],
            table_primary_keys=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.SPDREGIONCONSTRAINT = InputsByMatchDispatchConstraints(
            table_name='SPDREGIONCONSTRAINT', table_columns=['REGIONID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID',
                                                             'BIDTYPE', 'FACTOR'],
            table_primary_keys=['REGIONID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'BIDTYPE'], con=self.con)
        self.SPDCONNECTIONPOINTCONSTRAINT = InputsByMatchDispatchConstraints(
            table_name='SPDCONNECTIONPOINTCONSTRAINT', table_columns=['CONNECTIONPOINTID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                      'GENCONID', 'BIDTYPE', 'FACTOR'],
            table_primary_keys=['CONNECTIONPOINTID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'BIDTYPE'], con=self.con)
        self.SPDINTERCONNECTORCONSTRAINT = InputsByMatchDispatchConstraints(
            table_name='SPDINTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                     'GENCONID', 'FACTOR'],
            table_primary_keys=['INTERCONNECTORID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.INTERCONNECTOR = InputsNoFilter(
            table_name='INTERCONNECTOR', table_columns=['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO'],
            table_primary_keys=['INTERCONNECTORID'], con=self.con)
        self.INTERCONNECTORCONSTRAINT = InputsByEffectiveDateVersionNoAndDispatchInterconnector(
            table_name='INTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                  'FROMREGIONLOSSSHARE', 'LOSSCONSTANT', 'ICTYPE',
                                                                  'LOSSFLOWCOEFFICIENT', 'IMPORTLIMIT', 'EXPORTLIMIT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.LOSSMODEL = InputsByEffectiveDateVersionNoAndDispatchInterconnector(
            table_name='LOSSMODEL', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'LOSSSEGMENT',
                                                   'MWBREAKPOINT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.LOSSFACTORMODEL = InputsByEffectiveDateVersionNoAndDispatchInterconnector(
            table_name='LOSSFACTORMODEL', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'REGIONID',
                                                         'DEMANDCOEFFICIENT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.DISPATCHINTERCONNECTORRES = InputsBySettlementDate(
            table_name='DISPATCHINTERCONNECTORRES', table_columns=['INTERCONNECTORID', 'SETTLEMENTDATE', 'MWFLOW',
                                                                   'MWLOSSES'],
            table_primary_keys=['INTERCONNECTORID', 'SETTLEMENTDATE'], con=self.con)
        self.MNSP_INTERCONNECTOR = InputsByEffectiveDateVersionNo(
            table_name='MNSP_INTERCONNECTOR', table_columns=['INTERCONNECTORID', 'LINKID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                             'FROMREGION', 'TOREGION', 'FROM_REGION_TLF',
                                                             'TO_REGION_TLF'],
            table_primary_keys=['INTERCONNECTORID', 'LINKID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)

    def create_tables(self):
        """Drops any existing default tables and creates new ones, this method is generally called a new database.

        Examples
        --------
        Create the database or connect to an existing one.

        >>> con = sqlite3.connect('historical.db')

        Create the database manager.

        >>> historical = DBManager(con)

        Create a set of default table in the database.

        >>> historical.create_tables()

        Default tables will now exist, but will be empty.

        >>> print(pd.read_sql("Select * from DISPATCHREGIONSUM", con=con))
        Empty DataFrame
        Columns: [SETTLEMENTDATE, REGIONID, TOTALDEMAND, DEMANDFORECAST, INITIALSUPPLY]
        Index: []

        If you added data and then call create_tables again then any added data will be emptied.

        >>> historical.DISPATCHREGIONSUM.add_data(year=2020, month=1)

        >>> print(pd.read_sql("Select * from DISPATCHREGIONSUM limit 3", con=con))
                SETTLEMENTDATE REGIONID  TOTALDEMAND  DEMANDFORECAST  INITIALSUPPLY
        0  2020/01/01 00:05:00     NSW1      7245.31       -26.35352     7284.32178
        1  2020/01/01 00:05:00     QLD1      6095.75       -24.29639     6129.36279
        2  2020/01/01 00:05:00      SA1      1466.53         1.47190     1452.25647

        >>> historical.create_tables()

        >>> print(pd.read_sql("Select * from DISPATCHREGIONSUM", con=con))
        Empty DataFrame
        Columns: [SETTLEMENTDATE, REGIONID, TOTALDEMAND, DEMANDFORECAST, INITIALSUPPLY]
        Index: []

        Returns
        -------
        None
        """
        for name, attribute in self.__dict__.items():
            if hasattr(attribute, 'create_table_in_sqlite_db'):
                attribute.create_table_in_sqlite_db()


def datetime_dispatch_sequence(start_time, end_time):
    """Creates a list of datetimes in the string format '%Y/%m/%d %H:%M:%S', in 5 min intervals.

    Examples
    --------

    >>> date_times = datetime_dispatch_sequence(start_time='2020/01/01 12:00:00', end_time='2020/01/01 12:20:00')

    >>> print(date_times)
    ['2020/01/01 12:05:00', '2020/01/01 12:10:00', '2020/01/01 12:15:00', '2020/01/01 12:20:00']

    Parameters
    ----------
    start_time : str
        In the datetime in the format '%Y/%m/%d %H:%M:%S' e.g. '2020/01/01 12:00:00'
    end_time : str
        In the datetime in the format '%Y/%m/%d %H:%M:%S' e.g. '2020/01/01 12:00:00'
    """
    delta = timedelta(minutes=5)
    start_time = datetime.strptime(start_time, '%Y/%m/%d %H:%M:%S')
    end_time = datetime.strptime(end_time, '%Y/%m/%d %H:%M:%S')
    date_times = []
    curr = start_time + delta
    while curr <= end_time:
        # Change the datetime object to a timestamp and modify its format by replacing characters.
        date_times.append(curr.isoformat().replace('T', ' ').replace('-', '/'))
        curr += delta
    return date_times


dispatch_type_name_map = {'GENERATOR': 'generator', 'LOAD': 'load'}

service_name_mapping = {'ENERGY': 'energy', 'RAISEREG': 'raise_reg', 'LOWERREG': 'lower_reg', 'RAISE6SEC': 'raise_6s',
                        'RAISE60SEC': 'raise_60s', 'RAISE5MIN': 'raise_5min', 'LOWER6SEC': 'lower_6s',
                        'LOWER60SEC': 'lower_60s', 'LOWER5MIN': 'lower_5min'}


def format_fcas_trapezium_constraints(BIDPEROFFER_D):
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


def format_regional_demand(DISPATCHREGIONSUM):
    """Re-formats the AEMO MSS table DISPATCHREGIONSUM to be compatible with the Spot market class.

    Note the demand term used in the interconnector loss functions is calculated by summing the initial supply and the
    demand forecast.

    Examples
    --------

    >>> DISPATCHREGIONSUM = pd.DataFrame({
    ... 'REGIONID': ['NSW1', 'SA1'],
    ... 'TOTALDEMAND': [8000.0, 4000.0],
    ... 'DEMANDFORECAST': [10.0, -10.0],
    ... 'INITIALSUPPLY': [7995.0, 4006.0]})

    >>> regional_demand = format_regional_demand(DISPATCHREGIONSUM)

    >>> print(regional_demand)
      region  demand  loss_function_demand
    0   NSW1  8000.0                8005.0
    1    SA1  4000.0                3996.0

    Parameters
    ----------
    DISPATCHREGIONSUM : pd.DataFrame

        ================  ==========================================================================================
        Columns:          Description:
        REGIONID          unique identifier of a market region (as `str`)
        TOTALDEMAND       the non dispatchable demand the region, in MW (as `np.float64`)
        INITIALSUPPLY     the generation supplied in th region at the start of the interval, in MW (as `np.float64`)
        DEMANDFORECAST    the expected change in demand over dispatch interval, in MW (as `np.float64`)
        ================  ==========================================================================================

    Returns
    ----------
    regional_demand : pd.DataFrame

        ====================  ======================================================================================
        Columns:              Description:
        region                unique identifier of a market region (as `str`)
        demand                the non dispatchable demand the region, in MW (as `np.float64`)
        loss_function_demand  the measure of demand used when creating interconnector loss functions, in MW (as `np.float64`)
        ====================  ======================================================================================
    """

    DISPATCHREGIONSUM['loss_function_demand'] = DISPATCHREGIONSUM['INITIALSUPPLY'] + DISPATCHREGIONSUM['DEMANDFORECAST']
    regional_demand = DISPATCHREGIONSUM.loc[:, ['REGIONID', 'TOTALDEMAND', 'loss_function_demand']]
    regional_demand.columns = ['region', 'demand', 'loss_function_demand']
    return regional_demand


def use_historical_actual_availability_to_filter_fcas_bids(BIDPEROFFER_D, BIDDAYOFFER_D, DISPATCHLOAD):
    """Where AEMO determined zero actual availability of an FCAS offer filter it from the set of bids.

    Note there is an additional condition that the historical dispatch of the service must be zero, sometimes
    the MMS table dispatch load records a zero availability when there was a non-zero dispatch.

    Also energy bids are excluded from filtering.

    Parameters
    ----------
    BIDPEROFFER_D : pd.DataFrame

        ================  ====================================================
        Columns:          Description:
        DUID              unique identifier of a unit (as `str`)
        BIDTYPE           the service being provided (as `str`)
        others optional
        ================  ====================================================

    BIDDAYOFFER_D : pd.DataFrame

        ================  ====================================================
        Columns:          Description:
        DUID              unique identifier of a unit (as `str`)
        BIDTYPE           the service being provided (as `str`)
        others optional
        ================  ====================================================

    DISPATCHLOAD : pd.DataFrame

        ============================  ====================================================
        Columns:                      Description:
        RAISE6SECACTUALAVAILABILITY   calculated availabity after consider all unit based
                                      constraints on FCAS dispatch and assuming other
                                      service offered by the unit are at their dispatch level
        RAISE60SECACTUALAVAILABILITY
        RAISE5MINACTUALAVAILABILITY
        RAISEREGACTUALAVAILABILITY
        LOWER6SECACTUALAVAILABILITY
        LOWER60SECACTUALAVAILABILITY
        LOWER5MINACTUALAVAILABILITY
        LOWERREGACTUALAVAILABILITY
        LOWER5MIN                     dispatched volume of FCAS
        LOWER60SEC
        LOWER6SEC
        RAISE5MIN
        RAISE60SEC
        RAISE6SEC
        LOWERREG
        RAISEREG
        ============================  ====================================================

    Returns
    -------
    BIDPEROFFER_D : pd.DataFrame

    BIDDAYOFFER_D : pd.DataFrame

    """

    # The columns in DISPATCHLOAD containing post constraint availability of FCAS.
    availabilities = ['RAISE6SECACTUALAVAILABILITY', 'RAISE60SECACTUALAVAILABILITY',
                      'RAISE5MINACTUALAVAILABILITY', 'RAISEREGACTUALAVAILABILITY',
                      'LOWER6SECACTUALAVAILABILITY', 'LOWER60SECACTUALAVAILABILITY',
                      'LOWER5MINACTUALAVAILABILITY', 'LOWERREGACTUALAVAILABILITY']

    # The columns in DISPATCHLOAD containing the dispatch volume of each FCAS.
    bid_types = ['LOWER5MIN', 'LOWER60SEC', 'LOWER6SEC', 'RAISE5MIN', 'RAISE60SEC', 'RAISE6SEC', 'LOWERREG', 'RAISEREG']

    # Reshape the data frame so the availability values are by row rather than in different columns.
    availabilities = hf.stack_columns(DISPATCHLOAD, cols_to_keep=['DUID'], cols_to_stack=availabilities,
                                      type_name='BIDTYPE', value_name='availability')

    # Change BIDTYPE column to exclude the suffix ACTUALAVAILABILITY
    availabilities['BIDTYPE'] = availabilities['BIDTYPE'].apply(lambda x: x.replace('ACTUALAVAILABILITY', ''))

    # Reshape the data frame so the dispatch values are by row rather than in different columns.
    dispatch = hf.stack_columns(DISPATCHLOAD, cols_to_keep=['DUID'], cols_to_stack=bid_types,
                                type_name='BIDTYPE', value_name='dispatch')

    # Combine dispatch and availabilities into a single data frame.
    availabilities = pd.merge(availabilities, dispatch, 'inner', on=['DUID', 'BIDTYPE'])

    # Only retain bids that either have non
    availabilities = availabilities[(availabilities['availability'] > 0.0) |
                                    (availabilities['dispatch']) > 0.0]

    BIDPEROFFER_D_energy = BIDPEROFFER_D[BIDPEROFFER_D['BIDTYPE'] == 'ENERGY']
    BIDDAYOFFER_D_energy = BIDDAYOFFER_D[BIDDAYOFFER_D['BIDTYPE'] == 'ENERGY']

    BIDPEROFFER_D = pd.merge(BIDPEROFFER_D, availabilities.loc[:, ['DUID', 'BIDTYPE']], 'inner',
                             on=['DUID', 'BIDTYPE'])
    BIDPEROFFER_D = pd.concat([BIDPEROFFER_D, BIDPEROFFER_D_energy])

    BIDDAYOFFER_D = pd.merge(BIDDAYOFFER_D, availabilities.loc[:, ['DUID', 'BIDTYPE']], 'inner',
                             on=['DUID', 'BIDTYPE'])
    BIDDAYOFFER_D = pd.concat([BIDDAYOFFER_D, BIDDAYOFFER_D_energy])

    return BIDPEROFFER_D, BIDDAYOFFER_D


def format_fcas_market_requirements(SPDREGIONCONSTRAINT, DISPATCHCONSTRAINT, GENCONDATA):
    """

    Parameters
    ----------
    SPDREGIONCONSTRAINT

    Returns
    -------

    """
    # Assume we only use constraints with rhs greater than zero, and the that the rest are swamped
    DISPATCHCONSTRAINT = DISPATCHCONSTRAINT[DISPATCHCONSTRAINT['RHS'] > 0.0]
    fcas_market_requirements = pd.merge(SPDREGIONCONSTRAINT, DISPATCHCONSTRAINT, left_on='GENCONID',
                                        right_on='CONSTRAINTID')
    fcas_market_requirements = pd.merge(fcas_market_requirements, GENCONDATA, on='GENCONID')
    fcas_market_requirements = fcas_market_requirements.loc[:, ['GENCONID', 'BIDTYPE', 'REGIONID', 'RHS',
                                                                'CONSTRAINTTYPE']]
    fcas_market_requirements.columns = ['set', 'service', 'region', 'volume', 'type']
    fcas_market_requirements['service'] = fcas_market_requirements['service'].apply(lambda x: service_name_mapping[x])
    return fcas_market_requirements


def format_generic_constraints_rhs_and_type(DISPATCHCONSTRAINT, GENCONDATA):
    """Re-format AEMO MSS tables DISPATCHCONSTRAINT and GENCONDATA to provide inputs compatible to Spot market class.

    Examples
    --------

    >>> DISPATCHCONSTRAINT = pd.DataFrame({
    ...   'CONSTRAINTID': ['A', 'B'],
    ...   'RHS': [10.0, -20.0]})

    >>> GENCONDATA = pd.DataFrame({
    ...   'GENCONID': ['A', 'B'],
    ...   'CONSTRAINTTYPE': ['<=', '>=']})

    >>> generic_type_and_rhs = format_generic_constraints_rhs_and_type(DISPATCHCONSTRAINT, GENCONDATA)

    >>> print(generic_type_and_rhs)
      set type   rhs
    0   A   <=  10.0
    1   B   >= -20.0

    Parameters
    ----------
    DISPATCHCONSTRAINT : pd.DataFrame

        ============  ====================================================
        Columns:      Description:
        CONSTRAINTID  unique identifier of a constraint (as `str`)
        RHS           the rhs value of the constraint used in dispatch (as `np.float64`)
        ============  ====================================================

    GENCONDATA : pd.DataFrame

        ==============  ====================================================
        Columns:        Description:
        GENCONID        unique identifier of a constraint (as `str`)
        CONSTRAINTTYPE  the constraint type '>=', '<=' or '=' (as `str`)
        ==============  ====================================================

    Returns
    -------
    pd.DataFrame

        ==============  ====================================================
        Columns:        Description:
        set             unique identifier of a constraint (as `str`)
        type            the constraint type '>=', '<=' or '=' (as `str`)
        rhs             the rhs value of the constraint (as `np.float64`)
        ==============  ====================================================
    """
    generic_rhs = DISPATCHCONSTRAINT.loc[:, ['CONSTRAINTID', 'RHS']]
    generic_rhs.columns = ['set', 'rhs']
    generic_type = GENCONDATA.loc[:, ['GENCONID', 'CONSTRAINTTYPE']]
    generic_type.columns = ['set', 'type']
    generic_constraints_type_and_rhs = pd.merge(generic_type, generic_rhs, 'inner', on='set')
    return generic_constraints_type_and_rhs


def format_generic_unit_lhs(SPDCONNECTIONPOINTCONSTRAINT, DUDETAILSUMMARY):
    """Re-format AEMO MSS tables SPDCONNECTIONPOINTCONSTRAINT and DUDETAILSUMMARY to provide inputs to Spot market class.

    Examples
    --------

    >>> SPDCONNECTIONPOINTCONSTRAINT = pd.DataFrame({
    ...   'GENCONID': ['A', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG'],
    ...   'CONNECTIONPOINTID': ['XA1', 'Y2'],
    ...   'FACTOR': [1.0, 0.9]})

    >>> DUDETAILSUMMARY = pd.DataFrame({
    ...   'DUID': ['X', 'Y'],
    ...   'CONNECTIONPOINTID': ['XA1', 'Y2']})

    >>> generic_unit_lhs = format_generic_unit_lhs(SPDCONNECTIONPOINTCONSTRAINT, DUDETAILSUMMARY)

    >>> print(generic_unit_lhs)
      set unit    service  coefficient
    0   A    X     energy          1.0
    1   B    Y  raise_reg          0.9

    Parameters
    ----------
    SPDCONNECTIONPOINTCONSTRAINT : pd.DataFrame

        =================  ==================================================================
        Columns:           Description:
        GENCONID           unique identifier of a generic constraint (as `str`)
        BIDTYPE            the serivce type of the variables being constrained (as `str`)
        CONNECTIONPOINTID  the location in the grid of the variables being constrainted (as `str`)
        FACTOR             the coefficient of the variables being constrained, note if multiple
                           coefficients are provided for a generic constraint then the final
                           coeffficient used is the sum (as `np.float64`)
        =================  ==================================================================

    DUDETAILSUMMARY : pd.DataFrame

        =================  ==================================================================
        Columns:           Description:
        DUID               unique identifier of a unit (as `str`)
        CONNECTIONPOINTID  the location in the grid of the unit (as `str`)
        =================  ==================================================================

    Returns
    -------
    pd.DataFrame

        ==============  =====================================================================
        Columns:        Description:
        set             unique identifier of a generic constraint (as `str`)
        unit            the unit whoes variables are being constrained (as `str`)
        service         the serivce type of the variables being constrained (as `str`)
        coefficient     the coefficient of the variables being constrained (as `np.float64`)
        ==============  =====================================================================
    """
    unit_generic_lhs = SPDCONNECTIONPOINTCONSTRAINT.loc[:, ['GENCONID', 'BIDTYPE', 'CONNECTIONPOINTID', 'FACTOR']]
    unit_generic_lhs = pd.merge(unit_generic_lhs, DUDETAILSUMMARY.loc[:, ['DUID', 'CONNECTIONPOINTID']],
                                on='CONNECTIONPOINTID')
    unit_generic_lhs = unit_generic_lhs.loc[:, ['GENCONID', 'DUID', 'BIDTYPE', 'FACTOR']]
    unit_generic_lhs.columns = ['set', 'unit', 'service', 'coefficient']
    unit_generic_lhs['service'] = unit_generic_lhs['service'].apply(lambda x: service_name_mapping[x])
    return unit_generic_lhs


def format_generic_region_lhs(SPDREGIONCONSTRAINT):
    """Re-format AEMO MSS table SPDREGIONCONSTRAINT to provide inputs to Spot market class.

    Examples
    --------

    >>> SPDREGIONCONSTRAINT = pd.DataFrame({
    ...   'GENCONID': ['A', 'B'],
    ...   'BIDTYPE': ['ENERGY', 'RAISEREG'],
    ...   'REGIONID': ['NSW', 'VIC'],
    ...   'FACTOR': [1.0, 0.9]})

    >>> generic_region_lhs = format_generic_region_lhs(SPDREGIONCONSTRAINT)

    >>> print(generic_region_lhs)
      set region    service  coefficient
    0   A    NSW     energy          1.0
    1   B    VIC  raise_reg          0.9

    Parameters
    ----------
    SPDREGIONCONSTRAINT : pd.DataFrame

        =================  ==================================================================
        Columns:           Description:
        GENCONID           unique identifier of a generic constraint (as `str`)
        BIDTYPE            the serivce type of the variables being constrained (as `str`)
        REGIONID           the region whoes variables are being constrained, acting as shorthand
                           for all the units in this region (as `str`)
        FACTOR             the coefficient of the variables being constrained, note if multiple
                           coefficients are provided for a generic constraint then the final
                           coeffficient used is the sum (as `np.float64`)
        =================  ==================================================================

    Returns
    -------
    pd.DataFrame

        ==============  =====================================================================
        Columns:        Description:
        set             unique identifier of a generic constraint (as `str`)
        region          the region whoes variables are being constrained, acting as shorthand
                        for all the units in this region (as `str`)
        service         the serivce type of the variables being constrained (as `str`)
        coefficient     the coefficient of the variables being constrained (as `np.float64`)
        ==============  =====================================================================
    """
    region_generic_lhs = SPDREGIONCONSTRAINT.loc[:, ['GENCONID', 'BIDTYPE', 'REGIONID', 'FACTOR']]
    region_generic_lhs = region_generic_lhs.loc[:, ['GENCONID', 'REGIONID', 'BIDTYPE', 'FACTOR']]
    region_generic_lhs.columns = ['set', 'region', 'service', 'coefficient']
    region_generic_lhs['service'] = region_generic_lhs['service'].apply(lambda x: service_name_mapping[x])
    return region_generic_lhs


def format_generic_interconnector_lhs(SPDINTERCONNECTORCONSTRAINT):
    """Re-format AEMO MSS table SPDINTERCONNECTORCONSTRAINT to provide inputs to Spot market class.

    Examples
    --------

    >>> SPDINTERCONNECTORCONSTRAINT = pd.DataFrame({
    ...   'GENCONID': ['A', 'B'],
    ...   'INTERCONNECTORID': ['L1', 'L2'],
    ...   'FACTOR': [1.0, 0.9]})

    >>> generic_region_lhs = format_generic_interconnector_lhs(SPDINTERCONNECTORCONSTRAINT)

    >>> print(generic_region_lhs)
      set interconnector  coefficient
    0   A             L1          1.0
    1   B             L2          0.9

    Parameters
    ----------
    SPDINTERCONNECTORCONSTRAINT : pd.DataFrame

        =================  ==================================================================
        Columns:           Description:
        GENCONID           unique identifier of a generic constraint (as `str`)
        INTERCONNECTORID   the interconnector whoes variables are being constrained (as `str`)
        FACTOR             the coefficient of the variables being constrained, note if multiple
                           coefficients are provided for a generic constraint then the final
                           coeffficient used is the sum (as `np.float64`)
        =================  ==================================================================

    Returns
    -------
    pd.DataFrame

        ==============  =====================================================================
        Columns:        Description:
        set             unique identifier of a generic constraint (as `str`)
        interconnector  the interconnector whoes variables are being constrained (as `str`)
        coefficient     the coefficient of the variables being constrained (as `np.float64`)
        ==============  =====================================================================
    """
    interconnector_generic_lhs = SPDINTERCONNECTORCONSTRAINT.loc[:, ['GENCONID', 'INTERCONNECTORID', 'FACTOR']]
    interconnector_generic_lhs = interconnector_generic_lhs.loc[:, ['GENCONID', 'INTERCONNECTORID', 'FACTOR']]
    interconnector_generic_lhs.columns = ['set', 'interconnector', 'coefficient']
    return interconnector_generic_lhs




