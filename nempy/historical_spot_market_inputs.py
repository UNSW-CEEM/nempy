import requests
import zipfile
import io
import pandas as pd
import sqlite3
from nempy import check
from datetime import datetime, timedelta
from time import time
import os
import numpy as np


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

        >>> con = sqlite3.connect('historical_inputs.db')

        Create the table object.

        >>> table = _MMSTable(table_name='a_table', table_columns=['col_1', 'col_2'], table_primary_keys=['col_1'],
        ...                  con=con)

        Clean up by deleting database created.

        >>> con.close()
        >>> os.remove('historical_inputs.db')

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
            'SEMIDISPATCHCAP': 'REAL'
        }

    def create_table_in_sqlite_db(self):
        """Creates a table in the sqlite database that the object has a connection to.

        Note
        ----
        This method and its documentation is inherited from the _MMSTable class.

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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


class InputsBySettlementDate(_MultiDataSource):
    """Manages retrieving dispatch inputs by SETTLEMENTDATE."""

    def __init__(self, table_name, table_columns, table_primary_keys, con):
        _MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time e.g. 2019/01/01 11:55:00"

        Examples
        --------
        Set up a database or connect to an existing one.

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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


class InputsByMatchDispatchConstraints(_SingleDataSource):
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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

        >>> con = sqlite3.connect('historical_inputs.db')

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
        >>> os.remove('historical_inputs.db')

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

    >>> con = sqlite3.connect('historical_inputs.db')

    Create the database manager.

    >>> historical_inputs = DBManager(con)

    Create a set of default table in the database.

    >>> historical_inputs.create_tables()

    Add data from AEMO nemweb data portal. In this case we are adding data from the table BIDDAYOFFER_D which contains
    unit's volume bids on 5 min basis, the data comes in monthly chunks.

    >>> historical_inputs.BIDDAYOFFER_D.add_data(year=2020, month=1)

    >>> historical_inputs.BIDDAYOFFER_D.add_data(year=2020, month=2)

    This table has an add_data method indicating that data provided by AEMO comes in monthly files that do not overlap.
    If you need data for multiple months then multiple add_data calls can be made.

    Data for a specific 5 min dispatch interval can then be retrieved.

    >>> print(historical_inputs.BIDDAYOFFER_D.get_data('2020/01/10 12:35:00').head())
            SETTLEMENTDATE     DUID     BIDTYPE  PRICEBAND1  ...    T1   T2    T3   T4
    0  2020/01/10 00:00:00   AGLHAL      ENERGY     -974.80  ...  10.0  3.0  10.0  2.0
    1  2020/01/10 00:00:00   AGLSOM      ENERGY     -980.69  ...  20.0  2.0  35.0  2.0
    2  2020/01/10 00:00:00  ANGAST1      ENERGY     -941.23  ...   0.0  0.0   0.0  0.0
    3  2020/01/10 00:00:00    APD01   LOWER5MIN        0.00  ...   0.0  0.0   0.0  0.0
    4  2020/01/10 00:00:00    APD01  LOWER60SEC        0.00  ...   0.0  0.0   0.0  0.0
    <BLANKLINE>
    [5 rows x 17 columns]

    Some tables will have a set_data method instead of an add_data method, indicating that the most recent data file
    provided by AEMO contains all historical data for this table. In this case if multiple calls to the set_data method
    are made the new data replaces the old.

    >>> historical_inputs.DUDETAILSUMMARY.set_data(year=2020, month=2)

    Data for a specific 5 min dispatch interval can then be retrieved.

    >>> print(historical_inputs.DUDETAILSUMMARY.get_data('2020/01/10 12:35:00').head())
           DUID  ... DISTRIBUTIONLOSSFACTOR
    0    AGLHAL  ...                 1.0000
    1   AGLNOW1  ...                 1.0000
    2  AGLSITA1  ...                 1.0000
    3    AGLSOM  ...                 0.9891
    4   ANGAST1  ...                 0.9890
    <BLANKLINE>
    [5 rows x 8 columns]

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
                                                      'SEMIDISPATCHCAP'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID'], con=self.con)
        self.DUDETAILSUMMARY = InputsStartAndEnd(
            table_name='DUDETAILSUMMARY', table_columns=['DUID', 'START_DATE', 'END_DATE', 'DISPATCHTYPE',
                                                         'CONNECTIONPOINTID', 'REGIONID', 'TRANSMISSIONLOSSFACTOR',
                                                         'DISTRIBUTIONLOSSFACTOR'],
            table_primary_keys=['START_DATE', 'DUID'], con=self.con)
        self.DUDETAIL = InputsByEffectiveDateVersionNo(
            table_name='DUDETAIL', table_columns=['DUID', 'EFFECTIVEDATE', 'VERSIONNO', 'MAXCAPACITY'],
            table_primary_keys=['DUID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.DISPATCHCONSTRAINT = InputsBySettlementDate(
            table_name='DISPATCHCONSTRAINT', table_columns=['SETTLEMENTDATE', 'CONSTRAINTID', 'RHS',
                                                            'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO'],
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
                                                                     'GENCONID', 'BIDTYPE', 'FACTOR'],
            table_primary_keys=['INTERCONNECTORID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.INTERCONNECTOR = InputsNoFilter(
            table_name='INTERCONNECTOR', table_columns=['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO'],
            table_primary_keys=['INTERCONNECTORID'], con=self.con)
        self.INTERCONNECTORCONSTRAINT = InputsByEffectiveDateVersionNoAndDispatchInterconnector(
            table_name='INTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                  'FROMREGIONLOSSSHARE', 'LOSSCONSTANT',
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

    def create_tables(self):
        """Drops any existing default tables and creates new ones, this method is generally called a new database.

        Examples
        --------
        Create the database or connect to an existing one.

        >>> con = sqlite3.connect('historical_inputs.db')

        Create the database manager.

        >>> historical_inputs = DBManager(con)

        Create a set of default table in the database.

        >>> historical_inputs.create_tables()

        Default tables will now exist, but will be empty.

        >>> print(pd.read_sql("Select * from DISPATCHREGIONSUM", con=con))
        Empty DataFrame
        Columns: [SETTLEMENTDATE, REGIONID, TOTALDEMAND, DEMANDFORECAST, INITIALSUPPLY]
        Index: []

        If you added data and then call create_tables again then any added data will be emptied.

        >>> historical_inputs.DISPATCHREGIONSUM.add_data(year=2020, month=1)

        >>> print(pd.read_sql("Select * from DISPATCHREGIONSUM limit 3", con=con))
                SETTLEMENTDATE REGIONID  TOTALDEMAND  DEMANDFORECAST  INITIALSUPPLY
        0  2020/01/01 00:05:00     NSW1      7245.31       -26.35352     7284.32178
        1  2020/01/01 00:05:00     QLD1      6095.75       -24.29639     6129.36279
        2  2020/01/01 00:05:00      SA1      1466.53         1.47190     1452.25647

        >>> historical_inputs.create_tables()

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


def create_loss_functions(interconnector_coefficients, demand_coefficients, demand):
    """Creates a loss function for each interconnector.

    Transforms the dynamic demand dependendent interconnector loss functions into functions that only depend on
    interconnector flow. i.e takes the function f and creates g by pre-calculating the demand dependent terms.

        f(inter_flow, flow_coefficient, nsw_demand, nsw_coefficient, qld_demand, qld_coefficient) = inter_losses

    becomes

        g(inter_flow) = inter_losses

    The mathematics of the demand dependent loss functions is described in the
    :download:`Marginal Loss Factors documentation section 3 to 5  <../../docs/pdfs/Marginal Loss Factors for the 2020-21 Financial year.pdf>`.

    Examples
    --------
    >>> import pandas as pd

    Some arbitrary regional demands.

    >>> demand = pd.DataFrame({
    ...   'region': ['VIC1', 'NSW1', 'QLD1', 'SA1'],
    ...   'demand': [6000.0 , 7000.0, 5000.0, 3000.0]})

    Loss model details from 2020 Jan NEM web LOSSFACTORMODEL file

    >>> demand_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'NSW1-QLD1', 'VIC1-NSW1', 'VIC1-NSW1', 'VIC1-NSW1'],
    ...   'region': ['NSW1', 'QLD1', 'NSW1', 'VIC1', 'SA1'],
    ...   'demand_coefficient': [-0.00000035146, 0.000010044, 0.000021734, -0.000031523, -0.000065967]})

    Loss model details from 2020 Jan NEM web INTERCONNECTORCONSTRAINT file

    >>> interconnector_coefficients = pd.DataFrame({
    ...   'interconnector': ['NSW1-QLD1', 'VIC1-NSW1'],
    ...   'loss_constant': [0.9529, 1.0657],
    ...   'flow_coefficient': [0.00019617, 0.00017027],
    ...   'from_region_loss_share': [0.5, 0.5]})

    Create the loss functions

    >>> loss_functions = create_loss_functions(interconnector_coefficients, demand_coefficients, demand)

    Lets use one of the loss functions, first get the loss function of VIC1-NSW1 and call it g

    >>> g = loss_functions[loss_functions['interconnector'] == 'VIC1-NSW1']['loss_function'].iloc[0]

    Calculate the losses at 600 MW flow

    >>> print(g(600.0))
    -70.87199999999996

    Now for NSW1-QLD1

    >>> h = loss_functions[loss_functions['interconnector'] == 'NSW1-QLD1']['loss_function'].iloc[0]

    >>> print(h(600.0))
    35.70646799999993

    Parameters
    ----------
    interconnector_coefficients : pd.DataFrame

        ================  ============================================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        loss_constant     the constant term in the interconnector loss factor equation (as np.float64)
        flow_coefficient  the coefficient of interconnector flow variable in the loss factor equation (as np.float64)
        ================  ============================================================================================

    demand_coefficients : pd.DataFrame

        ==================  =========================================================================================
        Columns:            Description:
        interconnector      unique identifier of a interconnector (as `str`)
        region              the market region whose demand the coefficient applies too, required (as `str`)
        demand_coefficient  the coefficient of regional demand variable in the loss factor equation (as `np.float64`)
        ==================  =========================================================================================

    demand : pd.DataFrame

        ========  =====================================================================================
        Columns:  Description:
        region    unique identifier of a region (as `str`)
        demand    the estimated regional demand, as calculated by initial supply + demand forecast,
                  in MW (as `np.float64`)
        ========  =====================================================================================

    Returns
    -------
    pd.DataFrame

        loss_functions

        ================  ============================================================================================
        Columns:          Description:
        interconnector    unique identifier of a interconnector (as `str`)
        loss_function     a `function` object that takes interconnector flow (as `float`) an input and returns
                          interconnector losses (as `float`).
        ================  ============================================================================================
    """

    demand_loss_factor_offset = pd.merge(demand_coefficients, demand, 'inner', on=['region'])
    demand_loss_factor_offset['offset'] = demand_loss_factor_offset['demand'] * \
                                          demand_loss_factor_offset['demand_coefficient']
    demand_loss_factor_offset = demand_loss_factor_offset.groupby('interconnector', as_index=False)['offset'].sum()
    loss_functions = pd.merge(interconnector_coefficients, demand_loss_factor_offset, 'left', on=['interconnector'])
    loss_functions['loss_constant'] = loss_functions['loss_constant'] + loss_functions['offset'].fillna(0)
    loss_functions['loss_function'] = \
        loss_functions.apply(lambda x: create_function(x['loss_constant'], x['flow_coefficient']), axis=1)
    return loss_functions.loc[:, ['interconnector', 'loss_function', 'from_region_loss_share']]


def create_function(constant, flow_coefficient):
    def loss_function(flow):
        return (constant - 1) * flow + (flow_coefficient / 2) * flow ** 2

    return loss_function


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


def format_unit_info(unit_info):
    # Combine loss factors.
    unit_info['LOSSFACTOR'] = unit_info['TRANSMISSIONLOSSFACTOR'] * unit_info['DISTRIBUTIONLOSSFACTOR']
    unit_info = unit_info.loc[:, ['DUID', 'DISPATCHTYPE', 'CONNECTIONPOINTID', 'REGIONID', 'LOSSFACTOR']]
    unit_info.columns = ['unit', 'dispatch_type', 'connection_point', 'region', 'loss_factor']
    return unit_info


def format_volume_bids(volume_bids):
    volume_bids = volume_bids.loc[:, ['DUID', 'BIDTYPE', 'BANDAVAIL1', 'BANDAVAIL2', 'BANDAVAIL3', 'BANDAVAIL4',
                                      'BANDAVAIL5', 'BANDAVAIL6', 'BANDAVAIL7', 'BANDAVAIL8', 'BANDAVAIL9',
                                      'BANDAVAIL10']]
    volume_bids.columns = ['unit', 'service', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    return volume_bids


def format_price_bids(price_bids):
    price_bids = price_bids.loc[:, ['DUID', 'BIDTYPE', 'PRICEBAND1', 'PRICEBAND2', 'PRICEBAND3', 'PRICEBAND4',
                                    'PRICEBAND5', 'PRICEBAND6', 'PRICEBAND7', 'PRICEBAND8', 'PRICEBAND9',
                                    'PRICEBAND10']]
    price_bids.columns = ['unit', 'service', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
    return price_bids


def format_interconnector_definitions(interconnector_directions, interconnector_paramaters):
    interconnector_directions = interconnector_directions.loc[:, ['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO']]
    interconnector_directions.columns = ['interconnector', 'to_region', 'from_region']
    interconnector_paramaters = interconnector_paramaters.loc[:, ['INTERCONNECTORID', 'IMPORTLIMIT', 'EXPORTLIMIT']]
    interconnector_paramaters.columns = ['interconnector', 'min', 'max']
    interconnector_paramaters['min'] = -1 * interconnector_paramaters['min']
    interconnectors = pd.merge(interconnector_directions, interconnector_paramaters, 'inner', on='interconnector')
    return interconnectors


def format_interconnector_loss_coefficients(interconnector_paramaters):
    interconnector_paramaters = interconnector_paramaters.loc[:, ['INTERCONNECTORID', 'LOSSCONSTANT',
                                                                  'LOSSFLOWCOEFFICIENT', 'FROMREGIONLOSSSHARE']]
    interconnector_paramaters.columns = ['interconnector', 'loss_constant', 'flow_coefficient',
                                         'from_region_loss_share']
    return interconnector_paramaters


def format_interconnector_loss_demand_coefficient(interconnector_demand_coefficients):
    interconnector_demand_coefficients = \
        interconnector_demand_coefficients.loc[:, ['INTERCONNECTORID', 'REGIONID', 'DEMANDCOEFFICIENT']]
    interconnector_demand_coefficients.columns = ['interconnector', 'region', 'demand_coefficient']
    return interconnector_demand_coefficients


def format_regional_demand(regional_demand):
    regional_demand = regional_demand.loc[:, ['REGIONID', 'TOTALDEMAND', 'DEMANDFORECAST',
                                              'INITIALSUPPLY']]
    regional_demand.columns = ['region', 'demand', 'demand_forecast', 'initial_supply']
    return regional_demand


def format_interpolation_break_points(format_interpolation_break_points):
    format_interpolation_break_points = format_interpolation_break_points.loc[:, ['INTERCONNECTORID', 'MWBREAKPOINT']]
    format_interpolation_break_points.columns = ['interconnector', 'break_point']
    return format_interpolation_break_points


def determine_unit_limits(initial_conditions, offered_capacity):

    # Override ramp rates for fast start units.
    ic = initial_conditions
    ic['RAMPMAX'] = ic['INITIALMW'] + ic['RAMPUPRATE'] * (5/60)
    ic['RAMPUPRATE'] = np.where((ic['TOTALCLEARED'] > ic['RAMPMAX']) & (ic['DISPATCHMODE'] != 0.0),
                                (ic['TOTALCLEARED'] - ic['INITIALMW']) * (60/5), ic['RAMPUPRATE'])
    ic['RAMPMIN'] = ic['INITIALMW'] - ic['RAMPDOWNRATE'] * (5/60)
    ic['RAMPDOWNRATE'] = np.where((ic['TOTALCLEARED'] < ic['RAMPMIN']) & (ic['DISPATCHMODE'] != 0.0),
                                  (ic['INITIALMW'] - ic['TOTALCLEARED'] ) * (60/5), ic['RAMPDOWNRATE'])

    # Override ALAILABILITY when SEMIDISPATCHCAP is 1.0
    ic = pd.merge(ic, offered_capacity.loc[:, ['DUID', 'MAXAVAIL']], 'inner', on='DUID')
    ic['AVAILABILITY'] = np.where((ic['MAXAVAIL'] < ic['AVAILABILITY']) & (ic['SEMIDISPATCHCAP'] == 1.0) &
                                  (ic['MAXAVAIL'] > 0.01) & (ic['TOTALCLEARED'] < ic['MAXAVAIL']), ic['MAXAVAIL'],
                                  ic['AVAILABILITY'])

    # Where the availability is lower than the ramp down min set the availibility to equal the ramp down min.
    ic['AVAILABILITY'] = np.where(ic['AVAILABILITY'] < ic['RAMPMIN'], ic['RAMPMIN'], ic['AVAILABILITY'])

    # Format for compatibility with the Spot market class.
    ic = ic.loc[:, ['DUID', 'INITIALMW', 'AVAILABILITY', 'RAMPDOWNRATE', 'RAMPUPRATE']]
    ic.columns = ['unit', 'initial_output', 'capacity', 'ramp_down_rate', 'ramp_up_rate']

    return ic
