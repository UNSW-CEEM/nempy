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
    # Discard last row of DataFrame
    data = data[:-1]
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
        self.con = con
        self.table_name = table_name
        self.table_columns = table_columns
        self.table_primary_keys = table_primary_keys
        # url that sub classes will use to pull MMS tables from nemweb.
        self.url = 'http://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/{year}/MMSDM_{year}_{month}/' + \
                   'MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_DVD_{table}_{year}{month}010000.zip'
        with self.con:
            cur = self.con.cursor()
            cur.execute("""DROP TABLE IF EXISTS {};""".format(table_name))
            base_create_query = """CREATE TABLE {}({}, PRIMARY KEY ({}));"""
            columns = ','.join(['{} TEXT'.format(col) for col in self.table_columns])
            primary_keys = ','.join(['{}'.format(col) for col in self.table_primary_keys])
            create_query = base_create_query.format(table_name, columns, primary_keys)
            cur.execute(create_query)
            self.con.commit()


class SingleDataSource(MMSTable):
    """Manages downloading data from nemweb for tables where all relevant data is stored in lasted data file."""
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def set_data(self, year, month):
        """"Download data for the given table and time, replace any existing data.

        Examples
        --------
        This class is designed to be used after subclassing, however this is how it would be used on it own. This
        example will only work with an internet connection.

        >>> connection = sqlite3.connect('the_database.db')

        >>> table = SingleDataSource(table_name='DUDETAILSUMMARY',
        ...                          table_columns=['DUID', 'START_DATE', 'CONNECTIONPOINTID', 'REGIONID'],
        ...                          table_primary_keys=['START_DATE', 'DUID'], con=connection)

        >>> table.set_data(year=2020, month=1)

        Now the database should contain data for this table that is up to date as the end of Janurary.

        >>> query = "Select * from DUDETAILSUMMARY order by START_DATE DESC limit 1;"

        >>> print(pd.read_sql_query(query, con=connection))
              DUID           START_DATE CONNECTIONPOINTID REGIONID
        0  URANQ11  2020/02/04 00:00:00            NURQ1U     NSW1

        However if we subsequently set data from a previous date then any existing data will be replaced. Note the
        change in the most recent record in the data set below.

        >>> table.set_data(year=2019, month=1)

        >>> print(pd.read_sql_query(query, con=connection))
               DUID           START_DATE CONNECTIONPOINTID REGIONID
        0  WEMENSF1  2019/03/04 00:00:00            VWES2W     VIC1

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
        data = download_to_df(self.url, self.table_name, year, month)
        data = data.loc[:, self.table_columns]
        with self.con:
            data.to_sql(self.table_name, con=self.con, if_exists='replace', index=False)
            self.con.commit()


class MultiDataSource(MMSTable):
    """Manages downloading data from nemweb for tables where data main be stored across multiple monthly files."""
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def add_data(self, year, month):
        """"Download data for the given table and time, appends to any existing data.

        Examples
        --------
        This class is designed to be used after subclassing, however this is how it would be used on it own. This
        example will only work with an internet connection.

        >>> connection = sqlite3.connect('the_database.db')

        >>> table = InputsBySettlementDate(table_name='DISPATCHLOAD',
        ...                                table_columns=['SETTLEMENTDATE', 'DUID',  'RAMPDOWNRATE', 'RAMPUPRATE'],
        ...                                table_primary_keys=['SETTLEMENTDATE', 'DUID'], con=connection)

        >>> table.add_data(year=2020, month=1)

        Now the database should contain data for this table that is up to date as the end of Janurary.

        >>> query = "Select * from DISPATCHLOAD order by SETTLEMENTDATE DESC limit 1;"

        >>> print(pd.read_sql_query(query, con=connection))
                SETTLEMENTDATE   DUID RAMPDOWNRATE RAMPUPRATE
        0  2020/02/01 00:00:00  YWPS4        180.0      180.0

        If we subsequently add data from an earlier month the old data remains in the table, in addition to the new
        data.

        >>> table.add_data(year=2019, month=1)

        >>> print(pd.read_sql_query(query, con=connection))
                SETTLEMENTDATE   DUID RAMPDOWNRATE RAMPUPRATE
        0  2020/02/01 00:00:00  YWPS4        180.0      180.0

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
        data = download_to_df(self.url, self.table_name, year, month)
        data = data[data['INTERVENTION'] == 0]
        data = data.loc[:, self.table_columns]
        with self.con:
            data.to_sql(self.table_name, con=self.con, if_exists='append', index=False)
            self.con.commit()


class InputsBySettlementDate(MultiDataSource):
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        query = "Select * from {table} where SETTLEMENTDATE == '{datetime}'"
        query = query.format(table=self.table_name, datetime=date_time)
        return pd.read_sql_query(query, con=self.con)


class InputsByIntervalDateTime(MultiDataSource):
    """Manages retrieving dispatch inputs by INTERVAL_DATETIME."""
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time e.g. 2019/01/01 11:55:00"

        Examples
        --------
        Set up a dummy database
        >>> con = sqlite3.connect('historical_inputs.db')

        >>> table = InputsByIntervalDateTime(table_name='EXAMPLE', table_columns=['INTERVAL_DATETIME', 'VALUE'],
        ...                                  table_primary_keys=['INTERVAL_DATETIME'], con=con)

        Normally you would use the add_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'INTERVAL_DATETIME': ['2019/01/01 11:55:00', '2019/01/01 12:00:00'],
        ...   'VALUE': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by INTERVAL_DATETIME.

        >>> print(table.get_data(date_time='2019/01/01 12:00:00'))
             INTERVAL_DATETIME VALUE
        0  2019/01/01 12:00:00   2.0

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


class InputsByDay(MultiDataSource):
    """Manages retrieving dispatch inputs by SETTLEMENTDATE, where inputs are stored on a daily basis."""
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

    def get_data(self, date_time):
        """Retrieves data for the specified date_time e.g. 2019/01/01 11:55:00, where inputs are stored on daily basis.

        Note that a market day begins with the first 5 min interval as 04:05:00, there for if and input date_time of
        2019/01/01 04:05:00 is given inputs where the SETTLEMENDATE is 2019/01/01 00:00:00 will be retrieved and if
        a date_time of 2019/01/01 04:00:00 or earlier is given then inputs where the SETTLEMENDATE is
        2018/12/31 00:00:00 will be retrieved.

        Examples
        --------
        Set up a dummy database
        >>> con = sqlite3.connect('historical_inputs.db')

        >>> table = InputsByDay(table_name='EXAMPLE', table_columns=['SETTLEMENTDATE', 'VALUE'],
        ...                     table_primary_keys=['SETTLEMENTDATE'], con=con)

        Normally you would use the add_data method to add historical data, but here we will add data directly to the
        database so some simple example data can be added.

        >>> data = pd.DataFrame({
        ...   'SETTLEMENTDATE': ['2019/01/01 00:00:00', '2019/01/02 00:00:00'],
        ...   'VALUE': [1.0, 2.0]})

        >>> data.to_sql('EXAMPLE', con=con, if_exists='append', index=False)

        When we call get_data the output is filtered by SETTLEMENTDATE and the results from the appropriate market
        day starting at 04:05:00 are retrieved. In the results below note when the output changes

        >>> print(table.get_data(date_time='2019/01/01 12:00:00'))
                SETTLEMENTDATE VALUE
        0  2019/01/01 00:00:00   1.0

        >>> print(table.get_data(date_time='2019/01/02 04:00:00'))
                SETTLEMENTDATE VALUE
        0  2019/01/01 00:00:00   1.0

        >>> print(table.get_data(date_time='2019/01/02 04:05:00'))
                SETTLEMENTDATE VALUE
        0  2019/01/02 00:00:00   2.0

        >>> print(table.get_data(date_time='2019/01/02 12:00:00'))
                SETTLEMENTDATE VALUE
        0  2019/01/02 00:00:00   2.0

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


class InputsStartAndEnd(MultiDataSource):
    def __init__(self, table_name, table_columns, table_primary_keys, con):
        MMSTable.__init__(self, table_name, table_columns, table_primary_keys, con)

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
            table_primary_keys=['INTERVAL_DATETIME', 'DUID', 'BIDTYPE'], con=self.con)
        self.BIDDAYOFFER_D = InputsByIntervalDateTime(
            table_name='BIDDAYOFFER_D', table_columns=['SETTLEMENTDATE', 'DUID', 'BIDTYPE', 'PRICEBAND1', 'PRICEBAND2',
                                                       'PRICEBAND3', 'PRICEBAND4', 'PRICEBAND5', 'PRICEBAND6',
                                                       'PRICEBAND7', 'PRICEBAND8', 'PRICEBAND9', 'PRICEBAND10', 'T1',
                                                       'T2', 'T3', 'T4'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID', 'BIDTYPE'], con=self.con)
        self.DISPATCHREGIONSUM = InputsBySettlementDate(
            table_name='DISPATCHREGIONSUM', table_columns=['SETTLEMENTDATE', 'REGIONID', 'TOTALDEMAND',
                                                           'DEMANDFORECAST', 'INITIALSUPPLY'],
            table_primary_keys=['SETTLEMENTDATE', 'REGIONID'], con=self.con)
        self.DISPATCHLOAD = InputsBySettlementDate(
            table_name='DISPATCHLOAD', table_columns=['SETTLEMENTDATE', 'DUID', 'DISPATCHMODE', 'AGCSTATUS',
                                                      'INITIALMW', 'TOTALCLEARED', 'RAMPDOWNRATE', 'RAMPUPRATE',
                                                      'AVAILABILITY', 'RAISEREGENABLEMENTMAX', 'RAISEREGENABLEMENTMIN',
                                                      'LOWERREGENABLEMENTMAX', 'LOWERREGENABLEMENTMIN'],
            table_primary_keys=['SETTLEMENTDATE', 'DUID'], con=self.con)
        self.DUDETAILSUMMARY = InputsBySettlementDate(
            table_name='DUDETAILSUMMARY', table_columns=['DUID', 'START_DATE', 'END_DATE', 'DISPATCHTYPE',
                                                         'CONNECTIONPOINTID', 'REGIONID', 'STATIONID',
                                                         'LASTCHANGED', 'TRANSMISSIONLOSSFACTOR',
                                                         'DISTRIBUTIONLOSSFACTOR'],
            table_primary_keys=['START_DATE', 'DUID'], con=self.con)
        self.DISPATCHCONSTRAINT = InputsBySettlementDate(
            table_name='DISPATCHCONSTRAINT', table_columns=['SETTLEMENTDATE', 'CONSTRAINTID', 'RHS',
                                                           'GENCONID_EFFECTIVEDATE', 'GENCONID_VERSIONNO'],
            table_primary_keys=['SETTLEMENTDATE', 'CONSTRAINTID'], con=self.con)
        self.GENCONDATA = InputsByIntervalDateTime(
            table_name='GENCONDATA', table_columns=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'CONSTRAINTTYPE'
                                                    'GENERICCONSTRAINTWEIGHT'],
            table_primary_keys=['GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.SPDREGIONCONSTRAINT = InputsByIntervalDateTime(
            table_name='SPDREGIONCONSTRAINT', table_columns=['REGIONID', 'EFFECTIVEDATE', 'VERSIONNO', 'GENCONID',
                                                             'BIDTYPE', 'FACTOR'],
            table_primary_keys=['REGIONID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'BIDTYPE'], con=self.con)
        self.SPDCONNECTIONPOINTCONSTRAINT = InputsByIntervalDateTime(
            table_name='SPDCONNECTIONPOINTCONSTRAINT', table_columns=['CONNECTIONPOINTID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                      'GENCONID', 'BIDTYPE', 'FACTOR'],
            table_primary_keys=['CONNECTIONPOINTID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO', 'BIDTYPE'], con=self.con)
        self.SPDINTERCONNECTORCONSTRAINT = InputsByIntervalDateTime(
            table_name='SPDINTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                     'GENCONID', 'BIDTYPE', 'FACTOR'],
            table_primary_keys=['INTERCONNECTORID', 'GENCONID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.INTERCONNECTOR = InputsByIntervalDateTime(
            table_name='INTERCONNECTOR', table_columns=['INTERCONNECTORID', 'REGIONFROM', 'REGIONTO'],
            table_primary_keys=['INTERCONNECTORID'], con=self.con)
        self.INTERCONNECTORCONSTRAINT = InputsByIntervalDateTime(
            table_name='INTERCONNECTORCONSTRAINT', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO',
                                                                  'FROMREGIONLOSSSHARE', 'LOSSCONSTANT',
                                                                  'LOSSFLOWCOEFFICIENT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.LOSSMODEL = InputsByIntervalDateTime(
            table_name='LOSSMODEL', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'LOSSSEGMENT',
                                                   'MWBREAKPOINT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.LOSSFACTORMODEL = InputsByIntervalDateTime(
            table_name='LOSSFACTORMODEL', table_columns=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO', 'REGIONID',
                                                         'DEMANDCOEFFICIENT'],
            table_primary_keys=['INTERCONNECTORID', 'EFFECTIVEDATE', 'VERSIONNO'], con=self.con)
        self.DISPATCHINTERCONNECTORRES = InputsBySettlementDate(
            table_name='DISPATCHINTERCONNECTORRES', table_columns=['INTERCONNECTORID', 'SETTLEMENTDATE'],
            table_primary_keys=['INTERCONNECTORID', 'SETTLEMENTDATE'], con=self.con)


    # @check.table_exists()
    # def get_historical_inputs_old(self, table_name, applicable_for):
    #     settlement_date_query = "Select * from {table} where SETTLEMENTDATE == '{datetime}'"
    #     dispatch_interval_data_query = "Select * from {table} where INTERVAL_DATETIME == '{datetime}'"
    #     start_and_end_time_query = "Select * from {table} where START_DATE <= '{datetime}' and END_DATE > '{datetime}'"
    #     effective_date = """Create temporary table temp as
    #                             Select * from {table} where EFFECTIVEDATE <= '{datetime}';
    #
    #                         Create temporary table temp2 as
    #                             Select {id}, EFFECTIVEDATE, max(VERSIONNO) as VERSIONNO
    #                               from temp
    #                           group by {id}, EFFECTIVEDATE';
    #
    #                         Create temporary table temp3 as
    #                             Select {id}, VERSIONNO, max(EFFECTIVEDATE) as EFFECTIVEDATE
    #                               from temp2
    #                           group by {id}';
    #
    #                           Select * from {table} inner join temp3 on {id}, VERSIONNO, EFFECTIVEDATE
    #                      """
    #
    #     query_to_execute = queries_by_table[table_name].format(table=table_name, datetime=applicable_for,
    #                                                            id='INTERCONNECTORID')
    #     return pd.read_sql_query(query_to_execute, con=self.con)

# from time import time
# db = DBManager('historical_input.db')
# db.DISPATCHCONSTRAINT.add_data(year=2020, month=1)
# t0 = time()
# df = db.DISPATCHCONSTRAINT.get_data('2020/01/20 00:00:00')
# print(df)
# print(time()-t0)


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
    ...   'flow_coefficient': [0.00019617, 0.00017027]})

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
    return loss_functions.loc[:, ['interconnector', 'loss_function']]


def create_function(constant, flow_coefficient):
    def loss_function(flow):
        return (constant - 1) * flow + (flow_coefficient/2) * flow ** 2
    return loss_function
