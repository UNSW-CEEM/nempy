import numpy as np
import pandas as pd


class DataFrameSchema:
    def __init__(self, name, primary_keys=None, row_monatonic_increasing=None):
        self.name = name
        self.primary_keys = primary_keys
        self.columns = {}
        self.required_columns = []
        self.row_monatonic_increasing = row_monatonic_increasing

    def add_column(self, column, optional=False):
        self.columns[column.name] = column
        if not optional:
            self.required_columns.append(column.name)

    def validate(self, df):
        for col in df:
            if col not in self.columns:
                raise UnexpectedColumn("Column {} is not allowed in DataFrame {}.".format(col, self.name))

        for col in self.required_columns:
            if col not in df.columns:
                raise MissingColumnError("Column {} not in DataFrame {}.".format(col, self.name))

        for col in self.columns:
            if col in df.columns:
                self.columns[col].validate(df[col])

        if self.primary_keys is not None:
            self._check_for_repeated_rows(df)

    def _check_for_repeated_rows(self, df):
        cols_in_df = [col for col in self.primary_keys if col in df.columns]
        if len(df.index) != len(df.drop_duplicates(cols_in_df)):
            raise RepeatedRowError('{} should only have one row for each {}.'.format(self.name, ' '.join(cols_in_df)))

    def _check_row_monatonic_increasing(self, df):
        df = df.loc[:, self.row_monatonic_increasing]
        df = df.transpose()
        df.index = pd.to_numeric(df.index)
        df = df.sort_index()
        for col in df.columns:
            if not df[col].is_monotonic:
                raise BidsNotMonotonicIncreasing('Bids of each unit are not monotonic increasing.')


class SeriesSchema:
    def __init__(self, name, data_type, allowed_values=None, must_be_real_number=False, not_negative=False,
                 minimum=None, maximum=None):
        self.name = name
        self.data_type = data_type
        self.allowed_values = allowed_values
        self.must_be_real_number = must_be_real_number
        self.not_negative = not_negative
        self.min = minimum
        self.max = maximum

    def validate(self, series):
        self._check_data_type(series)
        self._check_allowed_values(series)
        self._check_is_real_number(series)
        self._check_is_not_negtaive(series)

    def _check_data_type(self, series):
        if self.data_type == str:
            if not all(series.apply(lambda x: type(x) == str)):
                raise ColumnDataTypeError('All elements of column {} should have type str'.format(self.name))
        elif self.data_type == callable:
            if not all(series.apply(lambda x: callable(x))):
                raise ColumnDataTypeError('All elements of column {} should have type callable'.format(self.name))
        elif self.data_type != series.dtype:
            raise ColumnDataTypeError('Column {} should have type {}'.format(self.name, self.data_type))

    def _check_allowed_values(self, series):
        if self.allowed_values is not None:
            if not series.isin(self.allowed_values).all():
                raise ColumnValues("The column {} can only contain the values {}.".format(self.name, self.allowed_values))

    def _check_is_real_number(self, series):
        if self.must_be_real_number:
            if np.inf in series.values:
                raise ColumnValues("Value inf not allowed in column {}.".format(self.name))
            if -np.inf in series.values:
                raise ColumnValues("Value -inf not allowed in column {}.".format(self.name))
            if series.isnull().any():
                raise ColumnValues("Null values not allowed in column {}.".format(self.name))

    def _check_is_not_negtaive(self, series):
        if self.not_negative:
            if series.min() < 0.0:
                raise ColumnValues("Negative values not allowed in column '{}'.".format(self.name))


class RepeatedRowError(Exception):
    """Raise for repeated rows."""


class ColumnDataTypeError(Exception):
    """Raise for columns with incorrect data types."""


class MissingColumnError(Exception):
    """Raise for required column missing."""


class UnexpectedColumn(Exception):
    """Raise for unexpected column."""


class ColumnValues(Exception):
    """Raise for unallowed column values."""


class BidsNotMonotonicIncreasing(Exception):
    """Raise for non monotonic increasing bids."""
