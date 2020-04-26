import numpy as np
import pandas as pd


def keep_details(fn):
    def wrapper(inner):
        inner.__name__ = fn.__name__
        inner.__doc__ = fn.__doc__
        return inner
    return wrapper


def energy_bid_ids_exist(func):
    @keep_details(func)
    def wrapper(*args):
        if 'energy_bids' not in args[0].decision_variables:
            raise ModelBuildError('This cannot be performed before energy volume bids are set.')
        func(*args)
    return wrapper


def bid_prices_monotonic_increasing(func):
    @keep_details(func)
    def wrapper(*args):
        bids = args[1].copy()
        bids = bids.set_index('unit', drop=True)
        bids = bids.transpose()
        bids.index = pd.to_numeric(bids.index)
        bids = bids.sort_index()
        for col in bids.columns:
            if not bids[col].is_monotonic:
                raise BidsNotMonotonicIncreasing('Bids of each unit are not monotonic increasing.')
        func(*args)
    return wrapper


def pre_dispatch(func):
    @keep_details(func)
    def wrapper(*args):
        if 'energy_bids' in args[0].decision_variables and 'energy_bids' not in \
                args[0].objective_function_components:
            raise ModelBuildError('No unit energy bids provided.')
        func(*args)
    return wrapper


def repeated_rows(name, cols):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check and len(args[1].index) != len(args[1].drop_duplicates(cols)):
                raise RepeatedRowError('{} should only have one row for each {}.'.format(name, ' '.join(cols)))
            func(*args)
        return wrapper
    return decorator


def column_data_types(name, dtypes):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in args[1].columns:
                    if column in dtypes and dtypes[column] == str:
                            if not hasattr(args[1][column], 'str'):
                                raise ColumnDataTypeError('Column {} in {} should have type str'.format(column, name))
                    elif column in dtypes and dtypes[column] != args[1][column].dtype:
                        raise ColumnDataTypeError('Column {} in {} should have type {}'.
                                                  format(column, name, dtypes[column]))
                    elif column not in dtypes and dtypes['else'] != args[1][column].dtype:
                        raise ColumnDataTypeError('Column {} in {} should have type {}'.
                                                  format(column, name, dtypes['else']))
            func(*args)
        return wrapper
    return decorator


def required_columns(name, required):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in required:
                    if column not in args[1].columns:
                        raise MissingColumnError("Column '{}' not in {}.".format(column, name))
                if len(args[1].columns) < 2:
                    raise MissingColumnError("No bid bands provided.")
            func(*args)
        return wrapper
    return decorator


def allowed_columns(name, allowed):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in args[1].columns:
                    if column not in allowed:
                        raise UnexpectedColumn("Column '{}' not allowed in {}.".format(column, name))
            func(*args)
        return wrapper
    return decorator


def column_values_must_be_real(name, cols_to_check):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in cols_to_check:
                    if column not in args[1].columns:
                        continue
                    if np.inf in args[1][column].values:
                        raise ColumnValues("Value inf not allowed in column '{}' in {}.".format(column, name))
                    if np.NINF in args[1][column].values:
                        raise ColumnValues("Value -inf not allowed in column '{}' in {}.".format(column, name))
                    if args[1][column].isnull().any():
                        raise ColumnValues("Null values not allowed in column '{}' in {}.".format(column, name))
            func(*args)
        return wrapper
    return decorator


def column_values_not_negative(name, cols_to_check):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in cols_to_check:
                    if column not in args[1].columns:
                        continue
                    if args[1][column].min() < 0.0:
                        raise ColumnValues("Negative values not allowed in column '{}' in {}.".format(column, name))
            func(*args)
        return wrapper
    return decorator


class ModelBuildError(Exception):
    """Raise for building model components in wrong order."""


class RepeatedRowError(Exception):
    """Raise for repeated rows."""


class ColumnDataTypeError(Exception):
    """Raise for columns with incorrect data types."""


class MissingColumnError(Exception):
    """Raise for required column missing."""


class UnexpectedColumn(Exception):
    """Raise for unexpected column."""


class ColumnValues(Exception):
    """Raise for unexpected column."""


class BidsNotMonotonicIncreasing(Exception):
    """Raise for non monotonic increasing bids."""

