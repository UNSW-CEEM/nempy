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
        if 'bids' not in args[0]._decision_variables:
            raise ModelBuildError('This cannot be performed before energy volume bids are set.')
        func(*args)

    return wrapper


def all_units_have_info(func):
    @keep_details(func)
    def wrapper(*args):
        if not set(args[1]['unit'].unique()) <= set(args[0]._unit_info['unit']):
            raise ModelBuildError('Not all unit with bids are present in the unit_info input.')
        func(*args)
    return wrapper


def interconnectors_exist(func):
    @keep_details(func)
    def wrapper(*args):
        if 'interconnectors' not in args[0]._decision_variables:
            raise ModelBuildError('Losses cannot be added to interconnectors because they do not exist yet.')
        existing_inters = args[0]._decision_variables['interconnectors']['interconnector'].unique()
        new_inters = args[1]['interconnector'].unique()
        if not all(inter in existing_inters for inter in new_inters):
            raise ModelBuildError('Losses cannot be added to interconnectors because they do not exist yet.')
        func(*args)
    return wrapper


def bid_prices_monotonic_increasing(func, arg=1):
    @keep_details(func)
    def wrapper(*args):
        bids = args[arg].copy()
        if 'service' in bids.columns:
            bids = bids.set_index(['unit', 'service'], drop=True)
        else:
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
        if 'energy_bids' in args[0]._decision_variables and 'energy_bids' not in \
                args[0]._objective_function_components:
            raise ModelBuildError('No unit energy bids provided.')
        func(*args)

    return wrapper


def repeated_rows(name, cols, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            cols_in_df = [col for col in cols if col in args[arg].columns]
            if args[0].check and len(args[arg].index) != len(args[arg].drop_duplicates(cols_in_df)):
                raise RepeatedRowError('{} should only have one row for each {}.'.format(name, ' '.join(cols_in_df)))
            func(*args)

        return wrapper

    return decorator


def column_data_types(name, dtypes, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in args[arg].columns:
                    if column in dtypes and dtypes[column] == str:
                        if not all(args[arg].apply(lambda x: type(x[column]) == str, axis=1)):
                            raise ColumnDataTypeError('Column {} in {} should have type str'.format(column, name))
                    elif column in dtypes and dtypes[column] == 'callable':
                        if not all(args[arg].apply(lambda x: callable(x[column]), axis=1)):
                            raise ColumnDataTypeError('Column {} in {} should be a function'.format(column, name))
                    elif column in dtypes and dtypes[column] != args[arg][column].dtype:
                        raise ColumnDataTypeError('Column {} in {} should have type {}'.
                                                  format(column, name, dtypes[column]))
                    elif column not in dtypes and dtypes['else'] != args[arg][column].dtype:
                        raise ColumnDataTypeError('Column {} in {} should have type {}'.
                                                  format(column, name, dtypes['else']))
            func(*args)

        return wrapper

    return decorator


def required_columns(name, required, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in required:
                    if column not in args[arg].columns:
                        raise MissingColumnError("Column '{}' not in {}.".format(column, name))
                if len(args[arg].columns) < 2:
                    raise MissingColumnError("No bid bands provided.")
            func(*args)

        return wrapper

    return decorator


def allowed_columns(name, allowed, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in args[arg].columns:
                    if column not in allowed:
                        raise UnexpectedColumn("Column '{}' not allowed in {}.".format(column, name))
            func(*args)

        return wrapper

    return decorator


def column_values_must_be_real(name, cols_to_check, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in cols_to_check:
                    if column not in args[arg].columns:
                        continue
                    if np.inf in args[arg][column].values:
                        raise ColumnValues("Value inf not allowed in column '{}' in {}.".format(column, name))
                    if np.NINF in args[arg][column].values:
                        raise ColumnValues("Value -inf not allowed in column '{}' in {}.".format(column, name))
                    if args[arg][column].isnull().any():
                        raise ColumnValues("Null values not allowed in column '{}' in {}.".format(column, name))
            func(*args)

        return wrapper

    return decorator


def column_values_not_negative(name, cols_to_check, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in cols_to_check:
                    if column not in args[arg].columns:
                        continue
                    if args[arg][column].min() < 0.0:
                        raise ColumnValues("Negative values not allowed in column '{}' in {}.".format(column, name))
            func(*args)

        return wrapper

    return decorator


def column_values_outside_range(name, column_ranges, arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column, allowed_range in column_ranges.items():
                    if not all(args[arg].apply(
                            lambda x: allowed_range[0] <= x[column] <= allowed_range[1], axis=1)):
                        raise ColumnValues(
                            "Values in {} in column '{}' outside the range {} to {}.".format(name, column,
                                                                                             allowed_range[0],
                                                                                             allowed_range[1]))
            func(*args)

        return wrapper

    return decorator


def table_exists(arg=1):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            with args[0].con:
                cur = args[0].con.cursor()
                check_query = ''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{}' '''
                cur.execute(check_query.format(args[1]))
                if cur.fetchone()[0] != 1:
                    raise MissingTable("The table {} does not exist.".format(args[1]))
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


class MissingTable(Exception):
    """Raise for trying to access missing table."""

