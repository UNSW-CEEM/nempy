import numpy as np

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


def pre_dispatch(func):
    @keep_details(func)
    def wrapper(*args):
        if 'energy_units' in args[0].constraints_lhs_coefficients and 'energy_bids' not in \
                args[0].objective_function_components:
            raise ModelBuildError('No unit energy bids provided.')
        if 'energy_units' in args[0].constraints_lhs_coefficients and 'energy_market' not in \
                args[0].market_constraints_lhs_coefficients:
            raise ModelBuildError('No energy market constraints provided.')
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
                    if column in dtypes and dtypes[column] == str and not hasattr(args[1][column], 'str'):
                        raise ColumnDataTypeError('Column {} in {} should have type str'.format(column, name))
                    elif column not in dtypes and dtypes['else'] != args[1][column].dtype:
                        raise ColumnDataTypeError('Column {} in {} should have type {}'.format(column, name,
                                                                                               dtypes['else']))
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


def column_values(name, cols_to_check):
    def decorator(func):
        @keep_details(func)
        def wrapper(*args):
            if args[0].check:
                for column in cols_to_check:
                    if column not in args[1].columns:
                        continue
                    if np.inf in args[1][column].values:
                        raise UnexpectedColumn("Value inf not allowed in column '{}' in {}.".format(column, name))
                    if args[1][column].min() < 0.0:
                        raise UnexpectedColumn("Negative values not allowed in column '{}' in {}.".\
                                               format(column, name))
                    if args[1][column].isnull().any():
                        raise UnexpectedColumn("Null values not allowed in column '{}' in {}.".\
                                               format(column, name))
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
    """Raise for required column missing"""


class UnexpectedColumn(Exception):
    """Raise for unexpected column"""


