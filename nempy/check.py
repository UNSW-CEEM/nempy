import math


def energy_bid_ids_exist(func):
    def wrapper(*args):
        if 'energy_units' not in args[0].decision_variables:
            raise ModelBuildError('Market energy constraints cannot be built before unit energy constraints.')
        func(*args)
    return wrapper


def pre_dispatch(func):
    def wrapper(*args):
        if 'energy_units' in args[0].constraints_lhs_coefficients and 'energy_bids' not in \
                args[0].objective_function_components:
            raise ModelBuildError('No unit energy bids provided.')
        if 'energy_units' in args[0].constraints_lhs_coefficients and 'energy_market' not in \
                args[0].market_constraints_lhs_coefficients:
            raise ModelBuildError('No energy market constraints provided.')
        func(*args)
    return wrapper


def one_one_row_per_unit(func):
    def wrapper(*args):
        if len(args[1].index) != len(args[1]['unit'].unique()):
            InputError('Unit DataFrames should only have one entry for each unit.')
        func(*args)
    return wrapper


def one_one_row_per_region(func):
    def wrapper(*args):
        if len(args[1].index) != len(args[1]['region'].unique()):
            InputError('Region DataFrames should only have one entry for each region.')
        func(*args)
    return wrapper


class ModelBuildError(Exception):
    """Raise for building model components in wrong order."""


class InputError(Exception):
    """Raise for incorrect inputs"""


