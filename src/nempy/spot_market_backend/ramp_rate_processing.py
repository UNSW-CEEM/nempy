import pandas as pd
import numpy as np

from nempy.help_functions import helper_functions as hf


def _calculate_composite_ramp_rates(ramp_rates, dispatch_interval, bidirectional_units):
    not_bidirectional_units = ramp_rates[~(ramp_rates['unit'].isin(bidirectional_units))].copy()
    bidirectional_units = ramp_rates[ramp_rates['unit'].isin(bidirectional_units)].copy()

    bidirectional_gen = bidirectional_units[bidirectional_units['dispatch_type'] == "generator"].copy()
    bidirectional_load = bidirectional_units[bidirectional_units['dispatch_type'] == "load"].copy()

    bidirectional_gen_ramp_up = bidirectional_gen.loc[:, ['unit', 'ramp_up_rate']]
    bidirectional_load_ramp_down = bidirectional_load.loc[:, ['unit', 'ramp_down_rate', 'initial_output']]
    bidirectional_ramp_up = pd.merge(bidirectional_gen_ramp_up, bidirectional_load_ramp_down, on='unit')

    def calc_composite_ramp_up_rate(row):
        interval_length_in_hours = (dispatch_interval / 60)
        if row["initial_output"] >= 0.0:
            rr = row['ramp_up_rate']
        elif row['ramp_down_rate'] == 0.0:
            rr = 0.0
        elif abs(row['initial_output'] / row['ramp_down_rate']) >= interval_length_in_hours:
            rr = row['ramp_down_rate']
        else:
            time_to_ramp_after_zero = (interval_length_in_hours - abs(row['initial_output'] / row['ramp_down_rate']))
            total_ramp_above_zero = time_to_ramp_after_zero * row['ramp_up_rate']
            rr = (total_ramp_above_zero - row['initial_output']) / interval_length_in_hours
        return rr

    bidirectional_ramp_up['ramp_up_rate'] = bidirectional_ramp_up.apply(
        lambda row: calc_composite_ramp_up_rate(row.to_dict()), axis=1)

    bidirectional_load_ramp_up = bidirectional_load.loc[:, ['unit', 'ramp_up_rate']]
    bidirectional_gen_ramp_down = bidirectional_gen.loc[:, ['unit', 'ramp_down_rate', 'initial_output']]
    bidirectional_ramp_down = pd.merge(bidirectional_load_ramp_up, bidirectional_gen_ramp_down, on='unit')

    def calc_composite_ramp_down_rate(row):
        interval_length_in_hours = (dispatch_interval / 60)
        if row["initial_output"] <= 0.0:
            rr = row['ramp_up_rate']
        elif row['ramp_down_rate'] == 0.0:
            rr = 0.0
        elif abs(row['initial_output'] / row['ramp_down_rate']) >= interval_length_in_hours:
            rr = row['ramp_down_rate']
        else:
            time_to_ramp_after_zero = (interval_length_in_hours - abs(row['initial_output'] / row['ramp_down_rate']))
            total_ramp_below_zero = time_to_ramp_after_zero * row['ramp_up_rate']
            rr = (total_ramp_below_zero + row['initial_output']) / interval_length_in_hours
        return rr

    bidirectional_ramp_down['ramp_down_rate'] = bidirectional_ramp_down.apply(
        lambda row: calc_composite_ramp_down_rate(row.to_dict()), axis=1)

    bidirectional_units = pd.merge(
        bidirectional_ramp_down.loc[:, ["unit", "ramp_down_rate"]],
        bidirectional_ramp_up.loc[:, ["unit", "ramp_up_rate", "initial_output"]],
        on="unit",
    )

    return not_bidirectional_units, bidirectional_units


def _adjust_ramp_rates_for_fast_start_profiles(ramp_rates, run_type, fast_start_profiles, dispatch_interval):
    if run_type == 'fast_start_first_run':
        ramp_rates = _remove_fast_start_units_starting_in_mode_0_1_2(ramp_rates, fast_start_profiles)
    elif run_type == 'fast_start_second_run':
        ramp_rates = _remove_fast_start_units_ending_in_mode_0_1_2(ramp_rates, fast_start_profiles)
        ramp_rates = _adjust_ramp_rates_of_units_ending_in_mode_three_and_four(
            ramp_rates, fast_start_profiles, dispatch_interval)
    elif run_type != 'no_fast_start_units':
        raise ValueError("run_type provided not recognised.")
    return ramp_rates


def _adjust_for_scada_ramp_rates(ramp_rates, scada_ramp_rates):
    ramp_rates = pd.merge(ramp_rates, scada_ramp_rates, on='unit', how="left")
    if "scada_ramp_down_rate" in ramp_rates.columns:
        ramp_rates['ramp_down_rate'] = np.fmin(ramp_rates['ramp_down_rate'], ramp_rates['scada_ramp_down_rate'])
        ramp_rates = ramp_rates.drop(columns=['scada_ramp_down_rate'])
    if "scada_ramp_up_rate" in ramp_rates.columns:
        ramp_rates['ramp_up_rate'] = np.fmin(ramp_rates['ramp_up_rate'], ramp_rates['scada_ramp_up_rate'])
        ramp_rates = ramp_rates.drop(columns=['scada_ramp_up_rate'])
    return ramp_rates


def _remove_fast_start_units_starting_in_mode_0_1_2(ramp_rates, fast_start_profiles):
    units_starting_in_mode_0_1_2 = list(
        fast_start_profiles[fast_start_profiles['current_mode'].isin([0, 1, 2])]['unit'].unique())
    dataframe = ramp_rates[~ramp_rates['unit'].isin(units_starting_in_mode_0_1_2)]
    return dataframe


def _remove_fast_start_units_ending_in_mode_0_1_2(ramp_rates, fast_start_profiles):
    units_starting_in_mode_0_1_2 = list(
        fast_start_profiles[fast_start_profiles['end_mode'].isin([0, 1, 2])]['unit'].unique())
    ramp_rates = ramp_rates[~ramp_rates['unit'].isin(units_starting_in_mode_0_1_2)]
    return ramp_rates


def _adjust_ramp_rates_of_units_ending_in_mode_three_and_four(ramp_rates, fast_start_profiles, dispatch_interval):
    """
    If a unit is ending in mode three of four but it has been less than 5 minutes since leaving mode 2 or 1 then
    adjust their ramp rate to account for the limited time operating without a dispatch inflexibility profile
    upper bound
    """
    if not fast_start_profiles.empty:
        profiles_to_adjust = fast_start_profiles[~fast_start_profiles['time_since_end_of_mode_two'].isna()]
        profiles_to_adjust = profiles_to_adjust.loc[:, ['unit', 'min_loading', 'time_since_end_of_mode_two']]
        profiles_to_adjust = pd.merge(ramp_rates, profiles_to_adjust, 'inner', on='unit')
        profiles_to_adjust['ramp_mw_per_min'] = profiles_to_adjust['ramp_up_rate'] / 60
        profiles_to_adjust['ramp_max'] = profiles_to_adjust['time_since_end_of_mode_two'] * \
                                               profiles_to_adjust['ramp_mw_per_min'] + \
                                               profiles_to_adjust['min_loading']
        profiles_to_adjust['ramp_up_rate'] = (profiles_to_adjust['ramp_max'] -
                                            profiles_to_adjust['initial_output']) * \
                                            (60 / dispatch_interval)
        profiles_to_adjust = profiles_to_adjust.drop(
            columns=["min_loading", "time_since_end_of_mode_two", "ramp_mw_per_min",
                     "ramp_max"])
        ramp_rates_not_adjusted = ramp_rates[~ramp_rates['unit'].isin(profiles_to_adjust['unit'])]
        ramp_rates = pd.concat([profiles_to_adjust, ramp_rates_not_adjusted])
    return ramp_rates


def _bidirectional_ramp_constraints(ramp_rates, type, next_constraint_id, dispatch_interval):
    rhs_and_type = ramp_rates.drop_duplicates('unit').copy()
    if type == 'up':
        rhs_and_type['rhs'] = ramp_rates['initial_output'] + ramp_rates['ramp_up_rate'] * (dispatch_interval / 60)
        rhs_and_type['type'] = "<="
    else:
        rhs_and_type['rhs'] = ramp_rates['initial_output'] - ramp_rates['ramp_down_rate'] * (dispatch_interval / 60)
        rhs_and_type['type'] = ">="
    rhs_and_type = hf.save_index(rhs_and_type.reset_index(drop=True), 'constraint_id', next_constraint_id)
    rhs_and_type['dispatch_type'] = 'bidirectional'
    rhs_and_type['service'] = 'energy'
    rhs_and_type = rhs_and_type.loc[:, ['unit', 'service', 'dispatch_type', 'constraint_id', 'type', 'rhs']]

    variable_map = ramp_rates.loc[:, ['unit']]
    variable_map['coefficient'] = 1.0
    variable_map = pd.merge(
        variable_map,
        rhs_and_type.loc[:, ['unit', 'service', 'constraint_id']],
        on=['unit']
    )

    variable_map_load = variable_map.copy()
    variable_map_load["dispatch_type"] = "load"

    variable_map_gen = variable_map.copy()
    variable_map_gen["dispatch_type"] = "generator"

    variable_map = pd.concat([
        variable_map_load,
        variable_map_gen
    ])

    variable_map = variable_map.loc[:, ['constraint_id', 'unit', 'service', 'dispatch_type', 'coefficient']]
    return rhs_and_type, variable_map