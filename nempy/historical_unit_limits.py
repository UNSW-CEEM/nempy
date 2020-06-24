


def fast_start_mode_one_constraints(fast_start_profile):
    dispatch_interval = 5
    units_ending_in_mode_one = \
        fast_start_profile[(fast_start_profile['CurrentMode'] == 1) &
                           (fast_start_profile['CurrentModeTime'] +
                            dispatch_interval <= fast_start_profile['T1'])]
    units_ending_in_mode_one['max'] = 0.0
    units_ending_in_mode_one['min'] = 0.0
    units_ending_in_mode_one = units_ending_in_mode_one.loc[:, ['DUID', 'min', 'max']]
    units_ending_in_mode_one = units_ending_in_mode_one.rename(columns={'DUID': 'unit'})
    return units_ending_in_mode_one


def fast_start_mode_two_constraints(fast_start_profile):
    dispatch_interval = 5
    units_ending_in_mode_two = \
        fast_start_profile[(fast_start_profile['CurrentMode'] == 2) &
                           (fast_start_profile['CurrentModeTime'] +
                            dispatch_interval <= fast_start_profile['T2'])]
    units_ending_in_mode_two['target'] = (((units_ending_in_mode_two['CurrentModeTime'] +
                                           dispatch_interval) / units_ending_in_mode_two['T2']) *
                                          units_ending_in_mode_two['MinLoadingMW'])
    units_ending_in_mode_two['min'] = units_ending_in_mode_two['target']
    units_ending_in_mode_two['max'] = units_ending_in_mode_two['target']
    units_ending_in_mode_two = units_ending_in_mode_two.loc[:, ['DUID', 'min', 'max']]
    units_ending_in_mode_two = units_ending_in_mode_two.rename(columns={'DUID': 'unit'})
    return units_ending_in_mode_two


def fast_start_mode_three_constraints(fast_start_profile):
    dispatch_interval = 5
    units_ending_in_mode_three = \
        fast_start_profile[(fast_start_profile['CurrentMode'] == 3) &
                           (fast_start_profile['CurrentModeTime'] +
                            dispatch_interval <= fast_start_profile['T3'])]
    units_ending_in_mode_three['min'] = units_ending_in_mode_three['target']
    units_ending_in_mode_three = units_ending_in_mode_three.loc[:, ['DUID', 'min']]
    units_ending_in_mode_three = units_ending_in_mode_three.rename(columns={'DUID': 'unit'})
    return units_ending_in_mode_three


def fast_start_mode_four_constraints(fast_start_profile):
    dispatch_interval = 5
    units_ending_in_mode_four = \
        fast_start_profile[(fast_start_profile['CurrentMode'] == 4) &
                           (fast_start_profile['CurrentModeTime'] +
                            dispatch_interval <= fast_start_profile['T2'])]
    units_ending_in_mode_four['target'] = (units_ending_in_mode_four['MinLoadingMW'] -
                                           (((units_ending_in_mode_four['CurrentModeTime'] +
                                           dispatch_interval) / units_ending_in_mode_four['T4']) *
                                          units_ending_in_mode_four['MinLoadingMW']))
    units_ending_in_mode_four['min'] = units_ending_in_mode_four['target']
    units_ending_in_mode_four['max'] = units_ending_in_mode_four['target']
    units_ending_in_mode_four = units_ending_in_mode_four.loc[:, ['DUID', 'min', 'max']]
    units_ending_in_mode_four = units_ending_in_mode_four.rename(columns={'DUID': 'unit'})
    return units_ending_in_mode_four