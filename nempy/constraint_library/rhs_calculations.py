import pandas as pd
import numpy as np

from nempy.constraint_library import generic_equations as ge


def global_lower_regulation_requirement(system_variables, date_time):
    """Global Lower Regulation requirement."""
    return 210


def global_raise_regulation_requirement(system_variables, date_time):
    """Global Lower Regulation requirement."""
    return 220


def limit_raise_6_second_dispatch_from_AUFLS2(system_variables, date_time):
    """Limit R6 dispatch from AUFLS2 load based on load armed for shedding.

    TAS AUFLS2 control scheme. Limit R6 enablement based on loaded armed for shedding by scheme."""
    sv = system_variables
    return sv['AUFLS2_MAX_R6'] + 0.001


def mainland_raise_regulation_requirement(system_variables, date_time):
    """Mainland Raise Regulation Requirement, Feedback in Dispatch, increase by 60 MW for each 1s of time error below -1.5s.

    Dropped time error dependency.

    """
    return 220


def ensure_minimum_35_mw_proportional_r6_for_tas(system_variables, date_time):
    """Ensure a minimum of 35 MW of proportional frequency control available to TAS region.

    Dropped time error dependency.

    """
    return 35


def lower_5m_for_loss_of_apd_potlines(system_variables, date_time):
    """Lower 5 min Service Requirement for the loss of APD potlines due to undervoltage following a fault on
    MOPS-HYTS-APD 500 kV line

    """
    sv = system_variables
    rhs = (-1 * sv['APDLOAD'] + sv['PTWF_MW'] + 0.3 * ge.mainland_load_relief(sv, date_time)
           + 0.3 * ge.tas_load_relief(sv, date_time))
    return rhs


def raise_5m_for_nem_generation_event(system_variables, date_time):
    """Raise 5 min requirement for a NEM Generation Event"""
    sv = system_variables
    rhs = max([ge.max_of_all_non_aggregated_units_in_qld(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_nsw(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_vic(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_sa(sv, date_time)])
    rhs += 0.3 * ge.mainland_load_relief(sv, date_time) + 0.3 * ge.tas_load_relief(sv, date_time)
    return rhs


def lower_60s_for_loss_of_apd_potlines(system_variables, date_time):
    """Lower 60 sec Service Requirement for the loss of APD potlines due to undervoltage following a fault on
    MOPS-HYTS-APD 500 kV line

    """
    sv = system_variables
    rhs = (-1 * sv['APDLOAD'] + sv['PTWF_MW'] + 1 * ge.mainland_load_relief(sv, date_time)
           + 4 * ge.tas_load_relief(sv, date_time))
    return rhs


rhs_functions = pd.DataFrame(
    np.array([
        ['F_I+LREG_0210', '2019-05-16 00:00:00', '1', global_lower_regulation_requirement],
        ['F_I+RREG_0220', '2019-05-16 00:00:00', '1', global_raise_regulation_requirement],
        ['F_T_AUFLS2_R6', '2018-05-04 00:00:00', '1', limit_raise_6_second_dispatch_from_AUFLS2],
        ['F_MAIN+NIL_DYN_RREG', '2019-05-23 00:00:00', '1', mainland_raise_regulation_requirement],
        ['MIN_PROPORTIONAL_R6', '2018-04-30 00:00:00', '1', ensure_minimum_35_mw_proportional_r6_for_tas],
        ['F_I+NIL_APD_TL_L5', '2019-08-22 00:00:00', '1', lower_5m_for_loss_of_apd_potlines],
        ['F_I+NIL_MG_R5', '2019-09-11 00:00:00', '1', raise_5m_for_nem_generation_event],
        ['F_I+NIL_APD_TL_L60', '2019-08-22 00:00:00', '1', lower_60s_for_loss_of_apd_potlines]
    ])
)