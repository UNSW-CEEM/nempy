import pandas as pd
import numpy as np

from nempy.constraint_library import generic_equations as ge


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

    Portland Wind Farm SPD_ID, PTWF_MW, has been replaced with its DUID from the DISPATCH_UNIT_SCADA table, PORTWF.

    """
    sv = system_variables
    rhs = (-1 * sv['APDLOAD'] + sv['PORTWF'] + 0.3 * ge.mainland_load_relief(sv, date_time)
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

    Portland Wind Farm SPD_ID, PTWF_MW, has been replaced with its DUID from the DISPATCH_UNIT_SCADA table, PORTWF.

    """
    sv = system_variables
    rhs = (-1 * sv['APDLOAD'] + sv['PORTWF'] + 1 * ge.mainland_load_relief(sv, date_time)
           + 4 * ge.tas_load_relief(sv, date_time))
    return rhs


def raise_6s_requirement_for_nem_generation_event(system_variables, date_time):
    sv = system_variables
    rhs = max([ge.max_of_all_non_aggregated_units_in_qld(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_nsw(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_vic(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_sa(sv, date_time)])
    rhs += 1 * ge.mainland_load_relief(sv, date_time) + 4 * ge.tas_load_relief(sv, date_time)
    return rhs


def raise_60s_requirement_for_nem_generation_event(system_variables, date_time):
    sv = system_variables
    rhs = max([ge.max_of_all_non_aggregated_units_in_qld(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_nsw(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_vic(sv, date_time),
               ge.max_of_all_non_aggregated_units_in_sa(sv, date_time)])
    rhs += 1 * ge.mainland_load_relief(sv, date_time) + 4 * ge.tas_load_relief(sv, date_time)
    return rhs


def lower_6s_requirement_for_nem_load_event(system_variables, date_time):
    sv = system_variables
    rhs = 400
    rhs += 1 * ge.mainland_load_relief(sv, date_time) + 4 * ge.tas_load_relief(sv, date_time)
    return rhs


def tas_raise_reg_requirement_when_basslink_can_transfer_fcas(system_variables, date_time):
    """Tasmania Raise Regulation Requirement greater than 50 MW, Basslink able transfer FCAS

    This constraint has been simplified by just setting a static requirement of 50 MW.

    """
    return 50


rhs_functions = pd.DataFrame(
    np.array([
        ['F_I+LREG_0210', '2019-05-16 00:00:00', '1', 'historical_rhs_valid'],
        ['F_I+RREG_0220', '2019-05-16 00:00:00', '1', 'historical_rhs_valid'],
        ['F_T_AUFLS2_R6', '2018-05-04 00:00:00', '1', 'historical_rhs_valid'],
        ['F_MAIN+NIL_DYN_RREG', '2019-05-23 00:00:00', '1', mainland_raise_regulation_requirement],
        ['MIN_PROPORTIONAL_R6', '2018-04-30 00:00:00', '1', ensure_minimum_35_mw_proportional_r6_for_tas],
        ['F_I+NIL_APD_TL_L5', '2019-08-22 00:00:00', '1', lower_5m_for_loss_of_apd_potlines],
        ['F_I+NIL_MG_R5', '2019-09-11 00:00:00', '1', raise_5m_for_nem_generation_event],
        ['F_I+NIL_APD_TL_L60', '2019-08-22 00:00:00', '1', lower_60s_for_loss_of_apd_potlines],
        ['T_MRWF_FOS', '2020-01-01 00:00:00', '1', 'historical_rhs_valid'],
        ['#DEVILS_G_E', '2019-08-15 00:00:00', '1', 'historical_rhs_valid'],
        ['F_I+NIL_MG_R6' '2019-08-22 00:00:00', '1', raise_6s_requirement_for_nem_generation_event],
        ['Q_CLCB_851', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_L5', '2017-02-17 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_L6', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_L60', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_LREG', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_R5', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_R6', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_R60', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_CALL_B_1_RREG', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_I+NIL_MG_R60', '2019-08-22 00:00:00', '1', raise_60s_requirement_for_nem_generation_event],
        ['N_DARLSF1_ZERO', '2020-08-10 00:00:00', '1', 'historical_rhs_valid'],
        ['VSML_ZERO', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['SVML_ZERO', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q_MACKAYGT_R5', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['Q_MKG_0', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['V_LYPSB1_ZERO', '2015-09-23 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_L5', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_L6', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_L60', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_LREG', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_R5', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_R6', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_R60', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_LOYYB1_RREG', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['Q_TRNTR_8828', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_I+ML_L6_0400', '2019-08-22 00:00:00', '1', lower_6s_requirement_for_nem_load_event],
        ['F_T++RREG_0050', '2015-01-29 00:00:00', '1', tas_raise_reg_requirement_when_basslink_can_transfer_fcas],
        ['F_V_YWPS1_R5', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS1_R6', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS1_R60', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS1_RREG', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS3_R5', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS3_R6', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS3_R60', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_YWPS3_RREG', '2013-08-21 00:00:00', '1', 'historical_rhs_valid'],
        ['N_X_MBTE2_B', '2013-11-25 00:00:00', '1', 'historical_rhs_valid'],
        ['SBEM2B', '2019-07-03 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_APD01_L5', '2020-02-03 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_APD01_L6', '2020-02-03 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_APD01_L60', '2020-02-03 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_APD01_R5', '2020-02-03 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_APD01_R6', '2020-02-03 00:00:00', '1', 'historical_rhs_valid'],
        ['F_V_APD01_R60', '2020-02-03 00:00:00', '1', 'historical_rhs_valid'],
        ['#MARYRSF1_E', '2020-04-21 00:00:00', '1', 'historical_rhs_valid'],
        ['N_BROKENHSF_FLT_26', '2020-04-21 00:00:00', '1', 'historical_rhs_valid'],
        ['F_Q++MUTW_L6', '2019-09-10 00:00:00', '1', 'historical_valid_if_qld_and_nsw_demand_unchanged']
    ])
)