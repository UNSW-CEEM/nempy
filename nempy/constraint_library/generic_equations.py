

def mainland_load_relief(system_variables, date_time):
    sv = system_variables

    rhs = (sv['regional_demand']['qld'] + sv['regional_demand']['nsw'] + sv['regional_demand']['vic'] +
           sv['regional_demand']['sa'])

    if date_time < '2020-01-02 00:00:00':
        rhs = rhs * - 0.007
    elif date_time < '2020-01-16 00:00:00':
        rhs = rhs * - 0.006
    else:
        rhs = rhs * - 0.005

    return rhs


def tas_load_relief(system_variables, date_time):
    sv = system_variables

    rhs = sv['regional_demand']['tas'] - sv['imports_after_losses']['tas']

    if date_time < '2020-12-09 00:00:00':
        rhs = rhs * - 0.01
    elif date_time < '2020-12-23 00:00:00':
        rhs = rhs * - 0.009
    else:
        rhs = rhs * - 0.008

    return rhs


def max_of_all_non_aggregated_units_in_nsw(system_variables, date_time):
    sv = system_variables
    units = ['BW01', 'ER03', 'ER02', 'ER01', 'BW04', 'BW03', 'BW02', 'ER04', 'HUMENSW', 'LD01', 'LD02', 'VP6', 'VP5',
             'TALWA1', 'MP2', 'MP1', 'LD04', 'LD03']
    unit_outputs = [sv['unit_outputs'][u] for u in units]
    return max(unit_outputs)


def max_of_all_non_aggregated_units_in_qld(system_variables, date_time):
    sv = system_variables
    units = ['YABULU2', 'MSTUART2', 'MSTUART3', 'OAKEY1', 'OAKEY2', 'ROMA_7', 'ROMA_8', 'STAN-1', 'STAN-2', 'STAN-3',
             'STAN-4', 'SWAN_E', 'TARONG#1', 'TARONG#2', 'TARONG#3', 'TARONG#4', 'TNPS1', 'W/HOE#1', 'W/HOE#2', 'YABULU',
             'MSTUART1', 'MPP_2', 'MACKAYGT', 'MPP_1', 'BRAEMAR3', 'BRAEMAR5', 'BRAEMAR6', 'BRAEMAR7', 'CALL_B_1',
             'CALL_B_2', 'CPP_3', 'CPP_4', 'GSTONE1', 'GSTONE2', 'GSTONE3', 'GSTONE4', 'GSTONE5', 'GSTONE6', 'KAREEYA1',
             'KAREEYA2', 'KAREEYA3', 'KAREEYA4', 'KPP_1', 'BRAEMAR2', 'BRAEMAR1', 'BARRON-1', 'BARCALDN', 'BARRON-2']
    unit_outputs = [sv['unit_outputs'][u] for u in units]
    return max(unit_outputs)


def max_of_all_non_aggregated_units_in_sa(system_variables, date_time):
    """Simplified logic compared to that used in NEMDE."""
    sv = system_variables
    units = ['TORRA1', 'TORRA2', 'TORRA3', 'TORRA4', 'TORRB1', 'TORRB2', 'TORRB3', 'TORRB4', 'POR03',
             'QPS1', 'QPS2', 'QPS3', 'QPS4', 'QPS5', 'OSB-AG', 'PPCCGT', 'DRYCGT1', 'DRYCGT2', 'DRYCGT3',
             'LADBROK1', 'LADBROK2', 'MINTARO']
    unit_outputs = [sv['unit_outputs'][u] for u in units]
    return max(unit_outputs)


def max_of_all_non_aggregated_units_in_vic(system_variables, date_time):
    """Simplified logic compared to that used in NEMDE."""
    sv = system_variables
    units = ['YWPS4', 'YWPS3', 'YWPS2', 'YWPS1', 'NPS', 'LYA4', 'LYA3', 'LYA2', 'LYA1', 'LOYYB2', 'LOYYB1', 'JLB03',
             'JLB02', 'JLB01', 'JLA04', 'JLA03', 'JLA02', 'JLA01', 'HUMEV', 'EILDON2', 'EILDON1', 'DARTM1', 'BDL02',
             'BDL01']
    unit_outputs = [sv['unit_outputs'][u] for u in units]
    return max(unit_outputs)
