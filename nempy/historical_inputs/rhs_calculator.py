import numpy as np
import pandas as pd

from nempy.historical_inputs import xml_cache


class RHSCalc:
    def __init__(self, xml_cache_manager):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)

        Parameters
        ----------
        xml

        Returns
        -------

        """
        inputs = xml_cache_manager.xml['NEMSPDCaseFile']['NemSpdInputs']
        self.scada_data = self._reformat_scada_data(inputs['ConstraintScadaDataCollection']['ConstraintScadaData'])
        self.generic_equations = self._format_generic_equations(inputs['GenericEquationCollection']['GenericEquation'])
        self.rhs_constraint_equations = self._format_rhs_constraint_equations(
            inputs['GenericConstraintCollection']['GenericConstraint'])
        self.unit_initial_mw = self._format_initial_conditions(xml_cache_manager.get_unit_initial_conditions())
        self.entered_values = (
            self._format_entered_values(inputs['PeriodCollection']['Period']['EnteredValuePeriodCollection']['EnteredValuePeriod']))
        self.msnsp_from_availbility, self.msnsp_to_availbility = (
            self._format_mnsp_availability(inputs['PeriodCollection']['Period']['InterconnectorPeriodCollection']
                                           ['InterconnectorPeriod']))
        self.nemde_rhs_values = self._format_nemde_rhs_values(xml_cache_manager.get_constraint_rhs())
        self._resolved_values = {}

    @staticmethod
    def _reformat_scada_data(scada_data):
        new_format = {}

        def add_entry(new_format_dict, type, entry, good_data):
            if entry['@SpdID'] not in new_format_dict[type]:
                new_format_dict[type][entry['@SpdID']] = []
            new_entry = {k: v for (k, v) in entry.items() if k != '@SpdID'}
            new_entry['@GoodValues'] = good_data
            new_format_dict[type][entry['@SpdID']].append(new_entry)

        for scada_type_set in scada_data:
            new_format[scada_type_set['@SpdType']] = {}
            if type(scada_type_set['ScadaValuesCollection']['ScadaValues']) == list:
                for entry in scada_type_set['ScadaValuesCollection']['ScadaValues']:
                    add_entry(new_format, scada_type_set['@SpdType'], entry, True)
            else:
                entry = scada_type_set['ScadaValuesCollection']['ScadaValues']
                add_entry(new_format, scada_type_set['@SpdType'], entry, True)

            if 'BadScadaValuesCollection' in scada_type_set:
                if type(scada_type_set['BadScadaValuesCollection']['ScadaValues']) == list:
                    for entry in scada_type_set['BadScadaValuesCollection']['ScadaValues']:
                        add_entry(new_format, scada_type_set['@SpdType'], entry, False)
                else:
                    entry = scada_type_set['BadScadaValuesCollection']['ScadaValues']
                    add_entry(new_format, scada_type_set['@SpdType'], entry, False)

        return new_format

    @staticmethod
    def _format_generic_equations(generic_equations):
        new_format = {}
        for entry in generic_equations:
            if 'RHSTermCollection' in entry:
                terms = entry['RHSTermCollection']['RHSTerm']
                if type(terms) != list:
                    terms = [terms]
                new_format[entry['@EquationID']] = terms
        return new_format

    @staticmethod
    def _format_rhs_constraint_equations(constraints):
        new_format = {}
        for entry in constraints:
            if 'RHSTermCollection' in entry:
                terms = entry['RHSTermCollection']['RHSTerm']
                if type(terms) != list:
                    terms = [terms]
                new_format[entry['@ConstraintID']] = terms
        return new_format

    @staticmethod
    def _format_initial_conditions(initial_conditions):
        return initial_conditions.set_index('DUID')['INITIALMW'].to_dict()

    @staticmethod
    def _format_nemde_rhs_values(constraints):
        return constraints.set_index('set')['rhs'].to_dict()

    @staticmethod
    def _format_entered_values(entered_values):
        new_format = {}
        for element in entered_values:
            new_format[element['@SpdID']] = element['@Value']
        return new_format

    @staticmethod
    def _format_mnsp_availability(interconnectors):
        from_availabilities = {}
        to_availabilities = {}
        for inter in interconnectors:
            if inter['@MNSP'] == '1':
                for offer in inter['MNSPOfferCollection']['MNSPOffer']:
                    if offer['@RegionID'] == inter['@FromRegion']:
                        from_availabilities[inter['@InterconnectorID']] = offer['@MaxAvail']
                    elif offer['@RegionID'] == inter['@ToRegion']:
                        to_availabilities[inter['@InterconnectorID']] = offer['@MaxAvail']
                    else:
                        raise ValueError('Interconnector direction mismatch.')
        return from_availabilities, to_availabilities

    def get_rhs_equations_that_dont_reference_generic_equations(self):
        equations_to_return = []
        for equation in self.rhs_constraint_equations.keys():
            references_generic = False
            for term in self.rhs_constraint_equations[equation]:
                if term['@SpdID'] in self.generic_equations.keys():
                    references_generic = True
            if not references_generic:
                equations_to_return.append(equation)
        return equations_to_return

    def get_nemde_rhs(self, constraint_id):
        return float(self.nemde_rhs_values[constraint_id])

    def compute_constraint_rhs(self, constraint_id):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager.xml)
        >>> rhs_calculator.compute_constraint_rhs('X_BASSLINK_OFF')

        Parameters
        ----------
        xml

        Returns
        -------

        """
        if type(constraint_id) == list:
            equation = self.rhs_constraint_equations[constraint_id]
            equation = self._resolve_term_values(equation)
            rhs = rpn_calc(equation)
        else:
            rhs = []
            for id in constraint_id:
                equation = self.rhs_constraint_equations[id]
                equation = self._resolve_term_values(equation)
                rhs.append(rpn_calc(equation))
            rhs = pd.DataFrame({
                'set': constraint_id,
                'rhs': rhs
            })
        return rhs

    def get_rhs_constraint_equations_that_depend_value(self, spd_id, type):
        """
        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)
        >>> rhs_calculator.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W')
        ['F_MAIN++APD_TL_L5', 'F_MAIN++APD_TL_L6', 'F_MAIN++APD_TL_L60', 'F_MAIN++ML_L5_0400', 'F_MAIN++ML_L5_APD', 'F_MAIN++ML_L60_0400', 'F_MAIN++ML_L60_APD', 'F_MAIN++ML_L6_0400', 'F_MAIN++ML_L6_APD', 'F_MAIN++NIL_DYN_LREG', 'F_MAIN++NIL_DYN_RREG', 'F_MAIN++NIL_MG_R5', 'F_MAIN++NIL_MG_R6', 'F_MAIN++NIL_MG_R60', 'F_MAIN+APD_TL_L5', 'F_MAIN+APD_TL_L6', 'F_MAIN+APD_TL_L60', 'F_MAIN+ML_L5_0400', 'F_MAIN+ML_L5_APD', 'F_MAIN+ML_L60_0400', 'F_MAIN+ML_L60_APD', 'F_MAIN+ML_L6_0400', 'F_MAIN+ML_L6_APD', 'F_MAIN+NIL_DYN_LREG', 'F_MAIN+NIL_DYN_RREG', 'F_MAIN+NIL_MG_R5', 'F_MAIN+NIL_MG_R6', 'F_MAIN+NIL_MG_R60', 'F_T++LREG_0050', 'F_T++NIL_BB_TG_R5', 'F_T++NIL_BB_TG_R6', 'F_T++NIL_BB_TG_R60', 'F_T++NIL_MG_R5', 'F_T++NIL_MG_R6', 'F_T++NIL_MG_R60', 'F_T++NIL_ML_L5', 'F_T++NIL_ML_L6', 'F_T++NIL_ML_L60', 'F_T++NIL_TL_L5', 'F_T++NIL_TL_L6', 'F_T++NIL_TL_L60', 'F_T++NIL_WF_TG_R5', 'F_T++NIL_WF_TG_R6', 'F_T++NIL_WF_TG_R60', 'F_T++RREG_0050', 'F_T+LREG_0050', 'F_T+NIL_BB_TG_R5', 'F_T+NIL_BB_TG_R6', 'F_T+NIL_BB_TG_R60', 'F_T+NIL_MG_R5', 'F_T+NIL_MG_R6', 'F_T+NIL_MG_R60', 'F_T+NIL_ML_L5', 'F_T+NIL_ML_L6', 'F_T+NIL_ML_L60', 'F_T+NIL_TL_L5', 'F_T+NIL_TL_L6', 'F_T+NIL_TL_L60', 'F_T+NIL_WF_TG_R5', 'F_T+NIL_WF_TG_R6', 'F_T+NIL_WF_TG_R60', 'F_T+RREG_0050', 'T_V_NIL_BL1', 'V_T_NIL_BL1']

        Parameters
        ----------
        spd_id
        type

        Returns
        -------

        """
        dependent_generic_equations = []
        for equation_id, equation in self.generic_equations.items():
            for term in equation:
                if (term["@SpdID"] == spd_id and term['@SpdType'] == type and equation_id not in
                        dependent_generic_equations):
                    dependent_generic_equations.append(equation_id)

        dependent_rhs_equations = []
        for equation_id, equation in self.rhs_constraint_equations.items():
            for term in equation:
                if ((term["@SpdID"] == spd_id and term['@SpdType'] == type and equation_id not in
                     dependent_rhs_equations) or (term['@SpdID'] in dependent_generic_equations and
                                                  term['@SpdType'] == 'X')):
                    dependent_rhs_equations.append(equation_id)

        return dependent_rhs_equations

    def update_spd_id_value(self, spd_id, type, value):
        if type in ['C', 'R', 'X']:
            raise ValueError('Spd term values of type C can\'t be updated')
        elif type in ['A', 'S', 'I', 'W']:
            if len(self.scada_data[type][spd_id]) > 1:
                raise ValueError('SPD ID and type has more than one value, update not possible.')
            else:
                self.scada_data[type][spd_id][0] = value
        elif spd_id in self.unit_initial_mw and type == 'T':
            self.unit_initial_mw[spd_id] = value
        elif spd_id in self.entered_values and type == 'E':
            self.entered_values[spd_id] = value
        elif spd_id in self.msnsp_from_availbility and type == 'M':
            self.msnsp_from_availbility[spd_id] = value
        elif spd_id in self.msnsp_to_availbility and type == 'N':
            self.msnsp_to_availbility[spd_id] = value
        else:
            raise ValueError('SPD ID could not be found, please check the ID and type provide exist in the raw '
                             'XML file.')

    def _resolve_term_values(self, equation):
        for term in equation:
            if '@Value' not in term:
                value, default_flag = self._resolve_term_value_new(term)
                if value is not None:
                    term["@Value"] = value
        return equation

    def _resolve_term_value(self, term):
        spd_id = term['@SpdID']
        default_flag = False
        if spd_id in self.generic_equations:
            value = self._compute_generic_equation(spd_id)
        elif spd_id in self.scada_data:
            value = self.scada_data[spd_id]['@Value']
            if not self.scada_data[spd_id]['@GoodValues']:
                raise ValueError('Scada data not good value')
        elif spd_id in self.unit_initial_mw:
            value = self.unit_initial_mw[spd_id]
        elif spd_id in self.entered_values:
            value = self.entered_values[spd_id]
        elif '@Default' in term:
            # if term['@SpdType'] == 'U':
            #     value = term['@Default']
            # else:
            #     value = 1
            value = None
            default_flag = True

        else:
            raise ValueError('Equation value could not be resolved.')
        return value, default_flag

    def _resolve_term_value_new(self, term):
        default_flag = False
        if term['@SpdType'] == 'C':
            value = None
        elif term['@SpdType'] in ['A', 'S', 'R', 'I', 'W']:
            scadas = self.scada_data[term['@SpdType']][term['@SpdID']]
            if len(scadas) > 0:
                value = 0
                for scada in scadas:
                    if scada['@Can_Use_Value'] == 'False':
                        raise ValueError("Bad SCADA value")
                    value += float(scada['@Value'])
        elif term['@SpdID'] in self.unit_initial_mw and term['@SpdType'] == 'T':
            value = self.unit_initial_mw[term['@SpdID']]
        elif term['@SpdID'] in self.entered_values and term['@SpdType'] == 'E':
            value = self.entered_values[term['@SpdID']]
        elif term['@SpdID'] in self.generic_equations and term['@SpdType'] == 'X':
            value = self._compute_generic_equation(term['@SpdID'])
        elif term['@SpdID'] in self.msnsp_from_availbility and term['@SpdType'] == 'M':
            value = self.msnsp_from_availbility[term['@SpdID']]
        elif term['@SpdID'] in self.msnsp_to_availbility and term['@SpdType'] == 'N':
            value = self.msnsp_to_availbility[term['@SpdID']]
        else:
            value = None
            default_flag = True
        return value, default_flag

    def _compute_generic_equation(self, equation_id):
        """

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager.xml)
        >>> rhs_calculator._compute_generic_equation('X_BASSLINK_OFF')

        Parameters
        ----------
        xml

        Returns
        -------

        """
        equation = self.generic_equations[equation_id]
        equation = self._resolve_term_values(equation)
        rhs = rpn_calc(equation)
        return rhs


def rpn_stack(equation, full_equation=None):
    stack = [0.0]
    ignore_groups = []
    # group_result = None
    multi_term_operators = ['ADD', 'SUB', 'MUL', 'DIV', 'MAX', 'MIN']
    skip_next_term = False
    pop_flag = False
    # equation = remove_redundant_group_terms(equation)
    # equation = move_spd_type_g_to_top_of_group(equation)
    clear_group_values(equation)
    # ignore_group = None
    for i, term in enumerate(equation):

        if (term['@SpdType'] == 'G' and '@Value' not in term and
                ('@GroupTerm' not in term or term['@GroupTerm'] not in ignore_groups)):
            # Groups are evaluate separately with their own stack. If a group exists we extract it from the main
            # equation calculate the result for the group and then save the group id in a list of groups to ignore
            # so that terms in this group are skipped in future iterations of the for loop.
            remove_first_member = False
            if '@GroupTerm' not in term:
                if '@GroupTerm' in equation[i+1]:
                    group_id = equation[i+1]['@GroupTerm']
                else:
                    continue
            else:
                group_id = term['@GroupTerm']
                remove_first_member = True
            group = collect_group(equation, group_id)
            if remove_first_member:
                group.pop(0)
            group_result = rpn_calc(group, equation)
            term['@Value'] = group_result
            ignore_groups.append(group_id)
            sub_groups_ids = get_sub_groups(group)
            ignore_groups += sub_groups_ids
            # ignore_group = group_id

        if '@GroupTerm' in term and term['@GroupTerm'] not in ignore_groups:
            group_id = term['@GroupTerm']
            group = collect_group(equation, group_id)
            # group_has_spd_type_term = False
            # for group_term in group:
            #     if group_term['@SpdType'] == 'G':
            #         group_has_spd_type_term = True
            # if not group_has_spd_type_term:
            group_result = rpn_calc(group, equation)
            if equation[i + len(group)]['@SpdType'] == 'G':
                equation[i + len(group)]['@Value'] = group_result
            else:
                stack[0] += group_result
            ignore_groups.append(term['@GroupTerm'])
            sub_groups_ids = get_sub_groups(group)
            ignore_groups += sub_groups_ids

        if skip_next_term:
            # If the last term was combined with a multi term operation then we skip the multi term operation because it
            # has alread been applied.
            skip_next_term = False
            assert term['@Operation'] in multi_term_operators
            continue
        # elif term['@SpdType'] == 'G':
        #
        #     # ignore_groups.append(term['@GroupID'])
        #     if len(stack) == 0 or ('@Operation' in term and term['@Operation'] == 'PUSH'):
        #         stack.insert(0, group_result * float(term['@Multiplier']))
        #     else:
        #         stack[0] += group_result * float(term['@Multiplier'])
        # # elif '@GroupID' in term and term['@GroupID'] not in ignore_groups:
        #
        #     group = collect_group(equation, term['@GroupID'])
        #     group_result = rpn_calc(group)
        elif '@GroupTerm' not in term:
            # if term['@SpdType'] != 'G':
            #     ignore_group = None
            # if group_result is not None:
            #     # If a group result has just been calculated the next term is treated as a multiplier to be applied to
            #     # the group result. Then the final value is added to the stack. See AEMO Constraint Implementation
            #     # Guidelines section A.3 Groups.
            #     stack.insert(0, group_result * float(term['@Multiplier']))
            #     group_result = None
            if term['@SpdType'] == 'U' and '@Value' not in term and '@Operation' not in term:
                stack = type_u_no_operator(stack, term)
            elif term['@SpdType'] == 'B':
                if full_equation is not None:
                    stack = branching(stack, term, full_equation)
                else:
                    stack = branching(stack, term, equation)
            elif '@Operation' not in term:
                # if (1 == len(equation[i:]) or '@Operation' not in equation[i + 1] or equation[i + 1]['@Operation']
                #         not in multi_term_operators or (equation[i + 1]['@Operation'] in multi_term_operators and
                #                                         equation[i + 1]['@SpdType'] in ['U', 'G'])):
                stack = no_operator(stack, term)
                # elif (1 < len(equation[i:]) and '@Operation' in equation[i + 1]
                #       and equation[i + 1]['@Operation'] in multi_term_operators and equation[i + 1]['@SpdType'] != 'U'
                #       and equation[i + 1]['@SpdType'] != 'G') and False:
                #     # If the next term is a multi term operator then apply that operation to the current term and the
                #     # next term.
                #     if equation[i + 1]['@Operation'] == 'ADD':
                #         add_on_equation(stack, term, equation[i + 1])
                #     if equation[i + 1]['@Operation'] == 'SUB':
                #         subtract_on_equation(stack, term, equation[i + 1])
                #     if equation[i + 1]['@Operation'] == 'MUL':
                #         multipy_on_equation(stack, term, equation[i + 1])
                #     if equation[i + 1]['@Operation'] == 'DIV':
                #         divide_on_equation(stack, term, equation[i + 1])
                #     if equation[i + 1]['@Operation'] == 'MAX':
                #         max_on_equation(stack, term, equation[i + 1])
                #     if equation[i + 1]['@Operation'] == 'MIN':
                #         min_on_equation(stack, term, equation[i + 1])
                #     skip_next_term = True
                # else:
                #     raise ValueError('Undefined RPN behaviour')
            elif term['@Operation'] == 'ADD' and term['@SpdType'] == 'U':
                stack = add_on_stack(stack, term)
            elif term['@Operation'] == 'ADD':
                stack = hybrid_add(stack, term)
            elif term['@Operation'] == 'SUB' and term['@SpdType'] == 'U':
                stack = subtract_on_stack(stack, term)
            elif term['@Operation'] == 'SUB':
                stack = hybrid_subtract(stack, term)
            elif term['@Operation'] == 'MUL' and term['@SpdType'] == 'U':
                stack = multipy_on_stack(stack, term)
            elif term['@Operation'] == 'MUL':
                stack = hybrid_multiply(stack, term)
            elif term['@Operation'] == 'DIV' and term['@SpdType'] == 'U':
                stack = divide_on_stack(stack, term)
            elif term['@Operation'] == 'DIV':
                stack = hybrid_divide(stack, term)
            elif term['@Operation'] == 'MAX' and term['@SpdType'] == 'U':
                stack = max_on_stack(stack, term)
            elif term['@Operation'] == 'MAX':
                stack = hybrid_max(stack, term)
            elif term['@Operation'] == 'MIN' and term['@SpdType'] == 'U':
                stack = min_on_stack(stack, term)
            elif term['@Operation'] == 'MIN':
                stack = hybrid_min(stack, term)
            elif term['@Operation'] == 'STEP':
                stack = step(stack, term)
            elif term['@Operation'] == 'POW2':
                stack = square(stack, term)
            elif term['@Operation'] == 'POW3':
                stack = cube(stack, term)
            elif term['@Operation'] == 'SQRT':
                stack = sqrt(stack, term)
            elif term['@Operation'] == 'ABS':
                stack = absolute_value(stack, term)
            elif term['@Operation'] == 'NEG':
                stack = negation(stack, term)
            elif term['@Operation'] == 'PUSH':
                stack = push(stack, term)
            elif term['@Operation'] == 'DUP' and term['@SpdType'] == 'U':
                stack = duplicate(stack, term)
            elif term['@Operation'] == 'EXCH' and term['@SpdType'] == 'U':
                stack = exchange(stack, term)
            elif term['@Operation'] == 'RSD' and term['@SpdType'] == 'U':
                stack = roll_stack_down(stack, term)
            elif term['@Operation'] == 'RSU' and term['@SpdType'] == 'U':
                stack = roll_stack_up(stack, term)
            elif term['@Operation'] == 'POP':
                pop_flag, stack = pop(stack, term)
            elif term['@Operation'] == 'EXLEZ' and term['@SpdType'] == 'U' and pop_flag:
                stack = exchange_if_less_than_zero(stack, term)
                pop_flag = False
    return stack


def rpn_calc(equation, full_equation=None):
    return rpn_stack(equation, full_equation)[0]


def get_default_if_needed(term):
    if '@Value' not in term:
        return term['@Default']
    else:
        return term['@Value']


def no_operator(stack, term):
    # If there is no operator in the term, and the next term is not a multi term operator then the
    # value of the term has the multiplier applied and is added to the top of the stack. See AEMO
    # Constraint Implementation Guidelines section A.2 No RPN operators.
    if len(stack) == 0:
        stack.insert(0, 0.0)
    if '@Value' in term:
        stack[0] += float(term['@Multiplier']) * float(term['@Value'])
    else:
        stack[0] += float(term['@Multiplier'])
    return stack


def type_u_no_operator(stack, term):
    # If a term is type U and has no operator then the multiplier is applied to the top element of the stack. See AEMO
    # Constraint Implementation Guidelines section A.5 Top stack element.
    if len(stack) == 0:
        stack.insert(0, 0.0)
    stack[0] = stack[0] * float(term['@Multiplier'])
    return stack


def step(stack, term):
    # For terms that are STEP operators if their value is greater than zero result is the value with the
    # multiplier applied otherwise zero is returned. See AEMO Constraint Implementation Guidelines section
    # A.6.1 Step function.
    if term['@SpdType'] == 'U':
        # If type U then apply the STEP operation to the element on top of the stack.
        if float(stack[0]) > 0.0:
            stack[0] = float(term['@Multiplier'])
        else:
            stack[0] = 0.0
    else:
        # If not type U the apply the STEP operation to the term value.
        value = get_default_if_needed(term)
        if float(value) > 0.0:
            value_to_add = float(term['@Multiplier'])
        else:
            value_to_add = 0.0
        if len(stack) > 0:
            stack[0] += value_to_add
        else:
            stack.append(value_to_add)
    return stack


def square(stack, term):
    # For terms that are POW2 operators either the term value or the top stack value is squared. See AEMO Constraint
    # Implementation Guidelines section A.6.2 Square.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 2 * float(term['@Multiplier'])
    else:
        # If not type U the apply the POW2 operation to the term value.
        stack[0] += float(term['@Value']) ** 2 * float(term['@Multiplier'])
    return stack


def cube(stack, term):
    # For terms that are POW3 operators either the term value or the top stack value is cubed. See AEMO Constraint
    # Implementation Guidelines section A.6.3 Cube.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 3 * float(term['@Multiplier'])
    else:
        # If not type U the apply the POW3 operation to the term value.
        stack[0] += float(term['@Value']) ** 3 * float(term['@Multiplier'])
    return stack


def sqrt(stack, term):
    # For terms that are SQRT operators either the term value or the top stack value is square rooted. See AEMO
    # Constraint Implementation Guidelines section A.6.4 Square Root.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 0.5 * float(term['@Multiplier'])
    else:
        # If not type U the apply the SQRT operation to the term value.
        stack[0] += float(term['@Value']) ** 0.5 * float(term['@Multiplier'])
    return stack


def absolute_value(stack, term):
    # For terms that are ABS operators either the absolute value of the term value or the top stack value. See AEMO
    # Constraint Implementation Guidelines section A.6.5 Absolute Value.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = abs(stack[0]) * float(term['@Multiplier'])
    else:
        # If not type U the apply the ABS operation to the term value.
        stack[0] += abs(float(term['@Value'])) * float(term['@Multiplier'])
    return stack


def negation(stack, term):
    # For terms that are NEG operators either the term value or the top stack value is negated. See AEMO Constraint
    # Implementation Guidelines section A.6.5 Negation.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = -1.0 * stack[0] * float(term['@Multiplier'])
    else:
        # If not type U the apply the NEG operation to the term value.
        stack[0] += -1.0 * float(term['@Value']) * float(term['@Multiplier'])
    return stack


def add_on_stack(stack, term):
    # Where an ADD operation is encountered in the equation with no previous term, without an operator, for the
    # ADD operation to act on, then the ADD operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    if len(stack) < 2:
        # raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
        next_top_element = stack[0] * float(term['@Multiplier'])
        # stack.pop(0)
        stack[0] = next_top_element
    else:
        next_top_element = (stack[0] + stack[1]) * float(term['@Multiplier'])
        stack.pop(0)
        stack[0] = next_top_element
    return stack


def hybrid_add(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = (float(value_one) + stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def add_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an ADD operator, then the term value
    # and the ADD operator value are summed and added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) + float(value_two)) * float(next_term['@Multiplier']))
    return stack


def subtract_on_stack(stack, term):
    # Where an SUB operation is encountered in the equation with no previous term, without an operator, for the
    # SUB operation to act on, then the SUB operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] - stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def hybrid_subtract(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = (stack[0] - float(value_one)) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def subtract_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an SUB operator, then
    # SUB operator value is subtracted from the term value and the result added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.2 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) - float(value_two)) * float(next_term['@Multiplier']))
    return stack


def multipy_on_stack(stack, term):
    # Where an MUL (multiply) operation is encountered in the equation with no previous term, without an operator, for
    # the MUL operation to act on, then the MUL operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.3 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] * stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def hybrid_multiply(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = (float(value_one) * stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def multipy_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an MUL operator, then
    # MUL operator value is multiplied with the term value and the result added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.3 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) * float(value_two)) * float(next_term['@Multiplier']))
    # stack[0] += (float(value_one) * float(value_two)) * float(next_term['@Multiplier'])
    return stack


def divide_on_stack(stack, term):
    # Where an DIV (divide) operation is encountered in the equation with no previous term, without an operator, for
    # the DIV operation to act on, then the DIV operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] / stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def hybrid_divide(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = (stack[0] / float(value_one)) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def divide_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an DIV operator, then
    # the term value is divide by DIV operator value and the result added to the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, (float(value_one) / float(value_two)) * float(next_term['@Multiplier']))
    return stack


def max_on_stack(stack, term):
    # Where an MAX (maximum) operation is encountered in the equation with no previous term, without an operator, for
    # the MAX operation to act on, then the MAX operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.5 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = max(stack[1], stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def max_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an MAX operator, then
    # the maximum of the term value and the operator value is taken and added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.5 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, max(float(value_one), float(value_two)) * float(next_term['@Multiplier']))
    return stack


def hybrid_max(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = max(float(value_one), stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def min_on_stack(stack, term):
    # Where an MIN (minimum) operation is encountered in the equation with no previous term, without an operator, for
    # the MIN operation to act on, then the MIN operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.6 Add.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = min(stack[1], stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def hybrid_min(stack, term):
    value_one = get_default_if_needed(term)
    next_top_element = min(float(value_one), stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def min_on_equation(stack, term, next_term):
    # Where a term without an operator, with a value, and with the next term being an MIN operator, then
    # the minimum of the term value and the operator value is taken and added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.6 Add.
    value_one = get_default_if_needed(term)
    value_two = get_default_if_needed(next_term)
    stack.insert(0, min(float(value_one), float(value_two)) * float(next_term['@Multiplier']))
    return stack


def push(stack, term):
    # If a push operator is given the value of the operator with the multiplier applied is added to the top of the
    # stack.
    # See AEMO Constraint Implementation Guidelines section A.8.1 Push.
    if term['@SpdType'] not in ['C']:  # Condition found through empirical testing
        value = get_default_if_needed(term)
        stack.insert(0, float(term['@Multiplier']) * float(value))
    else:
        stack.insert(0, float(term['@Multiplier']))
    return stack


def duplicate(stack, term):
    # If a DUP operator is given the value at the top of the stack is duplicated, the multiplier is applied and
    # the term is added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.8.2 Duplicate.
    stack.insert(0, stack[0] * float(term['@Multiplier']))
    return stack


def exchange(stack, term):
    # If a EXCH operator is given the top and second top elements of the stacked are swapped.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.insert(1, top_element)
    stack.insert(0, stack.pop(0) * float(term['@Multiplier']))
    return stack


def roll_stack_down(stack, term):
    # If a RSD operator is given the bottom element of the stack is moved to the top and the multiplier is applied.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    bottom_element = stack.pop(-1)
    stack.insert(0, bottom_element * float(term['@Multiplier']))
    return stack


def roll_stack_up(stack, term):
    # If a RSU operator is given the top element of the stack is moved to the bottom and the multiplier is applied to
    # the new top element.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.append(top_element)
    top_element = stack.pop(0)
    stack.insert(0, top_element * float(term['@Multiplier']))
    return stack


def pop(stack, term):
    # See AEMO Constraint Implementation Guidelines section A.9.1 and A.9.2.
    if term['@SpdType'] == 'U':
        # If the POP operator is given and the term is of type U the top element of the stack is removed. If the element
        # that was popped was less than of equal to zero than the POP flag is set to true.
        top_element = stack.pop(0)
        if top_element <= 0.0:
            pop_flag = True
        else:
            pop_flag = False
    else:
        # If the POP operator is given and the term is not of type U the top element of the stack is not removed. If
        # the term value is less than zero then the POP flag is set to true.
        if float(term['@Value']) <= 0.0:
            pop_flag = True
        else:
            pop_flag = False
    return pop_flag, stack


def exchange_if_less_than_zero(stack, term):
    # If the EXLEZ is given and the pop flag is true then the top two elements are exchanged.
    # See AEMO Constraint Implementation Guidelines section A.9.2
    if len(stack) < 2:
        return stack
        # raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.insert(1, top_element)
    stack.insert(0, stack.pop(0) * float(term['@Multiplier']))
    return stack


def get_term_by_id(equation, term_id):
    term_to_return = None
    for term in equation:
        if term['@TermID'] == term_id:
            term_to_return = term
    return term_to_return


def branching(stack, term, equation):
    # If a term with type B is given, we use the terms @ParameterTerm1 to retrieve the value of the term in the equation
    # with the matching term ID. If the value of that term is greater than 0.0, then we return the value of the term
    # Specified by @ParameterTerm2 else we return the value specified by @ParameterTerm3.
    # See AEMO Constraint Implementation Guidelines section A.9.3
    term_one = get_term_by_id(equation, term['@ParameterTerm1'])
    term_two = get_term_by_id(equation, term['@ParameterTerm2'])
    term_three = get_term_by_id(equation, term['@ParameterTerm3'])
    value_one = get_default_if_needed(term_one)
    value_two = get_default_if_needed(term_two)
    value_three = get_default_if_needed(term_three)
    if float(value_one) > 0.0:
        stack.insert(0, float(value_two) * float(term['@Multiplier']))
    else:
        stack.insert(0, float(value_three) * float(term['@Multiplier']))
    return stack


def move_spd_type_g_to_top_of_group(equation):
    last_term_was_group_term = False
    start_group_position = None
    start_position_group = None
    groups_seen_already = []
    for i, term in enumerate(equation):
        if '@GroupTerm' in term:
            this_term_is_group_term = True
        else:
            this_term_is_group_term = False

        if not last_term_was_group_term and this_term_is_group_term and term['@GroupTerm'] not in groups_seen_already:
            start_group_position = i
            start_position_group = term['@GroupTerm']

        if (last_term_was_group_term and this_term_is_group_term and term['@GroupTerm'] != equation[i-1]['@GroupTerm']
           and term['@GroupTerm'] not in groups_seen_already):
            start_group_position = i
            start_position_group = term['@GroupTerm']

        if (last_term_was_group_term and term['@SpdType'] == 'G' and
                equation[i - 1]['@GroupTerm'] == start_position_group):
            if start_group_position == 0:
                equation.insert(start_group_position, equation.pop(i))
            else:
                if equation[start_group_position - 1]['@SpdType'] != 'G':
                    equation.insert(start_group_position, equation.pop(i))

        if this_term_is_group_term and term['@GroupTerm'] not in groups_seen_already:
            groups_seen_already.append(term['@GroupTerm'])

        last_term_was_group_term = this_term_is_group_term

    return equation


def move_branching_terms_to_top_of_their_group(equation):
    for i, term in enumerate(equation):
        if term['@SpdType'] == 'B' and i != 0:
            if equation[i - 1] in [term['ParameterTerm1'], term['ParameterTerm2'], term['ParameterTerm3']]:
                term_above = equation[i - 1]
                equation[i - 1] = term
                equation[i] = term_above


def get_sub_groups(group):
    sub_group_ids = []
    for term in group:
        if '@GroupTerm' in term and term['@GroupTerm'] not in sub_group_ids:
            sub_group_ids.append(term['@GroupTerm'])
    return sub_group_ids


def remove_redundant_group_terms(equation):
    for i, term in enumerate(equation):
        if '@GroupTerm' in term and term['@SpdType'] == 'G':
            del term['@GroupTerm']
    return equation


def clear_group_values(equation):
    for i, term in enumerate(equation):
        if term['@SpdType'] == 'G' and '@Value' in term:
            del term['@Value']
    return equation


# def collect_group(equation, group_id):
#     group = []
#     for term in equation:
#         if '@GroupTerm' in term and term['@GroupTerm'] == group_id:
#             term = term.copy()
#             del term['@GroupTerm']
#             group.append(term)
#     return group


def collect_group(equation, group_id):
    group = []
    first_group_member_position = None
    last_group_member_position = None
    first_member_g_type = False
    for i, term in enumerate(equation):
        if '@GroupTerm' in term and term['@GroupTerm'] == group_id and first_group_member_position is None:
            first_group_member_position = i
            if term['@SpdType'] == 'G':
                first_member_g_type = True

        if '@GroupTerm' in term and term['@GroupTerm'] == group_id and first_group_member_position is not None:
            last_group_member_position = i

        if (last_group_member_position == i - 1 and '@GroupTerm' in term and term['@GroupTerm'] != group_id and first_member_g_type and
            (equation[i - 1]['@SpdType'] == 'G' or equation[i - 1]['@GroupTerm'] == term['@GroupTerm'])):
            last_group_member_position = i

    for i, term in enumerate(equation):
        if first_group_member_position <= i <= last_group_member_position:
            term = term.copy()
            if term['@GroupTerm'] == group_id:
                del term['@GroupTerm']
            group.append(term)


    return group


def are_groups(equation):
    for term in equation:
        if '@GroupTerm' in term.keys():
            return True
    return False





