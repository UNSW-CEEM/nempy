import numpy as np
import pandas as pd

from nempy.historical_inputs import xml_cache


class RHSCalc:
    """
    Engine for calculating generic constraint right hand side (RHS) values from scratch based on the equations
    provided in the NEMDE xml input files.

    AEMO publishes the RHS values used in dispatch, however, those values are dynamically calculated by NEMDE and
    depend on inputs such as transmission line flows, generator on statuses, and generator output levels. This class
    allows the user to update the input values which the RHS equations depend on and then recalulate RHS values. The
    primary reason for implementing this functionality is to allow the Bass link switch run to be implemented using
    Nempy, which requires that the RHS values of a number of constraints to be recalculated for the case where the
    bass link frequency controller is not active.

    The methodology for the calculation is based on the description in the Constraint Implementation Guidelines
    published by AEMO, :download:`see AEMO doc <../../docs/pdfs/Constraint Implementation Guidelines v3 FINAL Clean.pdf>`.
    The main limitation of the method implemented is that it does not allow for the calculation of constraints that
    use BRANCH operation. In 2013 there were three constraints using the branching operation (V^SML_NIL_3,
    V^SML_NSWRB_2, V^S_HYCP, Q^NIL_GC), and in 2023 it appears the branch operation is no longer in active use.
    While there are some difference between the RHS values produced, generally they are small,

    Examples
    --------
    >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
    >>> xml_cache_manager.load_interval('2019/01/01 00:00:00')
    >>> rhs_calculator = RHSCalc(xml_cache_manager)

    Parameters
    ----------
    xml_cache_manager: instance of nempy class XMLCacheManager
    """
    def __init__(self, xml_cache_manager):
        self.inputs = xml_cache_manager.xml['NEMSPDCaseFile']['NemSpdInputs']
        self.xml_cache_manager = xml_cache_manager
        self.scada_data = self._reformat_scada_data(self.inputs['ConstraintScadaDataCollection']['ConstraintScadaData'])
        self.generic_equations = self._format_generic_equations(self.inputs['GenericEquationCollection']['GenericEquation'])
        self.rhs_constraint_equations = self._format_rhs_constraint_equations(
            self.inputs['GenericConstraintCollection']['GenericConstraint'])
        self.unit_initial_mw = self._format_initial_conditions(self.xml_cache_manager.get_unit_initial_conditions())
        self.entered_values = (
            self._format_entered_values(self.inputs['PeriodCollection']['Period']['EnteredValuePeriodCollection']['EnteredValuePeriod']))
        self.msnsp_from_availbility, self.msnsp_to_availbility = (
            self._format_mnsp_availability(self.inputs['PeriodCollection']['Period']['InterconnectorPeriodCollection']
                                           ['InterconnectorPeriod']))
        self.nemde_rhs_values = self._format_nemde_rhs_values(self.xml_cache_manager.get_constraint_rhs())

    @staticmethod
    def _reformat_scada_data(scada_data):
        """
        Takes the SCADA from NEMDE xml input data that has been passed to dictionary format and re-formats the data so
        that the data is stored in a nested dictionary format with the highest level key specifying the data type and
        the next level down specifying the SPD ID (unique value for the data). There can be many values for an given
        SPD ID so the values are stored in a list, with each item of the containing dictionary with SCADA values and
        metadata. See the example structure below

        example_new_format = {
            'A': {
                '220_GEN_INERTIA':
                    [
                        {'@Value': '38.1619987487793', '@EMS_ID': 'INER', '@EMS_Key': 'YPS.SUMM.BASE.INER',
                          '@Grouping_ID': 'VIC1', '@Can_Use_Last_Good': 'True', '@Can_Use_Value': 'True',
                          '@EMS_Good': 'True', '@EMS_Replaced': 'False', '@Data_Flags': '1075904512', '@Site_ID': 'NOREEMP',
                          '@Good_Input_Count': '2', '@EMS_TimeStamp': '2012-12-28T14:21:35+10:00', '@Est_Value': '0',
                          '@Est_Flags': '0', '@Can_Use_Est_Value': 'False', '@IsReferenced': 'False', '@GoodValues': True}
                    ],
                '500_GEN_INERTIA':
                    [
                        {'@Value': '178.278015136719', '@EMS_ID': 'INER', '@EMS_Key': 'MARKET.V_H_EX.5_59*500GENS.INER',
                          '@Grouping_ID': 'VIC1', '@Can_Use_Last_Good': 'True', '@Can_Use_Value': 'True',
                          '@EMS_Good': 'False', '@EMS_Replaced': 'True', '@Data_Flags': '539033856', '@Site_ID': 'MANEEMP',
                          '@Good_Input_Count': '2', '@EMS_TimeStamp': '2012-12-31T19:12:35+10:00', '@Est_Value': '0',
                          '@Est_Flags': '0', '@Can_Use_Est_Value': 'False', '@IsReferenced': 'True', '@GoodValues': True}
                    ]
                }
            }

        Parameters
        ----------
        scada_data: dic, SCADA data from the NEMDE xml input file that has been passed to dict format.

        Returns
        -------
        dict in format described above

        """
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
        """
        Takes generic equations in dict format passed from NEMDE xml input and re-formats such that each equation is
        stored in a single level dict and be retrieved using the equation ID.

        example_new_format = {
            'BA-HO_66-LNK_STATUS':  # <- equation ID - And below in list are the terms that make up the equation
                [
                    OrderedDict([
                        ('@TermID', '1'), ('@Multiplier', '0.0001'), ('@Operation', 'PUSH'), ('@SpdID', 'Constant'),
                        ('@SpdType', 'C'), ('@Default', '1')]),
                    OrderedDict([('@TermID', '2'), ('@Multiplier', '9999'),
                        ('@Operation', 'PUSH'), ('@SpdID', 'Constant'), ('@SpdType', 'C'), ('@Default', '1')]),
                    OrderedDict([
                        ('@TermID', '3'), ('@Multiplier', '1'), ('@Operation', 'PUSH'), ('@SpdID', 'ART_CB_B_STAT'),
                        ('@SpdType', 'S'), ('@Default', '1')]),
                    OrderedDict([('@TermID', '4'), ('@Multiplier', '1'),
                        ('@Operation', 'MUL'), ('@SpdID', 'ART_CB_F_STAT'), ('@SpdType', 'S'), ('@Default', '1')]),
                ]
            }

        Parameters
        ----------
        generic_equations: dict, generic equation data from the NEMDE xml input file that has been passed to dict format.

        Returns
        -------
        dict
        """
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
        """
        Takes rhs equations in dict format passed from NEMDE xml input and re-formats such that each equation is
        stored in a single level dict and be retrieved using the equation ID.

        We use the same example as generic equation but in the actual data the rhs equations are distinct from the
        generic equations


        example_new_format = {
            'BA-HO_66-LNK_STATUS':  # <- equation ID - And below in list are the terms that make up the equation
                [
                    OrderedDict([
                        ('@TermID', '1'), ('@Multiplier', '0.0001'), ('@Operation', 'PUSH'), ('@SpdID', 'Constant'),
                        ('@SpdType', 'C'), ('@Default', '1')]),
                    OrderedDict([('@TermID', '2'), ('@Multiplier', '9999'),
                        ('@Operation', 'PUSH'), ('@SpdID', 'Constant'), ('@SpdType', 'C'), ('@Default', '1')]),
                    OrderedDict([
                        ('@TermID', '3'), ('@Multiplier', '1'), ('@Operation', 'PUSH'), ('@SpdID', 'ART_CB_B_STAT'),
                        ('@SpdType', 'S'), ('@Default', '1')]),
                    OrderedDict([('@TermID', '4'), ('@Multiplier', '1'),
                        ('@Operation', 'MUL'), ('@SpdID', 'ART_CB_F_STAT'), ('@SpdType', 'S'), ('@Default', '1')]),
                ]
            }

        Parameters
        ----------
        constraints: dict, rhs equation data from the NEMDE xml input file that has been passed to dict format.

        Returns
        -------
        dict
        """
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
        """
        Takes unit initial condition from the NEMDE xml input file passed from the XMLCacheManager and converts it
        such that each unit's initial condition is stored in a dict and can be retrieved using their DUID.

        Parameters
        ----------
        initial_conditions: pandas Dataframe with columns DUID and INITIALMW

        Returns
        -------
        dict
        """
        return initial_conditions.set_index('DUID')['INITIALMW'].to_dict()

    @staticmethod
    def _format_nemde_rhs_values(constraints):
        """
        Takes rhs constraint values from the NEMDE xml input file passed from the XMLCacheManager and converts it
        such that each constraint rhs is stored in a dict and can be retrieved using the constraint ID.

        Parameters
        ----------
        constraints: pandas Dataframe with columns set and rhs (set is the constraint ID)

        Returns
        -------
        dict
        """
        return constraints.set_index('set')['rhs'].to_dict()

    @staticmethod
    def _format_entered_values(entered_values):
        """
        Takes the entered value data from the dict format of the NEMDE xml input data and re-formats it such that each
        value is stored in a dict and can be retrieved using its SPD ID.

        Parameters
        ----------
        entered_values: dict in format passed from NEMDE xml input data

        Returns
        -------
        dict
        """
        new_format = {}
        for element in entered_values:
            new_format[element['@SpdID']] = element['@Value']
        return new_format

    @staticmethod
    def _format_mnsp_availability(interconnectors):
        """
        Takes the market interconnector offer data from the dict format of the NEMDE xml input data and extracts the
        maximum availabilities in the to and from direction and stores them in separate dictionaries such that they
        can be retrieved using the interconnector ID.

        Parameters
        ----------
        interconnectors: dict market interconnector offer data in format passed from NEMDE xml input data

        Returns
        -------
        dict, dict
        """
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

    def _get_rhs_equations_that_dont_reference_generic_equations(self):
        """
        Helper function for testing that retrieves the IDs of rhs equations that don't reference any generic equations.

        Returns
        -------

        """
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
        """
        Get the RHS values of a constraints as calculated by NEMDE. This method is implemented primarily to assist with
        testing.

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
        >>> xml_cache_manager.load_interval('2019/01/01 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)
        >>> rhs_calculator.get_nemde_rhs("F_MAIN++NIL_BL_R60")
        -10290.279635

        Parameters
        ----------
        constraint_id: str which is the unique ID of the constraint

        Returns
        -------
        float
        """
        return float(self.nemde_rhs_values[constraint_id])

    def compute_constraint_rhs(self, constraint_id):
        """
        Calculates the rhs values of the speficied constraint or list of constraints.

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
        >>> xml_cache_manager.load_interval('2019/01/01 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)
        >>> rhs_calculator.compute_constraint_rhs('F_MAIN++NIL_BL_R60')
        -10290.737541856766

        >>> rhs_calculator.compute_constraint_rhs(['F_MAIN++NIL_BL_R60', 'F_MAIN++NIL_BL_R6'])
                          set           rhs
        0  F_MAIN++NIL_BL_R60 -10290.737542
        1   F_MAIN++NIL_BL_R6 -10581.475084

        Parameters
        ----------
        constraint_id: str or list[str] which is the unique ID of the constraint or a list of the strings which are
            the constraint IDs

        Returns
        -------
        float or pandas DataFrame
        """
        if type(constraint_id) == str:
            equation = self.rhs_constraint_equations[constraint_id]
            equation = self._resolve_term_values(equation)
            rhs = _rpn_calc(equation)
        else:
            rhs = []
            for id in constraint_id:
                equation = self.rhs_constraint_equations[id]
                equation = self._resolve_term_values(equation)
                rhs.append(_rpn_calc(equation))
            rhs = pd.DataFrame({
                'set': constraint_id,
                'rhs': rhs
            })
        return rhs

    def get_rhs_constraint_equations_that_depend_value(self, spd_id, type):
        """
        A helper method used to find the which constraints' RHS depend on a given input value.

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('nemde_cache_2014_12')
        >>> xml_cache_manager.load_interval('2014/12/05 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)
        >>> rhs_calculator.get_rhs_constraint_equations_that_depend_value('BL_FREQ_ONSTATUS', 'W')
        ['F_MAIN++APD_TL_L5', 'F_MAIN++APD_TL_L6', 'F_MAIN++APD_TL_L60', 'F_MAIN++ML_L5_0400', 'F_MAIN++ML_L5_APD', 'F_MAIN++ML_L60_0400', 'F_MAIN++ML_L60_APD', 'F_MAIN++ML_L6_0400', 'F_MAIN++ML_L6_APD', 'F_MAIN++NIL_DYN_LREG', 'F_MAIN++NIL_DYN_RREG', 'F_MAIN++NIL_MG_R5', 'F_MAIN++NIL_MG_R6', 'F_MAIN++NIL_MG_R60', 'F_MAIN+APD_TL_L5', 'F_MAIN+APD_TL_L6', 'F_MAIN+APD_TL_L60', 'F_MAIN+ML_L5_0400', 'F_MAIN+ML_L5_APD', 'F_MAIN+ML_L60_0400', 'F_MAIN+ML_L60_APD', 'F_MAIN+ML_L6_0400', 'F_MAIN+ML_L6_APD', 'F_MAIN+NIL_DYN_LREG', 'F_MAIN+NIL_DYN_RREG', 'F_MAIN+NIL_MG_R5', 'F_MAIN+NIL_MG_R6', 'F_MAIN+NIL_MG_R60', 'F_T++LREG_0050', 'F_T++NIL_BB_TG_R5', 'F_T++NIL_BB_TG_R6', 'F_T++NIL_BB_TG_R60', 'F_T++NIL_MG_R5', 'F_T++NIL_MG_R6', 'F_T++NIL_MG_R60', 'F_T++NIL_ML_L5', 'F_T++NIL_ML_L6', 'F_T++NIL_ML_L60', 'F_T++NIL_TL_L5', 'F_T++NIL_TL_L6', 'F_T++NIL_TL_L60', 'F_T++NIL_WF_TG_R5', 'F_T++NIL_WF_TG_R6', 'F_T++NIL_WF_TG_R60', 'F_T++RREG_0050', 'F_T+LREG_0050', 'F_T+NIL_BB_TG_R5', 'F_T+NIL_BB_TG_R6', 'F_T+NIL_BB_TG_R60', 'F_T+NIL_MG_R5', 'F_T+NIL_MG_R6', 'F_T+NIL_MG_R60', 'F_T+NIL_ML_L5', 'F_T+NIL_ML_L6', 'F_T+NIL_ML_L60', 'F_T+NIL_TL_L5', 'F_T+NIL_TL_L6', 'F_T+NIL_TL_L60', 'F_T+NIL_WF_TG_R5', 'F_T+NIL_WF_TG_R6', 'F_T+NIL_WF_TG_R60', 'F_T+RREG_0050', 'T_V_NIL_BL1', 'V_T_NIL_BL1']

        Parameters
        ----------
        spd_id: str, the ID of the value used in the NEMDE xml input file.
        type: str, the type of the value used in the NEMDE xml input file. See the Constraint Implementation Guidelines
            published by AEMO for more information on SPD types, :download:`see AEMO doc <../../docs/pdfs/Constraint Implementation Guidelines v3 FINAL Clean.pdf>`

        Returns
        -------
        list[str] a list of strings detailing the constraits' whose RHS equations depend on the specified value.
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
        """
        Updates the value of one of the inputs which the RHS constraint equations depend on.

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
        >>> xml_cache_manager.load_interval('2019/01/01 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)
        >>> rhs_calculator.update_spd_id_value('220_GEN_INERTIA', 'A', '100.0')

        Parameters
        ----------
        spd_id: str, the ID of the value used in the NEMDE xml input file.
        type: str, the type of the value used in the NEMDE xml input file. See the Constraint Implementation Guidelines
            published by AEMO for more information on SPD types, :download:`see AEMO doc <../../docs/pdfs/Constraint Implementation Guidelines v3 FINAL Clean.pdf>`
        value: str (detailing a float number) the new value to set the input to.
        """
        if type in ['C', 'R', 'X']:
            raise ValueError('Spd term values of type C can\'t be updated')
        elif type in ['A', 'S', 'I', 'W']:
            if len(self.scada_data[type][spd_id]) > 1:
                raise ValueError('SPD ID and type has more than one value, update not possible.')
            else:
                self.scada_data[type][spd_id][0]['@Value'] = value
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
        """
        For each term in a rhs or generic equation find the terms value if it has one.

        Parameters
        ----------
        equation: list[dict] a rhs or generic equation.

        Returns
        -------
        equation: list[dict] a rhs or generic equation with values for each term, if the term is a type that has an
            associated value.
        """
        for term in equation:
            value = self._resolve_term_value(term)
            if value is not None:
                term["@Value"] = value
        return equation

    def _resolve_term_value(self, term):
        """
        Attempt to find a value for a term in a rhs or generic equation by looking for the corresponding term SPD ID in
        SCADA data, unit initial MW data, entered values, market interconnector availability data, or computing generic
        equations. If there are multiple SCADA values then the sum of these is returned.

        Parameters
        ----------
        term: dict

        Returns
        -------
        str, float, or None

        """
        if term['@SpdType'] == 'C':
            value = None
        elif term['@SpdType'] in ['A', 'S', 'R', 'I', 'W']:
            scadas = self.scada_data[term['@SpdType']][term['@SpdID']]
            if len(scadas) > 0:
                value = 0
                for scada in scadas:
                    # if scada['@Can_Use_Value'] == 'False':
                    #     raise ValueError("Bad SCADA value")
                    if '@EMS_ID' in scada.keys() and scada['@EMS_ID'] in []:
                        # value -= float(scada['@Value'])
                        pass
                    else:
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
        return value

    def _compute_generic_equation(self, equation_id):
        """
        Calculates the value of a gernic equation.

        Examples
        --------
        >>> xml_cache_manager = xml_cache.XMLCacheManager('test_nemde_cache')
        >>> xml_cache_manager.load_interval('2019/01/01 00:00:00')
        >>> rhs_calculator = RHSCalc(xml_cache_manager)
        >>> rhs_calculator._compute_generic_equation('X_BASSLINK_OFF')
        0.0001

        Parameters
        ----------
        equation_id: str specifying the equation to evaulate

        Returns
        -------
        float
        """
        equation = self.generic_equations[equation_id]
        equation = self._resolve_term_values(equation)
        value = _rpn_calc(equation)
        return value


def _rpn_stack(equation):
    """
    Calculates the stack state after evaluating a rhs or generic equation.

    Parameters
    ----------
    equation: list[dict] the equation to be evaluated

    Returns
    -------
    list[float]

    """
    stack = [0.0]  # Stack starts with height 1 and value 0.0.
    ignore_groups = []  # Once groups have been calculated they are added to list to be ignored so they aren't processed
    # again
    pop_flag = False
    _clear_group_values(equation)
    for i, term in enumerate(equation):

        if (term['@SpdType'] == 'G' and '@Value' not in term and
                ('@GroupTerm' not in term or term['@GroupTerm'] not in ignore_groups)):
            # Deals with the case where a term of type G precedes at set of terms that form a group. In this case the
            # group terms are evaluated separately and the resulting value is given the type G term which is the
            # processed as normal equation term. The group members have their group ID added to the list of groups to
            # ignore, so they are not processed again on subsequent iterations.
            remove_first_member = False
            if '@GroupTerm' not in term:
                # If the type G term does not share the group ID then we look at the next term to find the group ID.
                if '@GroupTerm' in equation[i+1]:
                    group_id = equation[i+1]['@GroupTerm']
                else:
                    continue
            else:
                group_id = term['@GroupTerm']
                # If the type G term at the front of the group shares the same group ID then we flag it to be removed
                # from the group before processing the group.
                remove_first_member = True
            group = _collect_group(equation, group_id)
            if remove_first_member:
                group.pop
            group_result = _rpn_calc(group)
            term['@Value'] = group_result
            ignore_groups.append(group_id)
            sub_groups_ids = _get_sub_groups(group)
            ignore_groups += sub_groups_ids
        elif '@GroupTerm' in term and term['@GroupTerm'] not in ignore_groups:
            # Deals with the case where a type G term does not proceed the group. In this case the group is processed
            # and if a type G term is at the end of the group, the group result is added to that type G term, otherwise
            # the value from the group is added directly to the stack.
            group_id = term['@GroupTerm']
            group = _collect_group(equation, group_id)
            group_result = _rpn_calc(group)
            if equation[i + len(group)]['@SpdType'] == 'G':
                equation[i + len(group)]['@Value'] = group_result
            else:
                stack[0] += group_result
            ignore_groups.append(term['@GroupTerm'])
            sub_groups_ids = _get_sub_groups(group)
            ignore_groups += sub_groups_ids

        if '@GroupTerm' not in term:
            # If a term is not a group term then evaluate it according to it operation. Not when group terms are
            # collected processed separately they have their @GroupTerm attribute removed so they will be processed
            # according to their operation.
            if term['@SpdType'] == 'U' and '@Value' not in term and '@Operation' not in term:
                stack = _type_u_no_operator(stack, term)
            elif term['@SpdType'] == 'B':
                raise ValueError("Failed when attempting to evaluate equation cannot process equations with"
                                 "terms of SPD Type B, as the branching method is not implemented.")
            elif '@Operation' not in term:
                stack = _no_operator(stack, term)
            elif term['@Operation'] == 'ADD' and term['@SpdType'] == 'U':
                stack = _add_on_stack(stack, term)
            elif term['@Operation'] == 'ADD':
                stack = _add(stack, term)
            elif term['@Operation'] == 'SUB' and term['@SpdType'] == 'U':
                stack = _subtract_on_stack(stack, term)
            elif term['@Operation'] == 'SUB':
                stack = _subtract(stack, term)
            elif term['@Operation'] == 'MUL' and term['@SpdType'] == 'U':
                stack = _multipy_on_stack(stack, term)
            elif term['@Operation'] == 'MUL':
                stack = _multiply(stack, term)
            elif term['@Operation'] == 'DIV' and term['@SpdType'] == 'U':
                stack = _divide_on_stack(stack, term)
            elif term['@Operation'] == 'DIV':
                stack = _divide(stack, term)
            elif term['@Operation'] == 'MAX' and term['@SpdType'] == 'U':
                stack = _max_on_stack(stack, term)
            elif term['@Operation'] == 'MAX':
                stack = _maximum(stack, term)
            elif term['@Operation'] == 'MIN' and term['@SpdType'] == 'U':
                stack = _min_on_stack(stack, term)
            elif term['@Operation'] == 'MIN':
                stack = _minimum(stack, term)
            elif term['@Operation'] == 'STEP':
                stack = _step(stack, term)
            elif term['@Operation'] == 'POW2':
                stack = _square(stack, term)
            elif term['@Operation'] == 'POW3':
                stack = _cube(stack, term)
            elif term['@Operation'] == 'SQRT':
                stack = _sqrt(stack, term)
            elif term['@Operation'] == 'ABS':
                stack = _absolute_value(stack, term)
            elif term['@Operation'] == 'NEG':
                stack = _negation(stack, term)
            elif term['@Operation'] == 'PUSH':
                stack = _push(stack, term)
            elif term['@Operation'] == 'DUP' and term['@SpdType'] == 'U':
                stack = _duplicate(stack, term)
            elif term['@Operation'] == 'EXCH' and term['@SpdType'] == 'U':
                stack = _exchange(stack, term)
            elif term['@Operation'] == 'RSD' and term['@SpdType'] == 'U':
                stack = _roll_stack_down(stack, term)
            elif term['@Operation'] == 'RSU' and term['@SpdType'] == 'U':
                stack = _roll_stack_up(stack, term)
            elif term['@Operation'] == 'POP':
                pop_flag, stack = _stack_pop(stack, term)
            elif term['@Operation'] == 'EXLEZ' and term['@SpdType'] == 'U' and pop_flag:
                stack = _exchange_if_less_than_zero(stack, term)
                pop_flag = False
    return stack


def _rpn_calc(equation):
    return _rpn_stack(equation)[0]


def _get_default_if_needed(term):
    if '@Value' not in term:
        return term['@Default']
    else:
        return term['@Value']


def _no_operator(stack, term):
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


def _type_u_no_operator(stack, term):
    # If a term is type U and has no operator then the multiplier is applied to the top element of the stack. See AEMO
    # Constraint Implementation Guidelines section A.5 Top stack element.
    if len(stack) == 0:
        stack.insert(0, 0.0)
    stack[0] = stack[0] * float(term['@Multiplier'])
    return stack


def _step(stack, term):
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
        value = _get_default_if_needed(term)
        if float(value) > 0.0:
            value_to_add = float(term['@Multiplier'])
        else:
            value_to_add = 0.0
        if len(stack) > 0:
            stack[0] += value_to_add
        else:
            stack.append(value_to_add)
    return stack


def _square(stack, term):
    # For terms that are POW2 operators either the term value or the top stack value is squared. See AEMO Constraint
    # Implementation Guidelines section A.6.2 Square.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 2 * float(term['@Multiplier'])
    else:
        # If not type U the apply the POW2 operation to the term value.
        if term['@SpdType'] == 'C':
            stack[0] += float(term['@Multiplier']) ** 2
        else:
            stack[0] += float(term['@Value']) ** 2 * float(term['@Multiplier'])
    return stack


def _cube(stack, term):
    # For terms that are POW3 operators either the term value or the top stack value is cubed. See AEMO Constraint
    # Implementation Guidelines section A.6.3 Cube.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 3 * float(term['@Multiplier'])
    else:
        # If not type U the apply the POW3 operation to the term value
        if term['@SpdType'] == 'C':
            stack[0] += float(term['@Multiplier']) ** 3
        else:
            stack[0] += float(term['@Value']) ** 3 * float(term['@Multiplier'])
    return stack


def _sqrt(stack, term):
    # For terms that are SQRT operators either the term value or the top stack value is square rooted. See AEMO
    # Constraint Implementation Guidelines section A.6.4 Square Root.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = stack[0] ** 0.5 * float(term['@Multiplier'])
    else:
        # If not type U the apply the SQRT operation to the term value.
        if term['@SpdType'] == 'C':
            stack[0] += float(term['@Multiplier']) ** 0.5
        else:
            stack[0] += float(term['@Value']) ** 0.5 * float(term['@Multiplier'])
    return stack


def _absolute_value(stack, term):
    # For terms that are ABS operators either the absolute value of the term value or the top stack value. See AEMO
    # Constraint Implementation Guidelines section A.6.5 Absolute Value.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = abs(stack[0]) * float(term['@Multiplier'])
    else:
        # If not type U the apply the ABS operation to the term value.
        if term['@SpdType'] == 'C':
            stack[0] += abs(float(term['@Multiplier']))
        else:
            stack[0] += abs(float(term['@Value'])) * float(term['@Multiplier'])
    return stack


def _negation(stack, term):
    # For terms that are NEG operators either the term value or the top stack value is negated. See AEMO Constraint
    # Implementation Guidelines section A.6.5 Negation.
    if term['@SpdType'] == 'U':
        # If type U then apply the POW2 operation to the element on top of the stack.
        stack[0] = -1.0 * stack[0] * float(term['@Multiplier'])
    else:
        # If not type U the apply the NEG operation to the term value.
        if term['@SpdType'] == 'C':
            stack[0] += -1.0 * float(term['@Multiplier'])
        else:
            stack[0] += -1.0 * float(term['@Value']) * float(term['@Multiplier'])
    return stack


def _add_on_stack(stack, term):
    # Where an ADD operation  has type U then the ADD operation is performed on the two top elements of the stack.
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


def _add(stack, term):
    # Where an ADD operation does not have type U then the ADD operation is performed with the top element of the stack
    # and the term value.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Add.
    value_one = _get_default_if_needed(term)
    if term['@SpdType'] == 'C':
        next_top_element = float(term['@Multiplier']) + stack[0]
    else:
        next_top_element = (float(value_one) + stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def _subtract_on_stack(stack, term):
    # Where an SUB operation  has type U then the subtract operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Subtract.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] - stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def _subtract(stack, term):
    # Where an SUB operation does not have type U then the subtract operation is performed with the top element of the
    # stack and the term value.
    # See AEMO Constraint Implementation Guidelines section A.7.1 Subtract.
    value_one = _get_default_if_needed(term)
    if term['@SpdType'] == 'C':
        next_top_element = stack[0] - float(term['@Multiplier'])
    else:
        next_top_element = (stack[0] - float(value_one)) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def _multipy_on_stack(stack, term):
    # Where an MUL (multiply) operation  has type U then the MUL operation is performed on the two top elements of the
    # stack.
    # See AEMO Constraint Implementation Guidelines section A.7.3 Multiply.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] * stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def _multiply(stack, term):
    # Where an MUL (multiply) operation does not have type U then the multiply operation is performed with the top
    # element of the stack and the term value.
    # See AEMO Constraint Implementation Guidelines section A.7.3 Multiply.
    value_one = _get_default_if_needed(term)
    if term['@SpdType'] == 'C':
        next_top_element = (stack[0]) * float(term['@Multiplier'])
    else:
        next_top_element = (float(value_one) * stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def _divide_on_stack(stack, term):
    # Where an DIV (divide) operation  has type U then the MUL operation is performed on the two top elements of the
    # stack.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Divide.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = (stack[1] / stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def _divide(stack, term):
    # Where an DIV (divide) operation does not have type U then the multiply operation is performed with the top
    # element of the stack and the term value.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Divide.
    value_one = _get_default_if_needed(term)
    if term['@SpdType'] == 'C':
        next_top_element = stack[0] / float(term['@Multiplier'])
    else:
        next_top_element = (stack[0] / float(value_one)) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def _max_on_stack(stack, term):
    # Where a MAX operation  has type U then the MAX operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.5 Maximum.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = max(stack[1], stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def _maximum(stack, term):
    # Where a MAX operation does not have type U then the MAX operation is performed with the top element of the stack
    # and the term value.
    # See AEMO Constraint Implementation Guidelines section A.7.4 Maximum.
    value_one = _get_default_if_needed(term)
    if term['@SpdType'] == 'C':
        next_top_element = max(float(term['@Multiplier']), stack[0])
    else:
        next_top_element = max(float(value_one), stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def _min_on_stack(stack, term):
    # Where an MIN (minimum) operation is encountered in the equation with no previous term, without an operator, for
    # the MIN operation to act on, then the MIN operation is performed on the two top elements of the stack.
    # See AEMO Constraint Implementation Guidelines section A.7.6 Minimum.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    next_top_element = min(stack[1], stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack[0] = next_top_element
    return stack


def _minimum(stack, term):
    # Where a MIN operation does not have type U then the MIN operation is performed with the top element of the stack
    # and the term value.
    # See AEMO Constraint Implementation Guidelines section A.7.6 Minimum.
    value_one = _get_default_if_needed(term)
    if term['@SpdType'] == 'C':
        next_top_element = min(float(term['@Multiplier']), stack[0])
    else:
        next_top_element = min(float(value_one), stack[0]) * float(term['@Multiplier'])
    stack.pop(0)
    stack.insert(0, next_top_element)
    return stack


def _push(stack, term):
    # If a push operator is given the value of the operator with the multiplier applied is added to the top of the
    # stack.
    # See AEMO Constraint Implementation Guidelines section A.8.1 Push.
    if term['@SpdType'] not in ['C']:  # Condition found through empirical testing
        value = _get_default_if_needed(term)
        stack.insert(0, float(term['@Multiplier']) * float(value))
    else:
        stack.insert(0, float(term['@Multiplier']))
    return stack


def _duplicate(stack, term):
    # If a DUP operator is given the value at the top of the stack is duplicated, the multiplier is applied and
    # the term is added to the top of the stack.
    # See AEMO Constraint Implementation Guidelines section A.8.2 Duplicate.
    stack.insert(0, stack[0] * float(term['@Multiplier']))
    return stack


def _exchange(stack, term):
    # If a EXCH operator is given the top and second top elements of the stacked are swapped.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.insert(1, top_element)
    stack.insert(0, stack.pop(0) * float(term['@Multiplier']))
    return stack


def _roll_stack_down(stack, term):
    # If a RSD operator is given the bottom element of the stack is moved to the top and the multiplier is applied.
    # See AEMO Constraint Implementation Guidelines section A.8.3 Exchange.
    if len(stack) < 2:
        raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    bottom_element = stack.pop(-1)
    stack.insert(0, bottom_element * float(term['@Multiplier']))
    return stack


def _roll_stack_up(stack, term):
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


def _stack_pop(stack, term):
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


def _exchange_if_less_than_zero(stack, term):
    # If the EXLEZ is given and the pop flag is true then the top two elements are exchanged.
    # See AEMO Constraint Implementation Guidelines section A.9.2
    if len(stack) < 2:
        return stack
        # raise ValueError('Attempting to perform multi value operation on stack with less than 2 elements.')
    top_element = stack.pop(0)
    stack.insert(1, top_element)
    stack.insert(0, stack.pop(0) * float(term['@Multiplier']))
    return stack


def _get_sub_groups(group):
    sub_group_ids = []
    for term in group:
        if '@GroupTerm' in term and term['@GroupTerm'] not in sub_group_ids:
            sub_group_ids.append(term['@GroupTerm'])
    return sub_group_ids


def _clear_group_values(equation):
    for i, term in enumerate(equation):
        if term['@SpdType'] == 'G' and '@Value' in term:
            del term['@Value']
    return equation


def _collect_group(equation, group_id):
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

        if (last_group_member_position == i - 1 and '@GroupTerm' in term and term['@GroupTerm'] != group_id and
                first_member_g_type and
                (equation[i - 1]['@SpdType'] == 'G' or equation[i - 1]['@GroupTerm'] == term['@GroupTerm'])):
            last_group_member_position = i

    for i, term in enumerate(equation):
        if first_group_member_position <= i <= last_group_member_position:
            term = term.copy()
            if term['@GroupTerm'] == group_id:
                del term['@GroupTerm']
            group.append(term)

    return group


def _are_groups(equation):
    for term in equation:
        if '@GroupTerm' in term.keys():
            return True
    return False





