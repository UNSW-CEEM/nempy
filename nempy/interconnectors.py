from nempy import helper_functions as hf
import numpy as np


def create(definitions, next_variable_id):
    definitions = hf.save_index(definitions, 'variable_id', next_variable_id)
    definitions = hf.stack_columns(definitions,
                                   ['variable_id', 'interconnector', 'max', 'min'],
                                   ['to_region', 'from_region'],
                                   'direction', 'region')
    definitions['coefficient'] = np.where(definitions['direction'] == 'to_region', 1.0, -1.0)
    definitions['service'] = 'energy'
    definitions['type'] = 'continuous'
    definitions = definitions.loc[:, ['variable_id', 'interconnector', 'region', 'min', 'max', 'type', 'service',
                                      'coefficient']]
    definitions.columns = ['variable_id', 'interconnector', 'region', 'lower_bound', 'upper_bound', 'type', 'service',
                           'coefficient']
    return definitions
