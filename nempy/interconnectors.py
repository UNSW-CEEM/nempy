import numpy as np
import pandas as pd
from nempy import helper_functions as hf


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


def add_losses(break_points, inter_variables, loss_functions, next_variable_id, next_constraint_id):
    loss_variables = create_loss_variables(inter_variables, loss_functions, next_variable_id)
    next_variable_id = loss_variables['variable_id'].max() + 1
    weight_variables = create_weights(break_points, next_variable_id)
    weights_sum_lhs, weights_sum_rhs = create_weights_must_sum_to_one(weight_variables, next_constraint_id)
    next_constraint_id = weights_sum_rhs['constraint_id'].max() + 1
    link_to_flow_lhs, link_to_flow_rhs = link_weights_to_inter_flow(weight_variables, inter_variables,
                                                                    next_constraint_id)
    next_constraint_id = link_to_flow_rhs['constraint_id'].max() + 1
    link_to_loss_lhs, link_to_loss_rhs = link_weights_to_inter_loss(weight_variables, loss_variables, loss_functions,
                                                                    next_constraint_id)
    lhs = pd.concat([weights_sum_lhs, link_to_flow_lhs, link_to_loss_lhs])
    dynamic_rhs = pd.concat([link_to_flow_rhs, link_to_loss_rhs])
    weight_variables = weight_variables.loc[:, ['variable_id', 'interconnector', 'lower_bound', 'upper_bound', 'type']]
    return loss_variables, weight_variables, lhs, weights_sum_rhs, dynamic_rhs


def link_weights_to_inter_loss(weight_variables, loss_variables, loss_functions, next_constraint_id):
    constraint_ids = weight_variables.loc[:, ['interconnector']].drop_duplicates('interconnector')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'variable_id', 'break_point']], constraint_ids, 'inner',
                   on='interconnector')
    lhs = pd.merge(lhs, loss_functions, 'inner', on='interconnector')
    lhs['coefficient'] = lhs.apply(lambda x: x['loss_function'](x['break_point']), axis=1)
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    rhs = pd.merge(constraint_ids,
                   loss_variables.drop_duplicates('variable_id').loc[:, ['variable_id', 'interconnector']],
                   'inner', on='interconnector')
    rhs['rhs_variable_id'] = rhs['variable_id']
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'constraint_id', 'rhs_variable_id', 'type']]
    return lhs, rhs


def link_weights_to_inter_flow(weight_variables, flow_variables, next_constraint_id):
    constraint_ids = weight_variables.loc[:, ['interconnector']].drop_duplicates('interconnector')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'variable_id', 'break_point']], constraint_ids, 'inner',
                   on='interconnector')
    lhs['coefficient'] = lhs['break_point']
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    rhs = pd.merge(constraint_ids,
                   flow_variables.drop_duplicates('variable_id').loc[:, ['variable_id', 'interconnector']],
                   'inner', on='interconnector')
    rhs['rhs_variable_id'] = rhs['variable_id']
    rhs['type'] = '='
    rhs = rhs.loc[:, ['interconnector', 'constraint_id', 'rhs_variable_id', 'type']]
    return lhs, rhs


def create_weights_must_sum_to_one(weight_variables, next_constraint_id):
    constraint_ids = weight_variables.loc[:, ['interconnector']].drop_duplicates('interconnector')
    constraint_ids = hf.save_index(constraint_ids, 'constraint_id', next_constraint_id)
    lhs = pd.merge(weight_variables.loc[:, ['interconnector', 'variable_id']], constraint_ids, 'inner',
                   on='interconnector')
    lhs['coefficient'] = 1.0
    lhs = lhs.loc[:, ['variable_id', 'constraint_id', 'coefficient']]
    rhs = constraint_ids
    rhs['rhs'] = 1
    rhs['type'] = '='
    return lhs, rhs


def create_weights(break_points, next_variable_id):
    weight_variables = hf.save_index(break_points, 'variable_id', next_variable_id)
    weight_variables['lower_bound'] = 0.0
    weight_variables['upper_bound'] = 1.0
    weight_variables['type'] = 'continuous'
    return weight_variables


def create_loss_variables(inter_variables, loss_function, next_variable_id):
    loss_variables = hf.save_index(loss_function.loc[:, ['interconnector', 'from_region_loss_share']], 'variable_id',
                                   next_variable_id)
    columns_for_loss_variables = inter_variables.loc[:, ['interconnector', 'region', 'lower_bound', 'upper_bound',
                                                         'type', 'service', 'coefficient']]
    columns_for_loss_variables['upper_bound'] = \
        columns_for_loss_variables.loc[:, ['lower_bound', 'upper_bound']].abs().max(axis=1)
    columns_for_loss_variables['lower_bound'] = 0.0
    loss_variables = pd.merge(loss_variables, columns_for_loss_variables)
    loss_variables['coefficient'] = np.where(loss_variables['coefficient'] < 0.0,
                                             - 1 * loss_variables['from_region_loss_share'],
                                             - 1 * (1 - loss_variables['from_region_loss_share']))
    loss_variables = loss_variables.loc[:, ['variable_id', 'interconnector', 'region', 'lower_bound', 'upper_bound',
                                            'type', 'service', 'coefficient']]
    return loss_variables
