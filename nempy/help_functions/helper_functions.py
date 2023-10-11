import numpy as np
import pandas as pd


def save_index(dataframe, new_col_name, offset=0):
    # Make sure index starts at zero.
    dataframe = dataframe.reset_index(drop=True)
    # Save the indexes of the data frame as an np array.
    index_list = np.array(dataframe.index.values)
    # Add an offset to each element of the array.
    offset_index_list = index_list + offset
    # Add the list of indexes as a column to the data frame.
    dataframe[new_col_name] = offset_index_list
    return dataframe


def max_constraint_index(newest_variable_data):
    # Find the maximum constraint index already in use in the constraint matrix.
    max_index = newest_variable_data['ROWINDEX'].max()
    return max_index


def stack_columns(data_in, cols_to_keep, cols_to_stack, type_name, value_name):
    # Wrapping pd.melt to make it easier to use in nemlite context.
    stacked_data = pd.melt(data_in, id_vars=cols_to_keep, value_vars=cols_to_stack,
                           var_name=type_name, value_name=value_name)
    return stacked_data


def add_capacity_band_type(df_with_price_bands, ns):
    # Map the names of the capacity bands to a dataframe that already has the names of the price bands.
    band_map = pd.DataFrame()
    band_map[ns.col_price_band_number] = ns.cols_bid_price_name_list
    band_map[ns.col_capacity_band_number] = ns.cols_bid_cap_name_list
    df_with_capacity_and_price_bands = pd.merge(df_with_price_bands, band_map, 'left', [ns.col_price_band_number])
    return df_with_capacity_and_price_bands


def max_variable_index(newest_variable_data):
    # Find the maximum variable index already in use in the constraint matrix.
    max_index = newest_variable_data['INDEX'].max()
    return max_index


def update_rhs_values(constraint_rhs_and_type, new_rhs_values):
    if 'volume' in constraint_rhs_and_type.columns:
        rhs_name = 'volume'
    else:
        rhs_name = 'rhs'
    new_rhs_values = new_rhs_values.rename(columns={'rhs': 'new_rhs'})
    constraint_rhs_and_type = pd.merge(constraint_rhs_and_type, new_rhs_values, on='set', how='left')
    constraint_rhs_and_type[rhs_name] = np.where(~constraint_rhs_and_type['new_rhs'].isna(),
                                                 constraint_rhs_and_type['new_rhs'],
                                                 constraint_rhs_and_type[rhs_name])
    constraint_rhs_and_type = constraint_rhs_and_type.drop(columns=['new_rhs'])
    return constraint_rhs_and_type
